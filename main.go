package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"
	"syscall"

	"github.com/shirou/gopsutil/v4/mem"
    "libvirt.org/go/libvirt"
	"golang.org/x/sync/semaphore"
)

const (
	parallelOperations	int64	= 5		// number of parallel domains processed
	TTL					int		= 5		// seconds
	changePercent		float64	= 0.1	// 10% of current memory balloon
)

var (
	sem *semaphore.Weighted
)

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	sem = semaphore.NewWeighted(parallelOperations)
}

func main() {
	slog.Info("Starting...")

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	ticker := time.NewTicker(time.Duration(TTL) * time.Second)
	defer ticker.Stop()

	// Connecting to QEMU
	conn, err := libvirt.NewConnect("qemu:///system")
	if err != nil {
		slog.Error("Failed to connect to QEMU", "error", err)
		return
	}
	defer conn.Close()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutting down...")
			return
		case <-ticker.C:
			err := processActiveDomains(ctx, conn)
			if err != nil {
				slog.Error("Error in processActiveDomains", "error", err)
			}
		}
	}
}

func processActiveDomains(ctx context.Context, conn *libvirt.Connect) error {
	// Running domains with memory stats
	stats, err := conn.GetAllDomainStats(
		[]*libvirt.Domain{},
		libvirt.DOMAIN_STATS_BALLOON,
		libvirt.CONNECT_GET_ALL_DOMAINS_STATS_RUNNING,
	)
	if err != nil {
		return fmt.Errorf("Failed to get active domains with memory stats: %v", err)
	}

	if len(stats) == 0 {
		return nil
	}

	nodeMemoryStats, err := mem.VirtualMemory()
	if err != nil {
		return fmt.Errorf("Failed to get node memory stats: %v", err)
	}

	// // Getmemorystatus does not return the values SReclaimable and KReclaimable
	// nodeMemoryStats, err := conn.GetMemoryStats(libvirt.NODE_MEMORY_STATS_ALL_CELLS, 0)
	// if err != nil {
	// 	return fmt.Errorf("Failed to get node memory stats: %v", err)
	// }
	// slog.Info("debug", "nodeMemoryStats", nodeMemoryStats)
	// nodeMemoryAvailable := nodeMemoryStats.Free + nodeMemoryStats.Buffers + nodeMemoryStats.Cached
	// nodeMemoryUsed := nodeMemoryStats.Total - nodeMemoryAvailable
	// nodeMemoryUsedPercent := float64(nodeMemoryUsed) / float64(nodeMemoryStats.Total) * 100

	for _, stat := range stats {
		err = sem.Acquire(ctx, 1)
		if err != nil {
			slog.Error("Semaphore acquire failed", "error", err)
			continue
		}

		go func(_stat libvirt.DomainStats) {
			defer sem.Release(1)
			err := processDomain(_stat, nodeMemoryStats.UsedPercent)
			if err != nil {
				slog.Error("Error in processDomain", "error", err)
			}
			_stat.Domain.Free()
		}(stat)
	}
	return nil
}

func processDomain(stat libvirt.DomainStats, nodeMemoryUsedPercent float64) error {

	// // Maybe it's better this way... It's not clear yet
	// memStats, err := stat.Domain.MemoryStats(13, 0)
	// if err != nil {
	// 	return fmt.Errorf("Failed to get domain memory stats: %v", err)
	// }
	// slog.Info("debug", "memStats", memStats)

	domainName, err := stat.Domain.GetName()
	if err != nil {
		return fmt.Errorf("Failed to get domain name: %v", err)
	}

	if !isMemoryStatsActual(stat.Balloon.LastUpdate) {
		err = stat.Domain.SetMemoryStatsPeriod(TTL, libvirt.DOMAIN_MEM_LIVE)
		if err != nil {
			return fmt.Errorf("Failed to set domain memory stats period: %v", err)
		}
		return nil
	}

	domainMemoryUsed := stat.Balloon.Available - stat.Balloon.Usable
	domainMemoryUsedPercent := float64(domainMemoryUsed) / float64(stat.Balloon.Available) * 100
	changeDirection := getChangeDirection(domainMemoryUsedPercent, nodeMemoryUsedPercent)
	changeAmount := float64(stat.Balloon.Current) * changePercent * float64(changeDirection)
	newCurrent := uint64(float64(stat.Balloon.Current) + changeAmount)

	if changeDirection == 0 {
		return nil
	}

	if newCurrent > stat.Balloon.Maximum {
		if stat.Balloon.Current < stat.Balloon.Maximum {
			newCurrent = stat.Balloon.Maximum
			changeAmount = float64(stat.Balloon.Maximum - stat.Balloon.Current)
		} else {
			return nil
		}
	}

	if newCurrent <= domainMemoryUsed {
		return nil
	}

	_, err = stat.Domain.QemuMonitorCommand(
		fmt.Sprintf("balloon %d", newCurrent / 1024),
		libvirt.DOMAIN_QEMU_MONITOR_COMMAND_HMP,
	)
	if err != nil {
		return fmt.Errorf("Failed to change domains (%s) memory balloon: %v", domainName, err)
	} else {
		slog.Info(
			domainName,
			"change", int(changeAmount),
			"current", newCurrent,
			"maximum", stat.Balloon.Maximum,
			"used", domainMemoryUsed,
			"domainMemoryUsedPercent", int(domainMemoryUsedPercent),
			"nodeMemoryUsedPercent", int(nodeMemoryUsedPercent),
		)
	}
	return nil
}

func getChangeDirection(domainMemoryUsedPercent float64, nodeMemoryUsedPercent float64) int {
	return int(domainMemoryUsedPercent - nodeMemoryUsedPercent) / 10
}

func isMemoryStatsActual(lastUpdate uint64) bool {
	maxAgeSeconds := int64(TTL)
	now := time.Now().Unix()
	return (now - int64(lastUpdate)) <= maxAgeSeconds
}
