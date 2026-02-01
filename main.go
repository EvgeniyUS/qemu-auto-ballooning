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
	parallelOperations	int64			= 5		// number of parallel domains processed
	TTL					time.Duration	= 5		// seconds
	memoryStatsPeriod	int				= 5		// the period in seconds for stats collection
	memoryAmountStep	float64			= 0.1	// 10% of current memory balloon
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

	ticker := time.NewTicker(TTL * time.Second)
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

	hostMemStats, err := mem.VirtualMemory()
	if err != nil {
		return fmt.Errorf("Failed to get node memory stats: %v", err)
	}
	hostMemStatus := usedMemStatus(hostMemStats.UsedPercent)

	// // Getmemorystatus does not return the values SReclaimable and KReclaimable
	// nodeMemoryStats, err := conn.GetMemoryStats(libvirt.NODE_MEMORY_STATS_ALL_CELLS, 0)
	// if err != nil {
	// 	return fmt.Errorf("Failed to get node memory stats: %v", err)
	// }
	// slog.Info("debug", "nodeMemoryStats", nodeMemoryStats)
	// nodeMemoryAvailable := nodeMemoryStats.Free + nodeMemoryStats.Buffers + nodeMemoryStats.Cached
	// nodeMemoryUsed := nodeMemoryStats.Total - nodeMemoryAvailable
	// nodeMemoryUsedPercent := float64(nodeMemoryUsed) / float64(nodeMemoryStats.Total) * 100
	// hostMemStatus := usedMemStatus(nodeMemoryUsedPercent)

	for _, stat := range stats {
		err = sem.Acquire(ctx, 1)
		if err != nil {
			slog.Error("Semaphore acquire failed", "error", err)
			continue
		}

		go func(_stat libvirt.DomainStats) {
			defer sem.Release(1)
			err := processDomain(_stat, hostMemStatus)
			if err != nil {
				slog.Error("Error in processDomain", "error", err)
			}
			_stat.Domain.Free()
		}(stat)
	}
	return nil
}

func processDomain(stat libvirt.DomainStats, hostMemStatus int) error {
	domainName, err := stat.Domain.GetName()
	if err != nil {
		return fmt.Errorf("Failed to get domain name: %v", err)
	}

	if !isMemoryStatsActual(stat.Balloon.LastUpdate) {
		err = stat.Domain.SetMemoryStatsPeriod(memoryStatsPeriod, libvirt.DOMAIN_MEM_LIVE)
		if err != nil {
			return fmt.Errorf("Failed to set domain memory stats period: %v", err)
		}
		return nil
	}

	// // func (d *Domain) GetMetadata(metadataType DomainMetadataType, uri string, flags DomainModificationImpact) (string, error)
	// metadata, err := stat.Domain.GetMetadata(libvirt.DomainMetadataType(1), "NULL", libvirt.DOMAIN_AFFECT_LIVE)
	// if err != nil {
	// 	return fmt.Errorf("Failed to get domain metadata: %v", err)
	// }
	// slog.Info("debug", "metadata", metadata)

	// // "MinGuaranteeSet":false,"MinGuarantee":0
	// memParams, err := stat.Domain.GetMemoryParameters(libvirt.DOMAIN_AFFECT_LIVE)
	// if err != nil {
	// 	return fmt.Errorf("Failed to get domain memory params: %v", err)
	// }
	// slog.Info("Domain params", "domainName", domainName, "memParams", memParams)

	domainMemUsed := stat.Balloon.Available - stat.Balloon.Usable
	domainMemUsedProc := float64(domainMemUsed) / float64(stat.Balloon.Available) * 100
	domainMemStatus := usedMemStatus(domainMemUsedProc)
	stepPower := domainMemStatus - hostMemStatus

	if stepPower == 0 {
		return nil
	}

	changeAmount := float64(stat.Balloon.Current) * memoryAmountStep * float64(stepPower)
	newBalloon := uint64(float64(stat.Balloon.Current) + changeAmount)

	if newBalloon > stat.Balloon.Maximum {
		if stat.Balloon.Current < stat.Balloon.Maximum {
			newBalloon = stat.Balloon.Maximum
			changeAmount = float64(stat.Balloon.Maximum - stat.Balloon.Current)
		} else {
			return nil
		}
	}

	if newBalloon <= domainMemUsed {
		return nil
	}

	_, err = stat.Domain.QemuMonitorCommand(
		fmt.Sprintf("balloon %d", newBalloon / 1024),
		libvirt.DOMAIN_QEMU_MONITOR_COMMAND_HMP,
	)
	if err != nil {
		return fmt.Errorf("Failed to change domains (%s) memory balloon: %v", domainName, err)
	} else {
		slog.Info(
			domainName,
			"changeAmount", int(changeAmount),
			"newBalloon", newBalloon,
			"maximum", stat.Balloon.Maximum,
			"used", domainMemUsed,
		)
	}
	return nil
}

func usedMemStatus(usedMemPercent float64) int {
    if usedMemPercent > 90.0 {
        return 3	// critical
    }
	if usedMemPercent > 70.0 {
        return 2	// high
    }
	if usedMemPercent > 50.0 {
        return 1	// middle
    }
	return 0		// low
}

func isMemoryStatsActual(lastUpdate uint64) bool {
	maxAgeSeconds := int64(memoryStatsPeriod)
	now := time.Now().Unix()
	return (now - int64(lastUpdate)) <= maxAgeSeconds
}
