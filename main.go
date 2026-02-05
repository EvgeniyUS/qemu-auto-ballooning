package main

import (
	"context"
	"encoding/json"
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
	configPath			string	= "/etc/qemu-auto-ballooning/qemu-auto-ballooning.conf"
	parallelOperations	int64	= 5		// number of parallel domains processed
	frequencyDefault	int		= 5
	changeDefault		float64	= 0.1	// 10% of current memory balloon
	spreadDefault		int		= 10	// +-10%
)

var (
	sem *semaphore.Weighted
	cfg Config
)

type Config struct {
    Frequency	int		`json:"frequency"`	// scan frequency of domains in seconds
    Change		float64	`json:"change"`		// % of current memory balloon
    Spread		int		`json:"spread"`		// the minimum acceptable spread (+%/-%) of memory usage values between the node and the VM 
}

func loadConfig() {
	fileBytes, err := os.ReadFile(configPath)   
	if err != nil {
		slog.Error("Failed to open config file", "error", err)
	} else {
		err = json.Unmarshal(fileBytes, &cfg)
		if err != nil {
			slog.Error("Failed to decode json in config file", "error", err)
		}
	}
	if cfg.Frequency == 0 {
		cfg.Frequency = frequencyDefault
	}
	if cfg.Change == 0 {
		cfg.Change = changeDefault
	}
	if cfg.Spread == 0 {
		cfg.Spread = spreadDefault
	}
	slog.Info("Loaded", "config", cfg)
}

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	loadConfig()
	sem = semaphore.NewWeighted(parallelOperations)
}

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	ticker := time.NewTicker(time.Duration(cfg.Frequency) * time.Second)
	defer ticker.Stop()

	// Connecting to QEMU
	conn, err := libvirt.NewConnect("qemu:///system")
	if err != nil {
		slog.Error("Failed to connect to QEMU", "error", err)
		return
	}
	defer conn.Close()

	slog.Info("Patrolling...")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopped")
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
	// slog.Info("debug", "stats", stats)

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

	domainInfo, err := stat.Domain.GetInfo()
	if err != nil {
		return fmt.Errorf("Failed to get domains (%s) info: %v", domainName, err)
	}
	domainCpuTime := int(domainInfo.CpuTime) / int(domainInfo.NrVirtCpu) / 1000000000
	if domainCpuTime < 20 { // 20s of CpuTime for domain boot
		return nil
	}

	if !isMemoryStatsActual(stat.Balloon.LastUpdate) {
		err = stat.Domain.SetMemoryStatsPeriod(cfg.Frequency, libvirt.DOMAIN_MEM_LIVE)
		if err != nil {
			return fmt.Errorf("Failed to set domains (%s) memory stats period: %v", domainName, err)
		}
		return nil
	}

	domainMemoryUsed := stat.Balloon.Available - stat.Balloon.Usable
	domainMemoryUsedPercent := float64(domainMemoryUsed) / float64(stat.Balloon.Available) * 100
	changeDirection := getChangeDirection(domainMemoryUsedPercent, nodeMemoryUsedPercent)

	if changeDirection == 0 {
		return nil
	}

	changeAmount := float64(stat.Balloon.Current) * cfg.Change * float64(changeDirection)
	newCurrent := uint64(float64(stat.Balloon.Current) + changeAmount)

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
	return int(domainMemoryUsedPercent - nodeMemoryUsedPercent) / cfg.Spread
}

func isMemoryStatsActual(lastUpdate uint64) bool {
	maxAgeSeconds := int64(cfg.Frequency)
	now := time.Now().Unix()
	return (now - int64(lastUpdate)) <= maxAgeSeconds
}
