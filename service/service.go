package service

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/sync/semaphore"
	"libvirt.org/go/libvirt"
)

const (
	configPath                string  = "/etc/qemu-auto-ballooning/qemu-auto-ballooning.conf"
	urlDefault                string  = "qemu:///system"     // for remote - qemu+ssh://user@IP/system
	parallelOperationsDefault int64   = 1                    // number of parallel domains processed
	operationsDelayDefault    int64   = 500                  // milliseconds
	vcpuTimeDefault           int64   = 40                   // seconds
	frequencyDefault          int64   = 5                    // seconds
	changeDefault             float64 = 0.1                  // 10% of current memory balloon
	spreadDefault             int     = 10                   // +-10%
	metadataUriDefault        string  = "http://controller/" // SpaceVM metadata uri
)

var (
	sem             *semaphore.Weighted
	cfg             Config
	operationsDelay time.Duration
	vcpuTime        uint64
)

type Metadata struct {
	XMLName            xml.Name `xml:"instance"`
	Safety             bool     `xml:"safety"`               // SpaceVM flag means that domain's memory is protected and should not be modified
	MemoryMinGuarantee uint64   `xml:"memory_min_guarantee"` // SpaceVM flag means minimum domain's memory
}

type Config struct {
	Url                string  `json:"url"`                 // hypervisor url
	ParallelOperations int64   `json:"parallel_operations"` // number of parallel domains processed
	OperationsDelay    int64   `json:"operations_delay"`    // waiting after processing one domain in milliseconds
	VcpuTime           int64   `json:"vcpu_time"`           // Vcpu time for domain boot in seconds
	Frequency          int64   `json:"frequency"`           // main service frequency and guests balloon driver statistics collection period in seconds
	Change             float64 `json:"change"`              // % of current memory balloon
	Spread             int     `json:"spread"`              // the minimum acceptable spread (+%/-%) of memory usage values between the node and the VM
}

func LogMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	slog.Info("Memory stats",
		"Alloc", m.Alloc,
		"Sys", m.Sys,
		"NumGC", m.NumGC,
		"Goroutines", runtime.NumGoroutine(),
	)
}

func LoadConfig() {
	fileBytes, err := os.ReadFile(configPath)
	if err != nil {
		slog.Error("Failed to open config file", "error", err)
	} else {
		err = json.Unmarshal(fileBytes, &cfg)
		if err != nil {
			slog.Error("Failed to decode json in config file", "error", err)
		}
	}
	if cfg.Url == "" {
		cfg.Url = urlDefault
	}
	if cfg.ParallelOperations == 0 {
		cfg.ParallelOperations = parallelOperationsDefault
	}
	if cfg.OperationsDelay == 0 {
		cfg.OperationsDelay = operationsDelayDefault
	}
	if cfg.VcpuTime == 0 {
		cfg.VcpuTime = vcpuTimeDefault
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

func TimeThis(start time.Time, name string) {
	elapsed := time.Since(start)
	slog.Info("TimeThis", name, elapsed)
}

// func init() {
// 	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
// }

func Run() {
	LoadConfig()

	sem = semaphore.NewWeighted(cfg.ParallelOperations)
	operationsDelay = time.Duration(cfg.OperationsDelay) * time.Millisecond // nanoseconds
	vcpuTime = uint64(time.Duration(cfg.VcpuTime) * time.Second)            // nanoseconds

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	frequency := time.Duration(cfg.Frequency) * time.Second // nanoseconds

	slog.Info("Patrolling...")
	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopped")
			return
		default:
			// LogMemoryStats()
			start := time.Now()
			err := ProcessActiveDomains(ctx)
			if err != nil {
				slog.Error("Error in ProcessActiveDomains", "error", err)
			}
			elapsed := time.Since(start)
			if elapsed < frequency {
				time.Sleep(frequency - elapsed)
			}
		}
	}
}

func ProcessActiveDomains(ctx context.Context) error {
	// defer TimeThis(time.Now(), "ProcessActiveDomains")

	conn, err := libvirt.NewConnect(cfg.Url)
	if err != nil {
		return fmt.Errorf("Failed to connect to hypervisor", "error", err)
	}
	defer conn.Close()

	domains, err := conn.ListAllDomains(libvirt.CONNECT_LIST_DOMAINS_RUNNING)
	if err != nil {
		return fmt.Errorf("Failed to get active domains: %v", err)
	}
	if len(domains) == 0 {
		// No domains, no problems
		return nil
	}

	for _, domain := range domains {
		select {
		case <-ctx.Done():
			return nil
		default:
			err = sem.Acquire(ctx, 1)
			if err != nil {
				continue
			}

			go func(_domain *libvirt.Domain, _conn *libvirt.Connect) {
				defer _domain.Free()
				defer sem.Release(1)
				err := ProcessDomain(_domain, _conn)
				if err != nil {
					slog.Error("Error in ProcessDomain", "error", err)
				}
			}(&domain, conn)
			time.Sleep(operationsDelay)
		}
	}
	return nil
}

func ProcessDomain(domain *libvirt.Domain, conn *libvirt.Connect) error {
	// defer TimeThis(time.Now(), "ProcessDomain")

	domainMetadata := GetMetadata(domain)

	if domainMetadata.Safety {
		return nil
	}

	domainName, err := domain.GetName()
	if err != nil {
		return fmt.Errorf("Failed to get domain name: %v", err)
	}

	domainStats, err := GetDomainStats(domain, conn)
	if err != nil {
		return fmt.Errorf("Failed to get domain (%s) stats: %v", domainName, err)
	}
	if len(domainStats) == 0 {
		// most likely, domain changed its status after ListAllDomains
		return nil
	}
	defer domainStats[0].Domain.Free()

	if domainStats[0].Vcpu[0].Time < vcpuTime {
		return nil
	}

	if !IsMemoryStatsActual(domainStats[0].Balloon.LastUpdate) {
		err = domain.SetMemoryStatsPeriod(int(cfg.Frequency), libvirt.DOMAIN_MEM_LIVE)
		if err != nil {
			return fmt.Errorf("Failed to set domain (%s) memory stats period: %v", domainName, err)
		}
		return nil
	}

	nodeMemoryUsedPercent, err := GetNodeMemoryUsedPercent(conn)
	if err != nil {
		return fmt.Errorf("Failed to get node memory stats while processing domain (%s): %v", domainName, err)
	}

	domainMemoryUsed := domainStats[0].Balloon.Available - domainStats[0].Balloon.Usable
	domainMemoryUsedPercent := float64(domainMemoryUsed) / float64(domainStats[0].Balloon.Available) * 100
	changeDirection := int(domainMemoryUsedPercent-nodeMemoryUsedPercent) / cfg.Spread

	if changeDirection == 0 {
		return nil
	}

	changeAmount := float64(domainStats[0].Balloon.Current) * cfg.Change * float64(changeDirection)
	newCurrent := uint64(float64(domainStats[0].Balloon.Current) + changeAmount)

	if newCurrent > domainStats[0].Balloon.Maximum {
		if domainStats[0].Balloon.Current < domainStats[0].Balloon.Maximum {
			newCurrent = domainStats[0].Balloon.Maximum
			changeAmount = float64(newCurrent - domainStats[0].Balloon.Current)
		} else {
			return nil
		}
	}

	if newCurrent <= domainMemoryUsed {
		return nil
	}

	if newCurrent < domainMetadata.MemoryMinGuarantee {
		if domainStats[0].Balloon.Current > domainMetadata.MemoryMinGuarantee {
			newCurrent = domainMetadata.MemoryMinGuarantee
			changeAmount = float64(domainStats[0].Balloon.Current - newCurrent)
		} else {
			return nil
		}
	}

	_, err = domain.QemuMonitorCommand(
		fmt.Sprintf("balloon %d", newCurrent/1024),
		libvirt.DOMAIN_QEMU_MONITOR_COMMAND_HMP,
	)
	if err != nil {
		return fmt.Errorf("Failed to change domain (%s) memory balloon: %v", domainName, err)
	} else {
		slog.Info(
			domainName,
			"change", int(changeAmount),
			"current", newCurrent,
			"maximum", domainStats[0].Balloon.Maximum,
			"used", domainMemoryUsed,
			"minGuarantee", domainMetadata.MemoryMinGuarantee,
			"domainMemoryUsedPercent", int(domainMemoryUsedPercent),
			"nodeMemoryUsedPercent", int(nodeMemoryUsedPercent),
		)
	}
	return nil
}

func GetDomainStats(domain *libvirt.Domain, conn *libvirt.Connect) ([]libvirt.DomainStats, error) {
	var domains []*libvirt.Domain
	domains = append(domains, domain)
	stats, err := conn.GetAllDomainStats(
		domains,
		libvirt.DOMAIN_STATS_BALLOON|libvirt.DOMAIN_STATS_VCPU,
		libvirt.CONNECT_GET_ALL_DOMAINS_STATS_RUNNING,
	)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

func GetNodeMemoryUsedPercent(conn *libvirt.Connect) (float64, error) {
	// GetMemoryStats does not return the values SReclaimable and KReclaimable
	nodeMemoryStats, err := conn.GetMemoryStats(libvirt.NODE_MEMORY_STATS_ALL_CELLS, 0)
	if err != nil {
		return 0.0, err
	}
	nodeMemoryAvailable := nodeMemoryStats.Free + nodeMemoryStats.Buffers + nodeMemoryStats.Cached
	nodeMemoryUsed := nodeMemoryStats.Total - nodeMemoryAvailable
	return float64(nodeMemoryUsed) / float64(nodeMemoryStats.Total) * 100, nil
}

func GetMetadata(domain *libvirt.Domain) *Metadata {
	var metadata Metadata
	xmlData, _ := domain.GetMetadata(
		libvirt.DOMAIN_METADATA_ELEMENT,
		metadataUriDefault,
		libvirt.DOMAIN_AFFECT_LIVE,
	)
	xml.Unmarshal([]byte(xmlData), &metadata)
	metadata.MemoryMinGuarantee *= 1024
	return &metadata
}

func IsMemoryStatsActual(lastUpdate uint64) bool {
	return (time.Now().Unix() - int64(lastUpdate)) <= cfg.Frequency
}
