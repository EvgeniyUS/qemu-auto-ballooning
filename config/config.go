package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
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

type Config struct {
	Url                string  `json:"url"`                 // hypervisor url
	ParallelOperations int64   `json:"parallel_operations"` // number of parallel domains processed
	OperationsDelay    int64   `json:"operations_delay"`    // waiting after processing one domain in milliseconds
	VcpuTime           int64   `json:"vcpu_time"`           // Vcpu time for domain boot in seconds
	Frequency          int64   `json:"frequency"`           // main service frequency and guests balloon driver statistics collection period in seconds
	Change             float64 `json:"change"`              // % of current memory balloon
	Spread             int     `json:"spread"`              // the minimum acceptable spread (+%/-%) of memory usage values between the node and the VM
}

func Load(cfg *Config) error {
	fileBytes, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("Failed to open config file: %v", err)
	} else {
		err = json.Unmarshal(fileBytes, &cfg)
		if err != nil {
			return fmt.Errorf("Failed to decode json in config file: %v", err)
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
	return nil
}
