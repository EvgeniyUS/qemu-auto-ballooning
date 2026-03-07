package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"qemu-auto-ballooning/monitor"
	"qemu-auto-ballooning/service"
)

var (
	flagRun     bool
	flagMonitor bool
	flagHelp    bool
)

func DefineFlags() {
	flag.BoolVar(&flagRun, "r", false, "Run QEMU Auto ballooning service")
	flag.BoolVar(&flagMonitor, "m", false, "Run QEMU balloon monitor")
	flag.BoolVar(&flagHelp, "h", false, "Show this help")
}

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	DefineFlags()
	flag.Parse()
}

func main() {
	if flagRun {
		// slog.Info("debug", "flagRun", flagRun)
		service.Run()
	} else if flagMonitor {
		// slog.Info("debug", "flagMonitor", flagMonitor)
		monitor.Run()
	} else if flagHelp {
		PrintHelp()
	} else {
		PrintHelp()
	}
}

func PrintHelp() {
	fmt.Println("Auto ballooning service for QEMU guests (VMs) on a memory over-committed host.")
	fmt.Println("systemctl start/stop qemu-auto-ballooning.service")
	flag.Usage()
}
