package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"qemu-auto-ballooning/config"
	"qemu-auto-ballooning/monitor"
	"qemu-auto-ballooning/service"
)

var (
	flagRun      bool
	flagMonitor  bool
	flagLogLevel int
	flagHelp     bool
	flagConfig   bool
)

func DefineFlags() {
	flag.BoolVar(&flagRun, "r", false, "Run QEMU Auto ballooning service")
	flag.BoolVar(&flagMonitor, "m", false, "Run QEMU balloon monitor")
	flag.IntVar(&flagLogLevel, "l", 0, "Debug = -4, Info = 0, Warn = 4, Error = 8")
	flag.BoolVar(&flagHelp, "h", false, "Show this help")
	flag.BoolVar(&flagConfig, "c", false, "Show config")
}

func init() {
	DefineFlags()
	flag.Parse()
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.Level(flagLogLevel)})
	slog.SetDefault(slog.New(h))
}

func main() {
	if flagRun {
		service.Run()
	} else if flagMonitor {
		monitor.Run()
	} else if flagConfig {
		config.Print()
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
