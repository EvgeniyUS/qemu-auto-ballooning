package utils

import (
	"log/slog"
	"runtime"
	"time"
)

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

func TimeThis(start time.Time, name string) {
	// Usage:
	// defer utils.TimeThis(time.Now(), "<FuncName>")
	elapsed := time.Since(start)
	slog.Info("TimeThis", name, elapsed)
}
