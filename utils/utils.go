package utils

import (
	"context"
	"log/slog"
	"runtime"
	"time"
)

func IsDebug() bool {
	return slog.Default().Enabled(context.Background(), slog.LevelDebug)
}

func LogMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	slog.Debug("Memory stats",
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
	slog.Debug("TimeThis", name, elapsed)
}
