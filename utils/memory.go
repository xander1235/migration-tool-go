package utils

import (
	"migration-tool-go/logger"
	"runtime"
	"time"
)

// MemoryThreshold represents a memory threshold in percentage of total system memory
type MemoryThreshold struct {
	WarningPercent  float64 // Percentage at which to log a warning
	CriticalPercent float64 // Percentage at which to pause processing
	CooldownSeconds int     // How long to pause when critical threshold is reached
}

// DefaultMemoryThreshold returns a default memory threshold configuration
func DefaultMemoryThreshold() MemoryThreshold {
	return MemoryThreshold{
		WarningPercent:  70.0, // Warn at 70% memory usage
		CriticalPercent: 85.0, // Pause at 85% memory usage
		CooldownSeconds: 5,    // Pause for 5 seconds
	}
}

// GetMemoryUsage returns the current memory usage as a percentage of total memory
func GetMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// Get memory usage in MB for logging
	memoryUsedMB := float64(m.Alloc) / 1024.0 / 1024.0
	
	// Calculate percentage of total memory
	totalMemory := float64(m.Sys) / 1024.0 / 1024.0
	percentUsed := (memoryUsedMB / totalMemory) * 100.0
	
	return percentUsed
}

// CheckMemoryThreshold checks if memory usage exceeds thresholds and takes appropriate action
// Returns true if processing should continue, false if it should wait
func CheckMemoryThreshold(threshold MemoryThreshold) bool {
	percentUsed := GetMemoryUsage()
	
	// Log memory usage at warning threshold
	if percentUsed >= threshold.WarningPercent {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		
		logger.Sugar.Warnf("High memory usage: %.2f%% (Alloc: %.2f MB, Sys: %.2f MB)",
			percentUsed,
			float64(m.Alloc)/1024.0/1024.0,
			float64(m.Sys)/1024.0/1024.0)
	}
	
	// If we're at critical threshold, trigger GC and pause
	if percentUsed >= threshold.CriticalPercent {
		logger.Sugar.Warnf("Critical memory usage (%.2f%%), pausing for %d seconds and running GC",
			percentUsed, threshold.CooldownSeconds)
		
		// Force garbage collection
		runtime.GC()
		
		// Sleep to allow system to stabilize
		time.Sleep(time.Duration(threshold.CooldownSeconds) * time.Second)
		
		// Check if memory usage improved
		newPercentUsed := GetMemoryUsage()
		logger.Sugar.Infof("After cooldown: memory usage now at %.2f%%", newPercentUsed)
		
		return false // Indicate that processing should wait
	}
	
	return true // Continue processing
}
