package logger

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Sugar is the global sugared logger instance
	Sugar *zap.SugaredLogger
)

// Config stores logger configuration
type Config struct {
	// LogToFile determines if logs should also be written to a file
	LogToFile bool
	// LogFilePath is the path where log files will be written
	LogFilePath string
	// LogLevel sets the minimum log level
	LogLevel string
}

// Initialize sets up the zap logger
func Initialize(config ...Config) {
	// Default configuration
	logConfig := Config{
		LogToFile:  false,
		LogFilePath: "logs/app.log",
		LogLevel:   "info",
	}

	// Apply provided configuration if any
	if len(config) > 0 {
		logConfig = config[0]
	}

	// Create encoder configuration
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	// Parse log level
	var level zapcore.Level
	switch logConfig.LogLevel {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}

	// Create writers
	var cores []zapcore.Core
	
	// Always log to stdout
	cores = append(cores, zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		level,
	))

	// Optionally log to file
	if logConfig.LogToFile && logConfig.LogFilePath != "" {
		// Ensure directory exists
		logDir := filepath.Dir(logConfig.LogFilePath)
		if err := os.MkdirAll(logDir, 0755); err == nil {
			logFile, err := os.OpenFile(logConfig.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err == nil {
				cores = append(cores, zapcore.NewCore(
					zapcore.NewJSONEncoder(encoderConfig),
					zapcore.AddSync(logFile),
					level,
				))
			}
		}
	}

	// Create multi-core
	core := zapcore.NewTee(cores...)

	// Create logger with call site information and stacktraces for errors
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	Sugar = logger.Sugar()
}

// Sync flushes any buffered log entries
func Sync() {
	if Sugar != nil {
		_ = Sugar.Sync()
	}
}
