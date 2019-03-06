package gorillaz

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log is the main logger.
var Log *zap.Logger

// Sugar is
var Sugar *zap.SugaredLogger

// InitLogs initializes the Sugar (*zap.SugaredLogger) and Log (*zap.Logger) elements
func InitLogs(logLevel string) {
	config := zap.NewProductionConfig()

	err := config.EncoderConfig.EncodeTime.UnmarshalText([]byte("iso8601"))
	if err != nil {
		log.Fatalf("error trying to define encoding %v", err)
	}

	config.Level = zap.NewAtomicLevelAt(zapcore.PanicLevel)
	if logLevel == "debug" {
		config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	} else if logLevel == "" || logLevel == "info" {
		config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	} else if logLevel == "warn" {
		config.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	} else if logLevel == "error" {
		config.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	} else if logLevel == "panic" {
		config.Level = zap.NewAtomicLevelAt(zapcore.PanicLevel)
	}

	l, err := config.Build()
	if err != nil {
		panic(err)
	}
	Log = l
	Sugar = Log.Sugar()
}

// NewLogger initializes and instantiates both Sugar and Log element with the given zapcore.Level
func NewLogger(level zapcore.Level) {
	config := zap.NewProductionConfig()

	if level == zapcore.DebugLevel {
		config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	} else if level == zapcore.InfoLevel {
		config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	} else if level == zapcore.WarnLevel {
		config.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	} else if level == zapcore.ErrorLevel {
		config.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	} else if level == zapcore.PanicLevel {
		config.Level = zap.NewAtomicLevelAt(zapcore.PanicLevel)
	}

	l, err := config.Build()
	if err != nil {
		panic(err)
	}
	Log = l
	Sugar = Log.Sugar()
}
