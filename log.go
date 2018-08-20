package gorillaz

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Log *zap.Logger

func InitLogs() {
	config := zap.NewProductionConfig()

	logLevel := viper.GetString("log.level")

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
}

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
}
