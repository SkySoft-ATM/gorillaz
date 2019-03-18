package gorillaz

import (
	"fmt"
	"log"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log is the main logger.
var Log *zap.Logger

// Sugar is
var Sugar *zap.SugaredLogger

// set default level to INFO
var atomLevel = zap.NewAtomicLevelAt(zapcore.InfoLevel)

func init() {
	config := zap.NewProductionConfig()
	err := config.EncoderConfig.EncodeTime.UnmarshalText([]byte("iso8601"))
	if err != nil {
		log.Fatalf("error trying to define encoding %v", err)
	}
	config.Level = atomLevel
	l, err := config.Build()
	if err != nil {
		panic(err)
	}
	Log = l
	Sugar = Log.Sugar()
}

// InitLogs initializes the Sugar (*zap.SugaredLogger) and Log (*zap.Logger) elements
// returns an error if logLevel can't be mapped to a zapcore.LogLevel, otherwise returns nil
func InitLogs(logLevel string) error {
	level, err := zapLogLevel(logLevel)
	if err != nil {
		return err
	}
	atomLevel.SetLevel(level)
	return nil
}

// LogLevel returns the current log level
func LogLevel() zapcore.Level {
	return atomLevel.Level()
}

func zapLogLevel(logLevel string) (zapcore.Level, error) {
	switch strings.ToLower(logLevel) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "":
		return zapcore.InfoLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "panic":
		return zapcore.PanicLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("invalid error level %s", logLevel)
	}
}
