package gorillaz

import (
	"log"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log is the main logger.
var Log *zap.Logger

// Sugar is
var Sugar *zap.SugaredLogger

var atomLevel = zap.NewAtomicLevel()

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
func InitLogs(logLevel string) {
	atomLevel.SetLevel(zapLogLevel(logLevel))
}

func zapLogLevel(logLevel string) zapcore.Level {
	switch strings.ToLower(logLevel) {
	case "debug":
		return zapcore.DebugLevel
	case "":
		return zapcore.InfoLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "panic":
		return zapcore.PanicLevel
	default:
		panic("unknown log level " + logLevel)
	}
}
