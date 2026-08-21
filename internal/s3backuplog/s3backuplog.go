package s3backuplog

import (
	"io"
	"log"
	"os"
	"sync/atomic"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
var debugEnabled atomic.Bool

func EnableDebug() {
	debugEnabled.Store(true)
}

func DebugPrint(format string, args ...interface{}) {
	if debugEnabled.Load() {
		logger.Printf("[DEBUG] "+format, args...)
	}
}

func InfoPrint(format string, args ...interface{}) {
	logger.Printf("[INFO] "+format, args...)
}

func ErrorPrint(format string, args ...interface{}) {
	logger.Printf("[ERROR] "+format, args...)
}

func WarnPrint(format string, args ...interface{}) {
	logger.Printf("[WARNING] "+format, args...)
}

func FatalPrint(format string, args ...interface{}) {
	logger.Fatalf("[FATAL] "+format, args...)
}

// SetOutput redirects logs and returns a restore function. It is primarily
// useful to embedders and tests; log.Logger itself remains concurrency-safe.
func SetOutput(w io.Writer) func() {
	previous := logger.Writer()
	logger.SetOutput(w)
	return func() { logger.SetOutput(previous) }
}
