package loader

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// LogLevel represents the severity level of a log message
type LogLevel string

const (
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelFatal LogLevel = "FATAL"
)

// Logger provides structured logging with optional timestamps
type Logger struct {
	// Whether to include timestamps in log output
	includeTimestamp bool
	// Whether to include log level in output
	includeLevel bool
}

// GlobalLogger is the global logger instance
var GlobalLogger *Logger

// InitLogger initializes the global logger with configuration from environment variables
// Environment variables:
//
//	LOG_TIMESTAMP - "true"/"false" - whether to include timestamps (default: true)
//	LOG_LEVEL - "true"/"false" - whether to include log level (default: true)
func InitLogger() {
	includeTimestamp := true
	includeLevel := true

	// Read LOG_TIMESTAMP config
	if ts := os.Getenv("LOG_TIMESTAMP"); ts != "" {
		includeTimestamp = strings.ToLower(ts) == "true"
	}

	// Read LOG_LEVEL config
	if ll := os.Getenv("LOG_LEVEL"); ll != "" {
		includeLevel = strings.ToLower(ll) == "true"
	}

	GlobalLogger = &Logger{
		includeTimestamp: includeTimestamp,
		includeLevel:     includeLevel,
	}

	GlobalLogger.Infof("Logger initialized (timestamp=%v, level=%v)", includeTimestamp, includeLevel)
}

// formatMessage formats a log message with optional timestamp and level
func (l *Logger) formatMessage(level LogLevel, message string) string {
	parts := []string{}

	if l.includeTimestamp {
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		parts = append(parts, timestamp)
	}

	if l.includeLevel {
		parts = append(parts, fmt.Sprintf("[%s]", level))
	}

	parts = append(parts, message)

	return strings.Join(parts, " ")
}

// Debug logs a debug message
func (l *Logger) Debug(message string) {
	if l == nil {
		fmt.Println(message)
		return
	}
	fmt.Println(l.formatMessage(LogLevelDebug, message))
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.Debug(fmt.Sprintf(format, args...))
}

// Info logs an info message
func (l *Logger) Info(message string) {
	if l == nil {
		fmt.Println(message)
		return
	}
	fmt.Println(l.formatMessage(LogLevelInfo, message))
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}

// Warn logs a warning message
func (l *Logger) Warn(message string) {
	if l == nil {
		fmt.Println(message)
		return
	}
	fmt.Println(l.formatMessage(LogLevelWarn, message))
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.Warn(fmt.Sprintf(format, args...))
}

// Error logs an error message
func (l *Logger) Error(message string) {
	if l == nil {
		fmt.Println(message)
		return
	}
	fmt.Println(l.formatMessage(LogLevelError, message))
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Error(fmt.Sprintf(format, args...))
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(message string) {
	if l == nil {
		fmt.Println(message)
		os.Exit(1)
	}
	fmt.Println(l.formatMessage(LogLevelFatal, message))
	os.Exit(1)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.Fatal(fmt.Sprintf(format, args...))
}

// Printf is an alias for Infof for compatibility with standard logger interface
func (l *Logger) Printf(format string, args ...interface{}) {
	l.Infof(format, args...)
}

// Print is an alias for Info for compatibility with standard logger interface
func (l *Logger) Print(message string) {
	l.Info(message)
}

// Println is an alias for Info for compatibility with standard logger interface
func (l *Logger) Println(message string) {
	l.Info(message)
}
