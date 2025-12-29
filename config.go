package loader

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration flags
type Config struct {
	// Debug - whether to print records before insert into MongoDB
	Debug bool
	// TimezoneOffset - timezone offset in hours (default: 7 for GMT+7)
	TimezoneOffset int
	// TimezoneLocation - parsed timezone location
	TimezoneLocation *time.Location
}

// GlobalConfig is the global configuration instance
var GlobalConfig *Config

// InitConfig initializes the global configuration from environment variables
// Environment variables:
//
//	DEBUG - "true"/"false" - whether to print records before MongoDB insert (default: false)
//	TIMEZONE_OFFSET - integer offset in hours from UTC (default: 7 for GMT+7)
func InitConfig() {
	tzOffset := parseIntEnv("TIMEZONE_OFFSET", 7)

	// Create timezone location with fixed offset
	tzName := "UTC"
	if tzOffset >= 0 {
		tzName = "GMT+" + strconv.Itoa(tzOffset)
	} else {
		tzName = "GMT" + strconv.Itoa(tzOffset)
	}
	tzLocation := time.FixedZone(tzName, tzOffset*3600)

	GlobalConfig = &Config{
		Debug:            parseBoolEnv("DEBUG", false),
		TimezoneOffset:   tzOffset,
		TimezoneLocation: tzLocation,
	}

	GlobalLogger.Infof("Config initialized: Debug=%v, TimezoneOffset=%d hours (%s)", GlobalConfig.Debug, GlobalConfig.TimezoneOffset, tzName)
}

// parseBoolEnv parses a boolean environment variable with a default value
func parseBoolEnv(key string, defaultValue bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	return strings.ToLower(val) == "true"
}

// parseIntEnv parses an integer environment variable with a default value
func parseIntEnv(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		GlobalLogger.Warnf("Invalid integer value for %s: %s, using default: %d", key, val, defaultValue)
		return defaultValue
	}
	return intVal
}
