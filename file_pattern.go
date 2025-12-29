package loader

import (
	"os"
	"regexp"
	"strings"
)

// FilePattern contains the regex patterns for file matching
type FilePattern struct {
	// AllowPatterns are regex patterns that files must match to be processed (if set)
	// Multiple patterns can be separated by semicolons (;)
	// If any pattern matches, the file is allowed
	AllowPatterns []*regexp.Regexp
	// IgnorePatterns are regex patterns that files must not match to be processed (if set)
	// Multiple patterns can be separated by semicolons (;)
	// If any pattern matches, the file is ignored
	IgnorePatterns []*regexp.Regexp
}

// GlobalFilePattern holds the compiled patterns for file matching
var GlobalFilePattern *FilePattern

// InitFilePatterns initializes the global file patterns from environment variables
// Should be called once at startup
// Supports regex patterns: \.csv$, upload/.*\.csv, sensor_data_.*\.csv, etc.
// Multiple patterns can be separated by semicolons (;)
func InitFilePatterns() {
	GlobalFilePattern = &FilePattern{
		AllowPatterns:  loadAllowPatterns(),
		IgnorePatterns: loadIgnorePatterns(),
	}
}

// loadAllowPatterns loads the regex patterns for allowed files from ALLOW_PATTERN env variable
// If set, only files matching at least one pattern will be processed
// Multiple patterns can be separated by semicolons (;)
// Examples: "\.csv$", "upload/.*\.csv;sensor_data_.*\.csv" (both patterns accepted)
func loadAllowPatterns() []*regexp.Regexp {
	patternStr := os.Getenv("ALLOW_PATTERNS")
	if patternStr == "" {
		GlobalLogger.Info("ALLOW_PATTERNS not set, no files will be allowed (if not ignored)")
		return []*regexp.Regexp{}
	}

	patternStrs := parsePatternString(patternStr)
	if len(patternStrs) == 0 {
		GlobalLogger.Info("ALLOW_PATTERNS is empty, all files will be allowed")
		return []*regexp.Regexp{}
	}

	// Compile and validate patterns
	var patterns []*regexp.Regexp
	var patternStrings []string
	for _, patternStr := range patternStrs {
		patternStr = strings.TrimSpace(patternStr)
		compiled, err := regexp.Compile(patternStr)
		if err != nil {
			GlobalLogger.Fatalf("invalid ALLOW_PATTERNS regex: %q - %v", patternStr, err)
		}
		patterns = append(patterns, compiled)
		patternStrings = append(patternStrings, patternStr)
	}
	GlobalLogger.Infof("Loaded %d ALLOW_PATTERN(s): %v", len(patterns), patternStrings)
	return patterns
}

// loadIgnorePatterns loads the regex patterns for ignored files from IGNORE_PATTERN env variable
// If set, files matching any pattern will be skipped
// Multiple patterns can be separated by semicolons (;)
// Examples: "test_.*\.csv", "debug/.*\.csv;.*_tmp\.csv" (either pattern causes skip)
func loadIgnorePatterns() []*regexp.Regexp {
	patternStr := os.Getenv("IGNORE_PATTERNS")
	if patternStr == "" {
		GlobalLogger.Info("IGNORE_PATTERNS not set, no files will be ignored (except by allow pattern)")
		return []*regexp.Regexp{}
	}

	patternStrs := parsePatternString(patternStr)
	if len(patternStrs) == 0 {
		GlobalLogger.Info("IGNORE_PATTERNS is empty, no files will be ignored")
		return []*regexp.Regexp{}
	}

	// Compile and validate patterns
	var patterns []*regexp.Regexp
	var patternStrings []string
	for _, patternStr := range patternStrs {
		patternStr = strings.TrimSpace(patternStr)
		compiled, err := regexp.Compile(patternStr)
		if err != nil {
			GlobalLogger.Fatalf("invalid IGNORE_PATTERNS regex: %q - %v", patternStr, err)
		}
		patterns = append(patterns, compiled)
		patternStrings = append(patternStrings, patternStr)
	}
	GlobalLogger.Infof("Loaded %d IGNORE_PATTERN(s): %v", len(patterns), patternStrings)
	return patterns
}

// parsePatternString splits pattern string by semicolons and trims whitespace
// Returns non-empty patterns
func parsePatternString(patternStr string) []string {
	if patternStr == "" {
		return []string{}
	}

	parts := strings.Split(patternStr, ";")
	var patterns []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			patterns = append(patterns, p)
		}
	}
	return patterns
}

// ShouldProcessFile checks if a file should be processed
// Returns true if:
//  1. File does NOT match any IGNORE_PATTERN (if set), AND
//  2. File matches at least one ALLOW_PATTERN (if set, else true)
//
// Processing order:
//   - Check IGNORE_PATTERNS first (if any pattern matches, skip immediately)
//   - Check ALLOW_PATTERNS (if set, file must match at least one)
func ShouldProcessFile(filename string) bool {
	if GlobalFilePattern == nil || len(GlobalFilePattern.AllowPatterns) < 1 {
		GlobalLogger.Infof("file %s: no ALLOW_PATTERNS, skipping", filename)
		return false // No patterns set, skip all files
	}

	// Check ignore patterns first (most restrictive)
	if len(GlobalFilePattern.IgnorePatterns) > 0 {
		for _, pattern := range GlobalFilePattern.IgnorePatterns {
			if pattern.MatchString(filename) {
				GlobalLogger.Infof("file %s: matched IGNORE_PATTERN %s, skipping", filename, pattern)
				return false
			}
		}
	}

	for _, pattern := range GlobalFilePattern.AllowPatterns {
		if pattern.MatchString(filename) {
			return true
		}
	}
	GlobalLogger.Infof("file %s: does not match any ALLOW_PATTERN, skipping", filename)
	return false
}

// MatchesPattern checks if a filename matches a specific regex pattern
// Returns true if the pattern is empty (no pattern set) or matches
// This is a utility function for testing compiled regex patterns
func MatchesPattern(compiledPattern *regexp.Regexp, filename string) bool {
	if compiledPattern == nil {
		return true
	}
	return compiledPattern.MatchString(filename)
}

// MatchesAllowPatterns checks if a file matches any of the allow patterns
// Returns true if no patterns are set or if the file matches at least one pattern
func MatchesAllowPatterns(filename string) bool {
	if GlobalFilePattern == nil || len(GlobalFilePattern.AllowPatterns) == 0 {
		return true
	}
	for _, pattern := range GlobalFilePattern.AllowPatterns {
		if pattern.MatchString(filename) {
			return true
		}
	}
	return false
}

// MatchesIgnorePatterns checks if a file matches any of the ignore patterns
// Returns true if the file matches any pattern
func MatchesIgnorePatterns(filename string) bool {
	if GlobalFilePattern == nil || len(GlobalFilePattern.IgnorePatterns) == 0 {
		return false
	}
	for _, pattern := range GlobalFilePattern.IgnorePatterns {
		if pattern.MatchString(filename) {
			return true
		}
	}
	return false
}
