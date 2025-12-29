package loader

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// FieldMapping represents field code and alias mapping
type FieldMapping struct {
	Code  string `bson:"code"`
	Alias string `bson:"alias"`
}

// FieldNameMapping defines the mapping between codes and aliases
var FieldNameMapping = []FieldMapping{
	{Code: "WA", Alias: "water"},
	{Code: "WAU", Alias: "water_up"},
	{Code: "WAD", Alias: "water_do"},
	{Code: "WAO", Alias: "water_overflow"},
	{Code: "DR", Alias: "drain"},
	{Code: "DR1", Alias: "drain_1"},
	{Code: "DR2", Alias: "drain_2"},
	{Code: "DR3", Alias: "drain_3"},
	{Code: "SA", Alias: "salt"},
	{Code: "RA", Alias: "rain"},
	{Code: "TE", Alias: "temp"},
	{Code: "VO", Alias: "volt"},
	{Code: "WP", Alias: "water_proof"},
	{Code: "WP1", Alias: "water_proof_1"},
	{Code: "WP2", Alias: "water_proof_2"},
	{Code: "WP3", Alias: "water_proof_3"},
	{Code: "WP4", Alias: "water_proof_4"},
	{Code: "TI", Alias: "tilt"},
	{Code: "TIS", Alias: "tilt_shift"},
}

// AliasToCode creates a map from alias to code
var AliasToCode map[string]string

// Global MongoDB connection and database (reused across events)
// Now moved to mongodb.go as MongoDatabase and MongoClient variables

const BATCH_SIZE = 1024

// EVENT_MAX_AGE_SECONDS is the maximum age of an event before it's considered stale
// Default: 86400 seconds (24 hours)
// Can be configured via MAX_EVENT_AGE_SECONDS environment variable
// Set to 0 to disable age checking
// Minimum enforced: 300 seconds (5 minutes)
var EVENT_MAX_AGE_SECONDS int64 = 86400

// MIN_EVENT_AGE_SECONDS is the minimum allowed event age (5 minutes)
const MIN_EVENT_AGE_SECONDS int64 = 300

func init() {
	AliasToCode = make(map[string]string)
	for _, mapping := range FieldNameMapping {
		AliasToCode[mapping.Alias] = mapping.Code
	}

	// Initialize logger first
	InitLogger()

	// Initialize config (debug flags)
	InitConfig()

	// Load glob patterns from environment
	InitFilePatterns()

	// Initialize MongoDB connection at startup
	InitMongoDB()

	// Load max event age configuration from environment
	initEventAgeConfig()
}

// initEventAgeConfig loads the maximum event age configuration from environment variables
func initEventAgeConfig() {
	maxAgeStr := os.Getenv("MAX_EVENT_AGE_SECONDS")
	if maxAgeStr == "" {
		GlobalLogger.Infof("MAX_EVENT_AGE_SECONDS not set, using default: %d seconds (24 hours)\n", EVENT_MAX_AGE_SECONDS)
		return
	}

	maxAge, err := strconv.ParseInt(maxAgeStr, 10, 64)
	if err != nil {
		GlobalLogger.Warnf("Invalid MAX_EVENT_AGE_SECONDS value '%s', using default: %d seconds\n", maxAgeStr, EVENT_MAX_AGE_SECONDS)
		return
	}

	// Enforce minimum age (5 minutes) unless explicitly disabled with 0
	if maxAge != 0 && maxAge < MIN_EVENT_AGE_SECONDS {
		GlobalLogger.Warnf("MAX_EVENT_AGE_SECONDS %d is too small (minimum: %d seconds / 5 minutes), using minimum\n", maxAge, MIN_EVENT_AGE_SECONDS)
		EVENT_MAX_AGE_SECONDS = MIN_EVENT_AGE_SECONDS
		hours := MIN_EVENT_AGE_SECONDS / 3600
		minutes := (MIN_EVENT_AGE_SECONDS % 3600) / 60
		seconds := MIN_EVENT_AGE_SECONDS % 60
		GlobalLogger.Infof("Event age limit set to: %d seconds (%dh %dm %ds) [minimum enforced]\n", MIN_EVENT_AGE_SECONDS, hours, minutes, seconds)
		return
	}

	EVENT_MAX_AGE_SECONDS = maxAge
	if maxAge == 0 {
		GlobalLogger.Infof("Event age checking disabled (MAX_EVENT_AGE_SECONDS=0)\n")
	} else {
		hours := maxAge / 3600
		minutes := (maxAge % 3600) / 60
		seconds := maxAge % 60
		GlobalLogger.Infof("Event age limit set to: %d seconds (%dh %dm %ds)\n", EVENT_MAX_AGE_SECONDS, hours, minutes, seconds)
	}
}

// isEventTooOld checks if an event is older than the configured threshold
// Returns true if event should be skipped, false if it should be processed
func isEventTooOld(eventTime time.Time) bool {
	if EVENT_MAX_AGE_SECONDS == 0 {
		// Age checking disabled
		return false
	}

	age := time.Since(eventTime)
	maxAge := time.Duration(EVENT_MAX_AGE_SECONDS) * time.Second

	return age > maxAge
}

// ExtractData extracts and formats data from CSV content
func ExtractData(filename string, content []byte) (map[string]interface{}, error) {
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	if len(lines) < 5 {
		return nil, fmt.Errorf("file %s: CSV has insufficient lines (got %d, need 5)", filename, len(lines))
	}

	// Parse meta line
	metaReader := csv.NewReader(strings.NewReader(lines[0]))
	meta, err := metaReader.Read()
	if err != nil {
		return nil, fmt.Errorf("file %s: failed to parse meta line: %w", filename, err)
	}

	// Parse columns line
	columnsReader := csv.NewReader(strings.NewReader(lines[1]))
	columns, err := columnsReader.Read()
	if err != nil {
		return nil, fmt.Errorf("file %s: failed to parse columns line: %w", filename, err)
	}

	// Parse CSV starting from line 4 (index 4)
	csvContent := strings.Join(lines[4:], "\n")
	csvReader := csv.NewReader(strings.NewReader(csvContent))
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("file %s: failed to parse CSV records: %w", filename, err)
	}

	return ExtractObject(filename, meta, columns, records)
}

// ExtractObject converts raw records to objects with proper formatting
func ExtractObject(filename string, meta []string, columns []string, data [][]string) (map[string]interface{}, error) {
	// "TOA5","T1","CR300","19531" -> CR300_19531
	if len(meta) < 4 {
		return nil, fmt.Errorf("file %s: meta data has insufficient fields (got %d, need 4)", filename, len(meta))
	}

	deviceID := fmt.Sprintf("%s_%s", meta[2], meta[3])
	var records []SensorRecord

	for _, row := range data {
		if len(row) < 2 {
			continue
		}

		// Parse timestamp
		t, err := time.ParseInLocation("2006-01-02 15:04:05", row[0], GlobalConfig.TimezoneLocation)
		if err != nil {
			GlobalLogger.Warnf("%s invalid time: %s", deviceID, row[0])
			continue
		}

		ts := t.Unix()
		n, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			GlobalLogger.Warnf("%s invalid n value: %s", deviceID, row[1])
			continue
		}

		record := SensorRecord{
			"_id": ts,
			"n":   n,
		}

		for i := 2; i < len(row) && i < len(columns); i++ {
			v, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				continue
			}

			k := columns[i]
			if field, exists := AliasToCode[k]; exists {
				record[field] = v
			} else {
				record[k] = v
			}
		}

		records = append(records, record)
	}

	return map[string]interface{}{
		"device_id": deviceID,
		"records":   records,
	}, nil
}

// ProcessCSVFile processes CSV file and inserts into MongoDB
// Uses the global MongoDatabase connection
// Special handling for HoAmChua_TramTT files
func ProcessCSVFile(ctx context.Context, bucket string, filename string) (int64, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return 0, fmt.Errorf("file %s: failed to create GCS client: %w", filename, err)
	}
	defer client.Close()

	bucketObj := client.Bucket(bucket)
	file := bucketObj.Object(filename)

	reader, err := file.NewReader(ctx)
	if err != nil {
		return 0, fmt.Errorf("file %s: failed to open GCS file (bucket: %s): %w", filename, bucket, err)
	}
	defer reader.Close()

	// Read file content
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return 0, fmt.Errorf("file %s: failed to read GCS file: %w", filename, err)
	}

	// Check if this is an AmChua file
	if IsAmChuaFile(filename) {
		return ProcessAmChuaFile(ctx, filename, buf.Bytes())
	}

	// Check if this is an BaRia file 
	if IsBariaFile(filename) {
		return ProcessBariaFile(ctx, filename, buf.Bytes())
	}

	// Extract and format data
	result, err := ExtractData(filename, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("file %s: %w", filename, err)
	}

	deviceID := result["device_id"].(string)
	records := result["records"].([]SensorRecord)

	// Find the box device
	box, err := FindBoxByDeviceID(ctx, deviceID)
	if err != nil {
		GlobalLogger.Warnf("file %s: %v\n", filename, err)
		return 0, nil
	}

	// Insert sensor records
	inserted, err := InsertSensorRecords(ctx, filename, deviceID, box, records)
	if err != nil {
		return 0, fmt.Errorf("file %s: %w", filename, err)
	}

	return inserted, nil
}

// copyToFailedFolder copies a failed file to the load_failed folder in GCS
// This helps with debugging and recovery of files that couldn't be processed
func copyToFailedFolder(ctx context.Context, bucket string, filename string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	bucketObj := client.Bucket(bucket)
	sourceObj := bucketObj.Object(filename)

	// Read the source file
	reader, err := sourceObj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to read source file: %w", err)
	}
	defer reader.Close()

	// Create destination path: load_failed/<original_filename>
	failedFilename := fmt.Sprintf("load_failed/%s", filename)
	destObj := bucketObj.Object(failedFilename)

	// Write to destination
	writer := destObj.NewWriter(ctx)
	if _, err := io.Copy(writer, reader); err != nil {
		return fmt.Errorf("failed to copy to load_failed folder: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close destination file: %w", err)
	}

	GlobalLogger.Infof("file %s: copied to load_failed folder for debugging\n", filename)
	return nil
}

// StorageObjectData represents the Cloud Storage event payload
type StorageObjectData struct {
	Name           string `json:"name"`
	Bucket         string `json:"bucket"`
	Metageneration string `json:"metageneration"`
	TimeCreated    string `json:"timeCreated"`
	Updated        string `json:"updated"`
}

// helloGCS handles Cloud Events from Cloud Storage
func helloGCS(ctx context.Context, ce cloudevents.Event) error {
	eventID := ce.ID()
	GlobalLogger.Infof("Event ID: %s\n", eventID)
	GlobalLogger.Infof("Event Type: %s\n", ce.Type())

	// Check event age to prevent processing old stale events
	eventTime := ce.Time()
	if !eventTime.IsZero() && isEventTooOld(eventTime) {
		age := time.Since(eventTime)
		maxAgeDisplay := EVENT_MAX_AGE_SECONDS / 3600
		GlobalLogger.Warnf("Event ID %s: Skipping - event is too old (%v, max: %d seconds / %d hours)\n", eventID, age, EVENT_MAX_AGE_SECONDS, maxAgeDisplay)
		return nil // Silently succeed to prevent retries
	}

	// Parse the Cloud Storage event data
	var data StorageObjectData
	if err := ce.DataAs(&data); err != nil {
		return fmt.Errorf("failed to parse event data: %w", err)
	}

	filename := data.Name
	bucketName := data.Bucket

	if filename == "" {
		return fmt.Errorf("missing file name in event")
	}

	if bucketName == "" {
		return fmt.Errorf("missing bucket in event")
	}

	GlobalLogger.Infof("Bucket: %s\n", bucketName)
	GlobalLogger.Infof("File: %s\n", filename)

	// Check allow and ignore patterns
	if !ShouldProcessFile(filename) {
		return nil
	}

	// Process the CSV file (using global MongoDB connection)
	_, err := ProcessCSVFile(ctx, bucketName, filename)
	if err != nil {
		// Copy failed file to load_failed folder for debugging
		if copyErr := copyToFailedFolder(ctx, bucketName, filename); copyErr != nil {
			GlobalLogger.Errorf("file %s: error copying to load_failed folder: %v\n", filename, copyErr)
		}
		GlobalLogger.Errorf("file processing error %s: %s", filename, err)
		return nil
	}

	GlobalLogger.Infof("file %s: processed successfully\n", filename)
	return nil
}

func init() {
	functions.CloudEvent("helloGCS", helloGCS)
}
