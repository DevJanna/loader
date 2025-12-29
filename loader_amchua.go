package loader

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type Metric struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

// AmChuaBox represents a box configuration for HoAmChua_TramTT files
type AmChuaBox struct {
	ID      string   `json:"id"`
	Metrics []Metric `json:"metrics"`
}

// AmChuaBoxes defines the boxes for HoAmChua_TramTT processing
var AmChuaBoxes = []AmChuaBox{
	{
		ID: "P7IBJJ87",
		Metrics: []Metric{{
			Code: "RA",
			Name: "rain_1",
		}},
	},
	{
		ID: "LPDNWOUM",
		Metrics: []Metric{{
			Code: "WAU",
			Name: "waterup",
		}},
	},
	{
		ID: "YLW16RKW",
		Metrics: []Metric{{
			Code: "DR1",
			Name: "TaperValve",
		}},
	},
	{
		ID: "RIENVHK4",
		Metrics: []Metric{{
			Code: "DR1",
			Name: "drain_1",
		}, {
			Code: "DR2",
			Name: "drain_2",
		}},
	},
	{
		ID: "T5CRC2FV",
		Metrics: []Metric{{
			Code: "RA",
			Name: "rain_2",
		}},
	},
}

// IsAmChuaFile checks if the filename is a HoAmChua_TramTT file
func IsAmChuaFile(filename string) bool {
	return strings.Contains(filename, "HoAmChua_TramTT")
}

// parseFilenameForTimestamp extracts the date and time from the filename,
// converts it to a UTC Unix timestamp, and rounds it to the nearest minute.
func parseFilenameForTimestamp(filename string) (int64, error) {
	// The expected format for the date/time part: YYYYMMDDhhmmss
	const timeLayout = "20060102150405"

	// Use the last path element (the actual filename) so we extract just the timestamp part.
	// e.g. "upload/HoAmChua_TramTT/2025/11/29/20251129190000.txt" -> "20251129190000.txt"
	filename = filepath.Base(filename)
	base := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))

	// Ensure the remaining string length matches the layout length
	if len(base) != len(timeLayout) {
		return 0, fmt.Errorf("filename component '%s' has incorrect length; expected %d digits", base, len(timeLayout))
	}

	// 2. Parse the time string
	t, err := time.ParseInLocation(timeLayout, base, GlobalConfig.TimezoneLocation)
	if err != nil {
		return 0, fmt.Errorf("failed to parse time string '%s': %w", base, err)
	}

	// 3. Round (truncate) the time to the minute.
	// This sets seconds and nanoseconds to zero, effectively finding the
	// timestamp for the start of the minute.
	tRounded := t.Truncate(time.Minute)

	// 4. Return the Unix timestamp (seconds since epoch)
	return tRounded.Unix(), nil
}

// ProcessAmChuaFile processes a HoAmChua_TramTT file
// Reads tab-separated key-value pairs and inserts them into MongoDB for each configured box
func ProcessAmChuaFile(ctx context.Context, filename string, content []byte) (int64, error) {
	// Convert timestamp to Unix
	ts, err := parseFilenameForTimestamp(filename)
	if err != nil {
		return 0, fmt.Errorf("file %s: %w", filename, err)
	}

	// Parse tab-separated or space-separated values from content
	contentStr := strings.TrimSpace(string(content))
	lines := strings.Split(contentStr, "\n")

	// Build key-value map from lines
	valueMap := make(map[string]float64)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)

		if len(parts) >= 2 {
			key := strings.TrimSpace(parts[0])
			value, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil {
				GlobalLogger.Infof("parse value failed %s %s: %s\n", filename, key, parts[1])
				return 0, nil
			}
			valueMap[key] = value
		}
	}

	GlobalLogger.Infof("file %s: processing with timestamp %d (%s)\n", filename, ts, time.Unix(ts, 0).UTC().Format("2006-01-02 15:04:05"))

	// Process for each configured box
	insertedCount := int64(0)
	now := time.Now().Unix()

	for _, box := range AmChuaBoxes {
		// Build document for this box
		doc := bson.M{
			"_id": ts,
			"c":   now, // current timestamp
		}

		// Add metrics from valueMap
		for _, metric := range box.Metrics {
			if value, exists := valueMap[metric.Name]; exists {
				doc[metric.Code] = value
			} else {
				doc[metric.Code] = 0
			}
		}

		// Insert into collection
		colName := fmt.Sprintf("sensor_data_%s", box.ID)
		collection := MongoDatabase.Collection(colName)

		// Print record before insert if debug flag is enabled
		if GlobalConfig != nil && GlobalConfig.Debug {
			GlobalLogger.Infof("file %s: [DEBUG] inserting record into collection %s: %+v", filename, box.ID, doc)
		}

		_, err := collection.InsertOne(ctx, doc)
		if err != nil {
			// Check if it's a duplicate key error (which we can ignore)
			if strings.Contains(err.Error(), "duplicate key") {
				GlobalLogger.Warnf("file %s: duplicate record for box %s at timestamp %d\n", filename, box.ID, ts)
				continue
			}
			GlobalLogger.Warnf("file %s: error inserting record for box %s: %v\n", filename, box.ID, err)
			continue
		}

		insertedCount++
		GlobalLogger.Debugf("file %s: inserted record into sensor_data_%s\n", filename, box.ID)
	}

	GlobalLogger.Infof("file %s: inserted %d records from AmChua file\n", filename, insertedCount)
	return insertedCount, nil
}
