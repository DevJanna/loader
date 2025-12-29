package loader

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Box represents a device/box document in MongoDB
type Box struct {
	ID       interface{} `bson:"_id"`
	DeviceID string      `bson:"device_id"`
}

// SensorRecord represents a sensor data record
type SensorRecord map[string]interface{}

// MongoDB connection and database (reused across events)
// These are exported to be accessible from main.go during init
var MongoClient *mongo.Client
var MongoDatabase *mongo.Database

// InitMongoDB initializes the global MongoDB connection
// This is called once at startup and reused for all events
func InitMongoDB() {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		GlobalLogger.Fatal("missing DB_URL env variable")
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		GlobalLogger.Fatal("missing DB_NAME env variable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	MongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(dbURL))
	if err != nil {
		GlobalLogger.Fatalf("failed to connect to MongoDB: %v", err)
	}

	// Test the connection
	err = MongoClient.Ping(ctx, nil)
	if err != nil {
		GlobalLogger.Fatalf("failed to ping MongoDB: %v", err)
	}

	MongoDatabase = MongoClient.Database(dbName)
	GlobalLogger.Infof("MongoDB connection initialized for database: %s", dbName)
}

// GetInt64FromInterface safely converts interface{} to int64
// Handles int, int32, int64, and float64 types
func GetInt64FromInterface(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("unsupported type for int64 conversion: %T", v)
	}
}

// InsertBatch inserts a batch of records, ignoring duplicates
func InsertBatch(ctx context.Context, col *mongo.Collection, data []SensorRecord) (int64, error) {
	if len(data) < 1 {
		return 0, nil
	}

	var docs []interface{}
	for _, record := range data {
		docs = append(docs, record)
	}

	// Print records before insert if debug flag is enabled
	if GlobalConfig != nil && GlobalConfig.Debug {
		for i, record := range data {
			GlobalLogger.Infof("[DEBUG] InsertBatch record [%d/%d]: %+v", i+1, len(data), record)
		}
	}

	result, err := col.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		// Check if it's a duplicate key error
		if strings.Contains(err.Error(), "E11000 duplicate key error") {
			return 0, nil
		}
		return 0, err
	}

	return int64(len(result.InsertedIDs)), nil
}

// InsertIgnoreDuplicate inserts all records with duplicate handling
func InsertIgnoreDuplicate(ctx context.Context, col *mongo.Collection, data []SensorRecord) (int64, error) {
	var inserted int64

	for i := 0; i < len(data); i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > len(data) {
			end = len(data)
		}

		arr := data[i:end]

		// Log batch processing if debug flag is enabled
		if GlobalConfig != nil && GlobalConfig.Debug {
			GlobalLogger.Infof("[DEBUG] InsertIgnoreDuplicate processing batch: %d-%d (total: %d)", i, end, len(data))
		}

		count, err := InsertBatch(ctx, col, arr)
		if err != nil {
			return inserted, err
		}
		inserted += count
	}

	return inserted, nil
}

// FindBoxByDeviceID finds a box document by device_id
// Returns the box or an error if not found
func FindBoxByDeviceID(ctx context.Context, deviceID string) (*Box, error) {
	boxCol := MongoDatabase.Collection("box")
	var box Box
	err := boxCol.FindOne(ctx, bson.M{"device_id": deviceID}).Decode(&box)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("unknown device_id %s", deviceID)
		}
		return nil, fmt.Errorf("failed to find box for device_id %s: %w", deviceID, err)
	}
	return &box, nil
}

// GetLatestRecord retrieves the latest (most recent by _id) record from a collection
// Returns the record or nil if no records exist
func GetLatestRecord(ctx context.Context, col *mongo.Collection) (*SensorRecord, error) {
	opts := options.FindOne().SetSort(bson.M{"_id": -1})
	var maxTs SensorRecord
	err := col.FindOne(ctx, bson.M{}, opts).Decode(&maxTs)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query latest record: %w", err)
	}
	return &maxTs, nil
}

// FilterNewRecords filters records to keep only those with _id greater than maxID
// Used to avoid re-inserting old data
func FilterNewRecords(records []SensorRecord, maxID int64) ([]SensorRecord, error) {
	var newRecords []SensorRecord
	for _, r := range records {
		rID, err := GetInt64FromInterface(r["_id"])
		if err != nil {
			GlobalLogger.Warnf("warning: invalid record _id type: %v", err)
			continue
		}
		if rID > maxID {
			newRecords = append(newRecords, r)
		}
	}
	return newRecords, nil
}

// InsertSensorRecords inserts sensor records for a device, filtering by latest timestamp
// Returns the number of records inserted
func InsertSensorRecords(ctx context.Context, filename string, deviceID string, box *Box, records []SensorRecord) (int64, error) {
	colName := fmt.Sprintf("sensor_data_%s", box.ID)
	col := MongoDatabase.Collection(colName)

	// Get the latest record
	maxTs, err := GetLatestRecord(ctx, col)
	if err != nil {
		return 0, fmt.Errorf("file %s: %w", filename, err)
	}

	var toInsert []SensorRecord
	if maxTs != nil {
		maxID, err := GetInt64FromInterface((*maxTs)["_id"])
		if err != nil {
			GlobalLogger.Warnf("warning: invalid max_id type: %v", err)
			toInsert = records
		} else {
			// Filter records to insert only new ones
			toInsert, err = FilterNewRecords(records, maxID)
			if err != nil {
				return 0, fmt.Errorf("file %s: %w", filename, err)
			}
		}
	} else {
		toInsert = records
	}

	// Insert records
	inserted, err := InsertIgnoreDuplicate(ctx, col, toInsert)
	if err != nil {
		return 0, fmt.Errorf("file %s: failed to insert records into %s: %w", filename, colName, err)
	}

	GlobalLogger.Infof("file %s: inserted %d records from device %s into %s", filename, inserted, deviceID, colName)
	return inserted, nil
}
