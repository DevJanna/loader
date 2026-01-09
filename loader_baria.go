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

type BoxBR struct {
	ID      string     `json:"id"`
	Path    string     `json:"path"`
	Metrics []Metric `json:"metrics"`
}

var BoxesBR = []BoxBR{
	{
		ID:   "S83FIGA0",
		Path: "HoSongRay_KenhSongRay",
		Metrics: []Metric{
			{Code: "WAU", Name: "MNK"},
			{Code: "DR1", Name: "Domocong"},
		},
	},
	{
		ID:   "A3M6KKN6",
		Path: "HoSongRay_HaLuu",
		Metrics: []Metric{
			{Code: "WAD", Name: "MNH"},
		},
	},
	{
		ID:   "L3Q7718O",
		Path: "HoDaBang_TramDoMN+MoCong",
		Metrics: []Metric{
			{Code: "WAU", Name: "MNH"},
			{Code: "DR1", Name: "Domocong"},
		},
	},
	{
		ID:   "45J68MTA",
		Path: "HoXuyenMoc_TramDoMN+MoCong",
		Metrics: []Metric{
			{Code: "WAU", Name: "MNH"},
			{Code: "DR1", Name: "Domocong_0"},
		},
	},
	{
		ID:   "V51HBHL0",
		Path: "HoSuoiCac_TramDoMN+MoCong",
		Metrics: []Metric{
			{Code: "WAU", Name: "MNH"},
			{Code: "DR1", Name: "Domocong_0"},
		},
	},
	{
		ID:   "2Y63JKZ2",
		Path: "HoKimLong_TramDoMoCong",
		Metrics: []Metric{
			{Code: "DR1", Name: "Domocong_KimLong_0"},
		},
	},
	{
		ID:   "KEHF4MGC",
		Path: "HoKimLong_TramDoMucNuoc",
		Metrics: []Metric{
			{Code: "WAU", Name: "MNH_KimLong"},
		},
	},
	{
		ID:   "JRUTJ9KX",
		Path: "HoChauPha",
		Metrics: []Metric{
			{Code: "WAU", Name: "MNH_ChauPha"},
			{Code: "DR1", Name: "Domocong"},
		},
	},
}

//
// ===== MATCH BOX BY PATH =====
//

func IsBariaFile(filename string) bool {
	box := MatchBariaBox(filename)
	return box != nil
}

func MatchBariaBox(filename string) *BoxBR {
	path := filepath.ToSlash(filename)

	for _, box := range BoxesBR {
		if strings.Contains(path, box.Path) {
			return &box
		}
	}
	return nil
}

//
// ===== PROCESS FILE =====
//

func ParseBariaTimestampFromFilename(filename string) (int64, error) {
	base := filepath.Base(filename)
	base = strings.TrimSuffix(base, filepath.Ext(base))

	idx := strings.LastIndex(base, "_")
	if idx == -1 {
		return 0, fmt.Errorf("invalid baria filename: %s", base)
	}

	tsStr := base[idx+1:] // 20251227200009

	t, err := time.ParseInLocation(
		"20060102150405",
		tsStr,
		GlobalConfig.TimezoneLocation,
	)
	if err != nil {
		return 0, err
	}

	return t.Truncate(time.Minute).Unix(), nil
}

func ProcessBariaFile(
	ctx context.Context,
	filename string,
	content []byte,
) (int64, error) {

	// 1. Match box theo path
	box := MatchBariaBox(filename)

	// 2. Parse timestamp từ filename (sau dấu _)
	ts, err := ParseBariaTimestampFromFilename(filename)
	if err != nil {
		return 0, fmt.Errorf("file %s: %w", filename, err)
	}

	// 3. Parse content (TAB-separated)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	valueMap := make(map[string]float64)

	for _, line := range lines {
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		valStr := strings.TrimSpace(parts[1])

		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			GlobalLogger.Warnf("file %s: parse failed %s=%s", filename, key, valStr)
			continue
		}

		valueMap[key] = v
	}

	// 4. Build document
	doc := bson.M{
		"_id": ts,
		"c":   time.Now().Unix(),
	}

	for _, m := range box.Metrics {
		if v, ok := valueMap[m.Name]; ok {
			doc[m.Code] = v
		} else {
			doc[m.Code] = 0
		}
	}

	// 5. Insert Mongo
	col := MongoDatabase.Collection(
		fmt.Sprintf("sensor_data_%s", box.ID),
	)

	if GlobalConfig != nil && GlobalConfig.Debug {
		GlobalLogger.Infof("[DEBUG] insert %s → %s : %+v", filename, box.ID, doc)
	}

	_, err = col.InsertOne(ctx, doc)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			GlobalLogger.Warnf(
				"file %s: duplicate ts %d for box %s",
				filename, ts, box.ID,
			)
			return 0, nil // chặn CSV
		}
		return 0, err
	}

	GlobalLogger.Infof("file %s: inserted record for box %s", filename, box.ID)
	return 1, nil
}
