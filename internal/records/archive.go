package records

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/types"
)

const (
	gzipLevel           = 6
	minCompressionRatio = 1.5
	retentionDays       = 30
)

type ArchiveManager struct {
	app core.App
}

func NewArchiveManager(app core.App) *ArchiveManager {
	return &ArchiveManager{app: app}
}

func (am *ArchiveManager) ArchiveOldRecords() error {
	cutoffDate := time.Now().UTC().AddDate(0, 0, -retentionDays)

	collections := []string{"system_stats", "container_stats"}
	recordTypes := []string{"1m", "10m", "20m", "120m", "480m"}

	for _, collection := range collections {
		for _, recordType := range recordTypes {
			if err := am.archiveCollectionType(collection, recordType, cutoffDate); err != nil {
				return fmt.Errorf("archive %s %s: %w", collection, recordType, err)
			}
		}
	}

	return nil
}

func (am *ArchiveManager) archiveCollectionType(collection, recordType string, cutoffDate time.Time) error {
	type systemID struct {
		System string `db:"system"`
	}
	var systems []systemID

	err := am.app.DB().
		Select("DISTINCT system").
		From(collection).
		Where(dbx.NewExp("type = {:type} AND created < {:cutoff}", dbx.Params{
			"type":   recordType,
			"cutoff": cutoffDate,
		})).
		All(&systems)
	if err != nil {
		return err
	}

	for _, sys := range systems {
		if err := am.archiveSystemData(collection, sys.System, recordType, cutoffDate); err != nil {
			return err
		}
	}

	return nil
}

func (am *ArchiveManager) archiveSystemData(collection, systemID, recordType string, cutoffDate time.Time) error {
	type rawRecord struct {
		ID      string         `db:"id"`
		Stats   []byte         `db:"stats"`
		Created types.DateTime `db:"created"`
	}
	var records []rawRecord

	err := am.app.DB().
		Select("id", "stats", "created").
		From(collection).
		Where(dbx.NewExp("system = {:system} AND type = {:type} AND created < {:cutoff}", dbx.Params{
			"system": systemID,
			"type":   recordType,
			"cutoff": cutoffDate,
		})).
		OrderBy("created").
		All(&records)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	monthlyData := make(map[string][]rawRecord)
	for _, rec := range records {
		month := rec.Created.Time().Format("2006-01")
		monthlyData[month] = append(monthlyData[month], rec)
	}

	for month, monthRecords := range monthlyData {
		if len(monthRecords) == 0 {
			continue
		}

		statsData := make([]map[string]interface{}, 0, len(monthRecords))
		for _, rec := range monthRecords {
			var stat map[string]interface{}
			if err := json.Unmarshal(rec.Stats, &stat); err != nil {
				continue
			}
			stat["_created"] = rec.Created.Time().Unix()
			statsData = append(statsData, stat)
		}

		jsonData, err := json.Marshal(statsData)
		if err != nil {
			return err
		}

		compressed, isCompressed, ratio := compressData(jsonData)

		if !isCompressed || ratio < minCompressionRatio {
			compressed = jsonData
			isCompressed = false
		}

		existing := struct {
			ID string `db:"id"`
		}{}
		err = am.app.DB().
			Select("id").
			From("stats_archive").
			Where(dbx.NewExp("system = {:system} AND month = {:month} AND type = {:type}", dbx.Params{
				"system": systemID,
				"month":  month,
				"type":   recordType,
			})).
			One(&existing)
		if err == nil && existing.ID != "" {
			continue
		}

		_, err = am.app.DB().
			Insert("stats_archive", dbx.Params{
				"system":     systemID,
				"month":      month,
				"type":       recordType,
				"data":       compressed,
				"compressed": isCompressed,
				"count":      len(monthRecords),
			}).
			Execute()
		if err != nil {
			return err
		}

		if isCompressed || ratio >= minCompressionRatio {
			ids := make([]string, 0, len(monthRecords))
			for _, rec := range monthRecords {
				ids = append(ids, rec.ID)
			}
			_, _ = am.app.DB().
				Delete(collection, dbx.NewExp("id IN ({:ids})", dbx.Params{"ids": ids})).
				Execute()
		}
	}

	return nil
}

func compressData(data []byte) ([]byte, bool, float64) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, gzipLevel)
	if err != nil {
		return data, false, 1.0
	}

	if _, err := writer.Write(data); err != nil {
		return data, false, 1.0
	}
	if err := writer.Close(); err != nil {
		return data, false, 1.0
	}

	compressed := buf.Bytes()
	ratio := float64(len(data)) / float64(len(compressed))

	return compressed, true, ratio
}

func decompressData(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func (am *ArchiveManager) QueryArchivedStats(systemID, recordType string, startTime, endTime time.Time) ([]map[string]interface{}, error) {
	startMonth := startTime.Format("2006-01")
	endMonth := endTime.Format("2006-01")

	type archiveRecord struct {
		Month      string `db:"month"`
		Data       []byte `db:"data"`
		Compressed bool   `db:"compressed"`
	}
	var archiveRecords []archiveRecord

	err := am.app.DB().
		Select("month", "data", "compressed").
		From("stats_archive").
		Where(dbx.NewExp("system = {:system} AND type = {:type} AND month >= {:start} AND month <= {:end}", dbx.Params{
			"system": systemID,
			"type":   recordType,
			"start":  startMonth,
			"end":    endMonth,
		})).
		OrderBy("month").
		All(&archiveRecords)
	if err != nil {
		return nil, err
	}

	var allStats []map[string]interface{}

	for _, rec := range archiveRecords {
		var jsonData []byte
		if rec.Compressed {
			var err error
			jsonData, err = decompressData(rec.Data)
			if err != nil {
				continue
			}
		} else {
			jsonData = rec.Data
		}

		var stats []map[string]interface{}
		if err := json.Unmarshal(jsonData, &stats); err != nil {
			continue
		}

		for _, stat := range stats {
			if createdTs, ok := stat["_created"].(float64); ok {
				createdTime := time.Unix(int64(createdTs), 0)
				if (createdTime.After(startTime) || createdTime.Equal(startTime)) &&
					(createdTime.Before(endTime) || createdTime.Equal(endTime)) {
					allStats = append(allStats, stat)
				}
			}
		}
	}

	return allStats, nil
}
