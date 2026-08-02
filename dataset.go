package golars

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
	"github.com/guywaldman/golars/internal/storageuri"
)

type DatasetMode uint8

const (
	DatasetCreate DatasetMode = iota
	DatasetAppend
)

type DatasetWriteOption func(*datasetWriteConfig)

type datasetWriteConfig struct {
	partitionBy []string
	mode        DatasetMode
	rowsPerFile int64
}

func WithPartitionBy(columns ...string) DatasetWriteOption {
	return func(config *datasetWriteConfig) { config.partitionBy = append([]string(nil), columns...) }
}
func WithDatasetMode(mode DatasetMode) DatasetWriteOption {
	return func(config *datasetWriteConfig) { config.mode = mode }
}
func WithRowsPerFile(rows int64) DatasetWriteOption {
	return func(config *datasetWriteConfig) {
		if rows > 0 {
			config.rowsPerFile = rows
		}
	}
}

// WriteParquetDataset writes a Hive-style local Parquet dataset. Create mode
// is non-destructive and writes _SUCCESS plus a small schema manifest; Append
// adds new part files without replacing existing objects.
func WriteParquetDataset(ctx context.Context, df *DataFrame, destination string, options ...DatasetWriteOption) error {
	if err := df.ensureOpen(); err != nil {
		return err
	}
	config := datasetWriteConfig{rowsPerFile: df.engine.batchSize}
	for _, option := range options {
		option(&config)
	}
	if err := validatePartitionNames(config.partitionBy); err != nil {
		return err
	}
	if storageuri.Scheme(destination) != "" {
		return fmt.Errorf("golars: Parquet dataset sinks currently require a local directory")
	}
	if config.mode == DatasetCreate {
		if _, err := os.Stat(destination); err == nil {
			return fmt.Errorf("golars: dataset exists: %q (use DatasetAppend to add files)", destination)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return err
	}
	table, err := df.ArrowTable()
	if err != nil {
		return err
	}
	defer table.Release()
	reader := array.NewTableReader(table, table.NumRows())
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return err
		}
		return finishDataset(destination, df.Schema(), config.mode)
	}
	batch := reader.RecordBatch()
	partitionArrays := make([]arrow.Array, len(config.partitionBy))
	for i, name := range config.partitionBy {
		indices := batch.Schema().FieldIndices(name)
		if len(indices) == 0 {
			return &ColumnNotFoundError{Name: name, Available: arrowutil.SchemaNames(batch.Schema())}
		}
		partitionArrays[i] = batch.Column(indices[0])
	}
	groups := make(map[string][]int)
	for row := 0; row < int(batch.NumRows()); row++ {
		parts := make([]string, len(partitionArrays))
		for i, values := range partitionArrays {
			value, err := scalar.GetScalar(values, row)
			if err != nil {
				return err
			}
			parts[i] = value.String()
		}
		groups[strings.Join(parts, "\x1f")] = append(groups[strings.Join(parts, "\x1f")], row)
	}
	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)
	groupIndex := 0
	if config.mode == DatasetAppend {
		groupIndex = nextDatasetPartIndex(destination)
	}
	for _, groupKey := range groupKeys {
		parts := strings.Split(groupKey, "\x1f")
		pathParts := []string{destination}
		for i, name := range config.partitionBy {
			pathParts = append(pathParts, name+"="+sanitizeHiveValue(parts[i]))
		}
		partitionDir := filepath.Join(pathParts...)
		if err := os.MkdirAll(partitionDir, 0o755); err != nil {
			return err
		}
		indices := groups[groupKey]
		for offset := int64(0); offset < int64(len(indices)); offset += config.rowsPerFile {
			end := offset + config.rowsPerFile
			if end > int64(len(indices)) {
				end = int64(len(indices))
			}
			selected := make([]int, end-offset)
			for i := range selected {
				selected[i] = indices[offset+int64(i)]
			}
			part, _, err := reorderRecord(ctx, df.engine, batch, selected)
			if err != nil {
				return err
			}
			partForWrite := dropRecordColumns(part, config.partitionBy)
			part.Release()
			partTable := array.NewTableFromRecords(partForWrite.Schema(), []arrow.RecordBatch{partForWrite})
			partForWrite.Release()
			partDF, err := DataFrameFromArrow(partTable)
			partTable.Release()
			if err != nil {
				return err
			}
			filename := filepath.Join(partitionDir, fmt.Sprintf("part-%05d.parquet", groupIndex))
			if err := WriteParquet(ctx, partDF, filename); err != nil {
				partDF.Close()
				return err
			}
			partDF.Close()
			groupIndex++
		}
	}
	return finishDataset(destination, df.Schema(), config.mode)
}

func dropRecordColumns(record arrow.RecordBatch, names []string) arrow.RecordBatch {
	drop := make(map[string]struct{}, len(names))
	for _, name := range names {
		drop[name] = struct{}{}
	}
	fields := make([]arrow.Field, 0, record.NumCols())
	columns := make([]arrow.Array, 0, record.NumCols())
	for i := 0; i < int(record.NumCols()); i++ {
		field := record.Schema().Field(i)
		if _, ok := drop[field.Name]; ok {
			continue
		}
		column := record.Column(i)
		column.Retain()
		columns = append(columns, column)
		fields = append(fields, field)
	}
	out := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, record.NumRows())
	arrowutil.ReleaseArrays(columns)
	return out
}

func finishDataset(destination string, schema Schema, mode DatasetMode) error {
	manifest := struct {
		Format string   `json:"format"`
		Fields []string `json:"fields"`
	}{Format: "parquet", Fields: schema.Names()}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(destination, "_golars_schema.json"), data, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(destination, "_SUCCESS"), nil, 0o644)
}

func sanitizeHiveValue(value string) string {
	if value == "" || value == "null" {
		return "__HIVE_DEFAULT_PARTITION__"
	}
	value = strings.ReplaceAll(value, string(filepath.Separator), "_")
	return strings.ReplaceAll(value, "..", "_")
}

func nextDatasetPartIndex(destination string) int {
	next := 0
	_ = filepath.WalkDir(destination, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		if !strings.HasPrefix(base, "part-") || !strings.HasSuffix(base, ".parquet") {
			return nil
		}
		var index int
		if _, scanErr := fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(base, "part-"), ".parquet"), "%d", &index); scanErr == nil && index >= next {
			next = index + 1
		}
		return nil
	})
	return next
}
