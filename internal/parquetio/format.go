package parquetio

import (
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func parquetColumnIndices(reader *pqarrow.FileReader, names []string, missing MissingColumnError) ([]int, error) {
	if len(names) == 0 {
		return nil, nil
	}
	schema, err := reader.Schema()
	if err != nil {
		return nil, err
	}
	indices := make([]int, 0, len(names))
	seen := make(map[int]struct{}, len(names))
	for _, name := range names {
		fields := schema.FieldIndices(name)
		if len(fields) == 0 {
			if missing != nil {
				return nil, missing(name, arrowutil.SchemaNames(schema))
			}
			return nil, &missingColumnError{name: name}
		}
		if _, exists := seen[fields[0]]; !exists {
			indices = append(indices, fields[0])
			seen[fields[0]] = struct{}{}
		}
	}
	sort.Ints(indices)
	return indices, nil
}

type missingColumnError struct {
	name string
}

func (e *missingColumnError) Error() string {
	return "golars: column not found: " + e.name
}

func parquetRequestedSchema(schema *arrow.Schema, indices []int) *arrow.Schema {
	fields := make([]arrow.Field, len(indices))
	for i, index := range indices {
		fields[i] = schema.Field(index)
	}
	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata)
}
