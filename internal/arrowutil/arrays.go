package arrowutil

import "github.com/apache/arrow-go/v18/arrow"

// ReleaseArrays releases every non-nil array in a borrowed column slice.
func ReleaseArrays(arrays []arrow.Array) {
	for _, arr := range arrays {
		if arr != nil {
			arr.Release()
		}
	}
}

// SchemaNames returns the field names in schema order.
func SchemaNames(schema *arrow.Schema) []string {
	fields := schema.Fields()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}

// FieldIndices returns every field position with the requested name.
func FieldIndices(fields []arrow.Field, name string) []int {
	var indices []int
	for i, field := range fields {
		if field.Name == name {
			indices = append(indices, i)
		}
	}
	return indices
}
