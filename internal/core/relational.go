package core

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func uniqueDataFrame(df *DataFrame, columns []string, engine *Engine) (*DataFrame, error) {
	batch, reader, err := wholeTableBatch(df.table, engine)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	defer batch.Release()
	indices := make([]int, 0, len(columns))
	if len(columns) == 0 {
		for i := 0; i < int(batch.NumCols()); i++ {
			indices = append(indices, i)
		}
	} else {
		for _, name := range columns {
			fields := batch.Schema().FieldIndices(name)
			if len(fields) == 0 {
				return nil, &ColumnNotFoundError{Name: name, Available: arrowutil.SchemaNames(batch.Schema())}
			}
			indices = append(indices, fields[0])
		}
	}
	seen := make(map[string]struct{}, batch.NumRows())
	rows := make([]int64, 0, batch.NumRows())
	for row := int64(0); row < batch.NumRows(); row++ {
		key, err := rowKey(batch, indices, row)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		rows = append(rows, row)
	}
	return subsetDataFrame(batch, rows, engine)
}

func rowKey(batch arrow.RecordBatch, columns []int, row int64) (string, error) {
	var key string
	for _, column := range columns {
		value, err := scalar.GetScalar(batch.Column(column), int(row))
		if err != nil {
			return "", err
		}
		if !value.IsValid() {
			key += "null|"
			continue
		}
		key += fmt.Sprintf("%d:%d:%s|", value.DataType().ID(), len(value.String()), value.String())
	}
	return key, nil
}

func subsetDataFrame(batch arrow.RecordBatch, rows []int64, engine *Engine) (*DataFrame, error) {
	fields := batch.Schema().Fields()
	builders := make([]array.Builder, len(fields))
	for i, field := range fields {
		builders[i] = array.NewBuilder(engine.allocator, field.Type)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for _, row := range rows {
		for column, builder := range builders {
			if err := appendArrayValue(builder, batch.Column(column), int(row)); err != nil {
				return nil, err
			}
		}
	}
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}
	defer arrowutil.ReleaseArrays(arrays)
	table := array.NewTableFromSlice(batch.Schema(), arrowutil.ArraysAsSlices(arrays))
	return newDataFrameWithTable(table, engine), nil
}

type listView struct {
	values arrow.Array
	start  int64
	end    int64
	valid  bool
}

func listElements(values arrow.Array, row int) (listView, error) {
	switch values := values.(type) {
	case *array.List:
		if !values.IsValid(row) {
			return listView{}, nil
		}
		start, end := values.ValueOffsets(row)
		return listView{values: values.ListValues(), start: start, end: end, valid: true}, nil
	case *array.LargeList:
		if !values.IsValid(row) {
			return listView{}, nil
		}
		start, end := values.ValueOffsets(row)
		return listView{values: values.ListValues(), start: start, end: end, valid: true}, nil
	case *array.FixedSizeList:
		if !values.IsValid(row) {
			return listView{}, nil
		}
		start, end := values.ValueOffsets(row)
		return listView{values: values.ListValues(), start: start, end: end, valid: true}, nil
	case *array.ListView:
		if !values.IsValid(row) {
			return listView{}, nil
		}
		start, end := values.ValueOffsets(row)
		return listView{values: values.ListValues(), start: start, end: end, valid: true}, nil
	case *array.LargeListView:
		if !values.IsValid(row) {
			return listView{}, nil
		}
		start, end := values.ValueOffsets(row)
		return listView{values: values.ListValues(), start: start, end: end, valid: true}, nil
	default:
		return listView{}, fmt.Errorf("%w: explode requires a list column, got %s", ErrTypeMismatch, values.DataType())
	}
}

func explodeDataFrame(df *DataFrame, columns []string, engine *Engine) (*DataFrame, error) {
	batch, reader, err := wholeTableBatch(df.table, engine)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	defer batch.Release()
	explodeColumns := make(map[int]struct{}, len(columns))
	for _, name := range columns {
		indices := batch.Schema().FieldIndices(name)
		if len(indices) == 0 {
			return nil, &ColumnNotFoundError{Name: name, Available: arrowutil.SchemaNames(batch.Schema())}
		}
		explodeColumns[indices[0]] = struct{}{}
	}
	rows := make([]struct {
		row    int
		offset int64
		valid  bool
	}, 0)
	for row := 0; row < int(batch.NumRows()); row++ {
		length := int64(1)
		views := make(map[int]listView, len(explodeColumns))
		for column := range explodeColumns {
			view, viewErr := listElements(batch.Column(column), row)
			if viewErr != nil {
				return nil, viewErr
			}
			views[column] = view
			if view.valid {
				current := view.end - view.start
				if current > length {
					length = current
				}
			}
		}
		for offset := int64(0); offset < length; offset++ {
			rows = append(rows, struct {
				row    int
				offset int64
				valid  bool
			}{row: row, offset: offset, valid: true})
		}
	}
	fields := append([]arrow.Field(nil), batch.Schema().Fields()...)
	builders := make([]array.Builder, len(fields))
	for i, field := range fields {
		if _, exploding := explodeColumns[i]; exploding {
			elementType, typeErr := listElementType(field.Type)
			if typeErr != nil {
				return nil, typeErr
			}
			fields[i].Type = elementType
			field.Type = elementType
		}
		builders[i] = array.NewBuilder(engine.allocator, field.Type)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for _, output := range rows {
		for column, builder := range builders {
			if _, exploding := explodeColumns[column]; !exploding {
				if err := appendArrayValue(builder, batch.Column(column), output.row); err != nil {
					return nil, err
				}
				continue
			}
			view, viewErr := listElements(batch.Column(column), output.row)
			if viewErr != nil {
				return nil, viewErr
			}
			if !view.valid || view.start+output.offset >= view.end {
				builder.AppendNull()
				continue
			}
			if err := appendArrayValue(builder, view.values, int(view.start+output.offset)); err != nil {
				return nil, err
			}
		}
	}
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}
	defer arrowutil.ReleaseArrays(arrays)
	table := array.NewTableFromSlice(arrow.NewSchema(fields, nil), arrowutil.ArraysAsSlices(arrays))
	return newDataFrameWithTable(table, engine), nil
}

func listElementType(dtype arrow.DataType) (arrow.DataType, error) {
	switch dtype := dtype.(type) {
	case *arrow.ListType:
		return dtype.Elem(), nil
	case *arrow.LargeListType:
		return dtype.Elem(), nil
	case *arrow.FixedSizeListType:
		return dtype.Elem(), nil
	case *arrow.ListViewType:
		return dtype.Elem(), nil
	case *arrow.LargeListViewType:
		return dtype.Elem(), nil
	default:
		return nil, fmt.Errorf("%w: explode requires a list column, got %s", ErrTypeMismatch, dtype)
	}
}

func unnestDataFrame(df *DataFrame, columns []string, engine *Engine) (*DataFrame, error) {
	batch, reader, err := wholeTableBatch(df.table, engine)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	defer batch.Release()
	requested := make(map[int]struct{}, len(columns))
	for _, name := range columns {
		indices := batch.Schema().FieldIndices(name)
		if len(indices) == 0 {
			return nil, &ColumnNotFoundError{Name: name, Available: arrowutil.SchemaNames(batch.Schema())}
		}
		requested[indices[0]] = struct{}{}
	}
	fields := make([]arrow.Field, 0)
	builders := make([]array.Builder, 0)
	used := make(map[string]struct{})
	for column, field := range batch.Schema().Fields() {
		if _, ok := requested[column]; !ok {
			fields = append(fields, field)
			builders = append(builders, array.NewBuilder(engine.allocator, field.Type))
			used[field.Name] = struct{}{}
			continue
		}
		if _, ok := batch.Column(column).(*array.Struct); !ok {
			return nil, fmt.Errorf("%w: unnest requires struct column %q", ErrTypeMismatch, field.Name)
		}
		structType := field.Type.(*arrow.StructType)
		for _, childField := range structType.Fields() {
			if _, exists := used[childField.Name]; exists {
				return nil, fmt.Errorf("%w: unnest would create duplicate column %q", ErrInvalidPlan, childField.Name)
			}
			used[childField.Name] = struct{}{}
			fields = append(fields, childField)
			builders = append(builders, array.NewBuilder(engine.allocator, childField.Type))
		}
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for row := 0; row < int(batch.NumRows()); row++ {
		outputColumn := 0
		for column := range batch.Schema().Fields() {
			if _, ok := requested[column]; !ok {
				if err := appendArrayValue(builders[outputColumn], batch.Column(column), row); err != nil {
					return nil, err
				}
				outputColumn++
				continue
			}
			structArray := batch.Column(column).(*array.Struct)
			for child := 0; child < structArray.NumField(); child++ {
				if !structArray.IsValid(row) {
					builders[outputColumn].AppendNull()
				} else if err := appendArrayValue(builders[outputColumn], structArray.Field(child), row); err != nil {
					return nil, err
				}
				outputColumn++
			}
		}
	}
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}
	defer arrowutil.ReleaseArrays(arrays)
	table := array.NewTableFromSlice(arrow.NewSchema(fields, nil), arrowutil.ArraysAsSlices(arrays))
	return newDataFrameWithTable(table, engine), nil
}

func appendArrayValue(builder array.Builder, values arrow.Array, row int) error {
	value, err := scalar.GetScalar(values, row)
	if err != nil {
		return err
	}
	return scalar.Append(builder, value)
}
