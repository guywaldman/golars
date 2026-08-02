package core

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestRowsAndToDicts(t *testing.T) {
	id, err := NewSeries("id", []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	name, err := NewSeries("name", []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	defer name.Close()
	df, err := NewDataFrame(id, name)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	want := []Row{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "name": "b"},
		{"id": int64(3), "name": "c"},
	}
	rows, err := df.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("Rows()=%v, want %v", rows, want)
	}
	dicts, err := df.ToDicts()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dicts, want) {
		t.Fatalf("ToDicts()=%v, want %v", dicts, want)
	}
}

func TestLazyRowsHeightAndForEachRow(t *testing.T) {
	id, err := NewSeries("id", []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	df, err := NewDataFrame(id)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	lf := df.Lazy().Limit(2)
	defer lf.Close()

	if height, err := lf.Height(context.Background()); err != nil || height != 2 {
		t.Fatalf("Height()=(%d,%v), want (2,nil)", height, err)
	}
	rows, err := lf.ToDicts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []Row{{"id": int64(1)}, {"id": int64(2)}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("ToDicts()=%v, want %v", rows, want)
	}

	var seen int
	errStop := errors.New("stop")
	err = lf.ForEachRow(context.Background(), func(row Row) error {
		seen++
		if seen == 1 {
			return errStop
		}
		return nil
	})
	if !errors.Is(err, errStop) || seen != 1 {
		t.Fatalf("ForEachRow() error=%v rows=%d, want stop after one row", err, seen)
	}
}
