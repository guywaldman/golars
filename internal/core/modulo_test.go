package core

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestModuloFilterKeepsEvenRows(t *testing.T) {
	ids, err := NewSeries("id", []int64{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	df, err := NewDataFrame(ids)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	even, err := df.Filter(Col("id").Mod(Lit(int64(2))).Eq(Lit(int64(0))))
	if err != nil {
		t.Fatal(err)
	}
	defer even.Close()

	rows, err := even.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["id"] != int64(2) || rows[1]["id"] != int64(4) {
		t.Fatalf("rows=%v, want [{id:2} {id:4}]", rows)
	}
}

func TestModuloProjectionPreservesType(t *testing.T) {
	ids, err := NewSeries("id", []int32{5, 6, 7})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	df, err := NewDataFrame(ids)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	out, err := df.Select(Col("id").Mod(Lit(int32(3))).Alias("remainder"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if out.Schema().Fields()[0].Type.ID() != arrow.INT32 {
		t.Fatalf("type=%s, want int32", out.Schema().Fields()[0].Type)
	}
	rows, err := out.Rows()
	if err != nil {
		t.Fatal(err)
	}
	want := []int32{2, 0, 1}
	if len(rows) != len(want) {
		t.Fatalf("rows=%v, want %v", rows, want)
	}
	for i, row := range rows {
		if row["remainder"] != want[i] {
			t.Fatalf("row %d=%v, want %d", i, row["remainder"], want[i])
		}
	}
}

func TestModuloByZeroReturnsError(t *testing.T) {
	ids, err := NewSeries("id", []int64{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	df, err := NewDataFrame(ids)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	if _, err := df.Filter(Col("id").Mod(Lit(int64(0))).Eq(Lit(int64(0)))); err == nil {
		t.Fatal("expected modulo by zero error")
	}
}
