package core

import (
	"context"
	"testing"
)

func TestLazyBranchesRemainIndependentAfterParentClose(t *testing.T) {
	id, err := NewSeries("id", []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	value, err := NewSeries("value", []int64{10, 20, 30})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := NewDataFrame(id, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	root := df.Lazy()
	left := root.Select(Col("id"))
	right := root.Filter(Col("value").Gt(Lit(int64(10))))
	root.Close()

	leftResult, err := left.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	leftResult.Close()
	rightResult, err := right.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rightResult.Close()
	left.Release()
	right.Release()
}

func TestReleaseAliasesAreIdempotent(t *testing.T) {
	series, err := NewSeries("id", []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	series.Release()
	series.Close()

	closedValue, err := NewSeries("value", []int64{2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDataFrame(series, closedValue)
	if err == nil {
		t.Fatal("expected closed series to be rejected")
	}
	closedValue.Release()

	liveID, err := NewSeries("id", []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	defer liveID.Close()
	liveValue, err := NewSeries("value", []int64{2})
	if err != nil {
		t.Fatal(err)
	}
	defer liveValue.Close()
	df, err := NewDataFrame(liveID, liveValue)
	if err != nil {
		t.Fatal(err)
	}
	df.Release()
	df.Close()
}
