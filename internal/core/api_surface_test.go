package core

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestSeriesValuesSliceAndClone(t *testing.T) {
	series, err := NewSeries("value", []int64{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()

	values, err := series.Values()
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{int64(1), int64(2), int64(3), int64(4)}; !reflect.DeepEqual(values, want) {
		t.Fatalf("Values()=%v, want %v", values, want)
	}
	if series.NChunks() != 1 || series.IsEmpty() {
		t.Fatalf("series metadata chunks=%d empty=%v", series.NChunks(), series.IsEmpty())
	}

	sliced, err := series.Slice(-3, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer sliced.Close()
	slicedValues, err := sliced.ToSlice()
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{int64(2), int64(3)}; !reflect.DeepEqual(slicedValues, want) {
		t.Fatalf("Slice()=%v, want %v", slicedValues, want)
	}

	clone, err := series.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	if clone == series || clone.Name() != series.Name() {
		t.Fatalf("Clone() did not create an independent named series")
	}
	series.Close()
	if _, err := series.Values(); err != ErrClosed {
		t.Fatalf("closed Values() error=%v, want %v", err, ErrClosed)
	}
	cloneValues, err := clone.Values()
	if err != nil || !reflect.DeepEqual(cloneValues, values) {
		t.Fatalf("clone Values()=%v,%v, want %v,nil", cloneValues, err, values)
	}
}

func TestFrameSequenceAndGroupConveniences(t *testing.T) {
	group, err := NewSeries("group", []int64{1, 1, 2, 2})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	value, err := NewSeries("value", []float64{math.NaN(), 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := NewDataFrame(group, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	sliced, err := df.Slice(-3, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer sliced.Close()
	if rows, err := sliced.Rows(); err != nil {
		t.Fatal(err)
	} else if want := []Row{{"group": int64(1), "value": 2.0}, {"group": int64(2), "value": 3.0}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("Slice()=%v, want %v", rows, want)
	}

	reversed, err := df.Reverse()
	if err != nil {
		t.Fatal(err)
	}
	defer reversed.Close()
	if rows, err := reversed.Rows(); err != nil {
		t.Fatal(err)
	} else if rows[0]["group"] != int64(2) || rows[3]["group"] != int64(1) {
		t.Fatalf("Reverse()=%v", rows)
	}

	withoutNaN, err := df.DropNaNs("value")
	if err != nil {
		t.Fatal(err)
	}
	defer withoutNaN.Close()
	if withoutNaN.Height() != 3 {
		t.Fatalf("DropNaNs() height=%d, want 3", withoutNaN.Height())
	}

	counts, err := df.GroupBy(Col("group")).Len()
	if err != nil {
		t.Fatal(err)
	}
	defer counts.Close()
	if counts.Height() != 2 || counts.Width() != 2 {
		t.Fatalf("GroupBy.Len() shape=(%d,%d), want (2,2)", counts.Height(), counts.Width())
	}

	sequential, err := df.WithColumnsSeq(
		Col("group").Add(Lit(int64(10))).Alias("bucket_plus_ten"),
		Col("value").FillNaN(Lit(0.0)).Alias("value_filled"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sequential.Close()
	if rows, err := sequential.Rows(); err != nil {
		t.Fatal(err)
	} else if rows[0]["bucket_plus_ten"] != int64(11) || rows[0]["value_filled"] != 0.0 {
		t.Fatalf("WithColumnsSeq()=%v", rows)
	}
}

func TestNumericListStringAndTemporalExpressions(t *testing.T) {
	numbers, err := NewSeries("number", []float64{-2.75, 3.25})
	if err != nil {
		t.Fatal(err)
	}
	defer numbers.Close()
	items, err := NewSeries("items", [][]int64{{1, 2, 3}, {4, 5}})
	if err != nil {
		t.Fatal(err)
	}
	defer items.Close()
	words, err := NewSeries("word", []string{"  hello  ", "éclair"})
	if err != nil {
		t.Fatal(err)
	}
	defer words.Close()
	times, err := NewSeries("ts", []time.Time{
		time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2024, time.July, 3, 13, 14, 15, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer times.Close()
	df, err := NewDataFrame(numbers, items, words, times)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	numeric, err := df.Select(
		Col("number").Abs().Alias("abs"),
		Col("number").FloorDiv(Lit(2.0)).Alias("floor_div"),
		Col("number").Pow(Lit(2.0)).Alias("pow"),
		Col("number").Round(1).Alias("round"),
		Col("number").Clip(Lit(-1.0), Lit(3.0)).Alias("clip"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer numeric.Close()
	if rows, err := numeric.Rows(); err != nil {
		t.Fatal(err)
	} else if rows[0]["abs"] != 2.75 || rows[1]["clip"] != 3.0 {
		t.Fatalf("numeric expressions=%v", rows)
	}

	list, err := df.Select(
		Col("items").List().Len().Alias("len"),
		Col("items").List().Get(Lit(int64(0))).Alias("first"),
		Col("items").List().Contains(Lit(int64(5))).Alias("contains"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer list.Close()
	if rows, err := list.Rows(); err != nil {
		t.Fatal(err)
	} else if rows[0]["len"] != uint32(3) || rows[1]["first"] != int64(4) || rows[1]["contains"] != true {
		t.Fatalf("list expressions=%v", rows)
	}

	strings, err := df.Select(
		Col("word").Str().StripChars(Lit(" ")).Alias("stripped"),
		Col("word").Str().ReplaceAll(Lit("l"), Lit("x")).Alias("replaced"),
		Col("word").Str().LenBytes().Alias("bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer strings.Close()
	if rows, err := strings.Rows(); err != nil {
		t.Fatal(err)
	} else if rows[0]["stripped"] != "hello" || rows[0]["replaced"] != "  hexxo  " || rows[1]["bytes"] != int64(7) {
		t.Fatalf("string expressions=%v", rows)
	}

	temporal, err := df.Select(
		Col("ts").Dt().Hour().Alias("hour"),
		Col("ts").Dt().Quarter().Alias("quarter"),
		Col("ts").Dt().Strftime("%Y-%m-%d").Alias("date"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer temporal.Close()
	if rows, err := temporal.Rows(); err != nil {
		t.Fatal(err)
	} else if rows[0]["hour"] != float64(3) || rows[0]["quarter"] != float64(1) || rows[1]["quarter"] != float64(3) || rows[0]["date"] != "2024-01-02" {
		t.Fatalf("temporal expressions=%v", rows)
	}

	seq := df.Lazy().SelectSeq(Col("number"))
	defer seq.Close()
	selected, err := df.SelectSeq(Col("number").Alias("base"), Col("number").Add(Lit(1.0)).Alias("next"))
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	if selected.Width() != 2 || selected.Height() != 2 {
		t.Fatalf("SelectSeq() shape=(%d,%d)", selected.Height(), selected.Width())
	}
}
