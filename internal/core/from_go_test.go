package core

import (
	"reflect"
	"testing"
)

type fromGoUser struct {
	ID      int64         `json:"id"`
	Name    string        `json:"name"`
	Score   *int64        `json:"score"`
	Profile fromGoProfile `json:"profile"`
	Ignored string        `json:"-"`
	Alias   string        `compute:"alias"`
}

type fromGoProfile struct {
	Country string `json:"country"`
	Active  bool   `json:"active"`
}

func TestDataFrameFromStructsUsesTagsAndNulls(t *testing.T) {
	score := int64(99)
	df, err := DataFrameFromStructs([]fromGoUser{
		{ID: 1, Name: "Ada", Score: &score, Profile: fromGoProfile{Country: "IL", Active: true}, Ignored: "hidden", Alias: "a"},
		{ID: 2, Name: "Grace", Profile: fromGoProfile{Country: "US"}, Alias: "g"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if want := []string{"id", "name", "score", "profile", "alias"}; !reflect.DeepEqual(df.Columns(), want) {
		t.Fatalf("columns=%v, want %v", df.Columns(), want)
	}
	rows, err := df.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["id"] != int64(1) || rows[0]["score"] != int64(99) || rows[1]["score"] != nil {
		t.Fatalf("rows=%v", rows)
	}
	profile, err := df.Select(Col("profile").Struct().Element("country").Alias("country"))
	if err != nil {
		t.Fatal(err)
	}
	defer profile.Close()
	profileRows, err := profile.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if profileRows[0]["country"] != "IL" || profileRows[1]["country"] != "US" {
		t.Fatalf("profile rows=%v", profileRows)
	}
}

func TestDataFrameFromMapsSortsColumnsAndFillsMissingValues(t *testing.T) {
	df, err := DataFrameFromMaps([]map[string]any{
		{"z": int64(3), "name": "Ada"},
		{"z": nil, "name": "Grace"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if want := []string{"name", "z"}; !reflect.DeepEqual(df.Columns(), want) {
		t.Fatalf("columns=%v, want %v", df.Columns(), want)
	}
	rows, err := df.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["z"] != int64(3) || rows[1]["z"] != nil {
		t.Fatalf("rows=%v", rows)
	}
}

func TestDataFrameFromStructPointersSupportsNilRows(t *testing.T) {
	first := &fromGoUser{ID: 1, Name: "Ada"}
	df, err := DataFrameFromStructs([]*fromGoUser{first, nil})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	rows, err := df.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["id"] != int64(1) || rows[1]["id"] != nil {
		t.Fatalf("rows=%v", rows)
	}
}
