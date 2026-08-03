package core

import (
	"context"
	"strings"
	"testing"

	nativebackend "github.com/guywaldman/golars/internal/native"
)

type jsonDecodeProfile struct {
	Country string `json:"country"`
}

type jsonDecodePayload struct {
	ID      int64              `json:"id"`
	Name    string             `json:"name"`
	Tags    []string           `json:"tags"`
	Profile *jsonDecodeProfile `json:"profile"`
	Ignored string             `json:"-"`
}

func TestDecodeJSONDerivesSchemaFromGoStruct(t *testing.T) {
	if _, err := nativebackend.Open(); err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	raw, err := NewSeries("raw", []string{
		`{"id":1,"name":"alpha","tags":["a","b"],"profile":{"country":"IL"},"ignored":"hidden"}`,
		`{"id":2,"name":"beta","tags":[],"profile":null}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	input, err := NewDataFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	decoded := DecodeJSON(Col("raw"), jsonDecodePayload{}).Alias("payload")
	result, err := input.Lazy().Select(
		decoded,
		DecodeJSONAs[jsonDecodePayload](Col("raw")).Alias("payload_by_type"),
	).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()

	rows, err := result.Rows()
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"payload", "payload_by_type"} {
		payload, ok := rows[0][column].(map[string]any)
		if !ok || payload["id"] != int64(1) || payload["name"] != "alpha" {
			t.Fatalf("%s row=%#v, want decoded struct", column, rows[0][column])
		}
		if _, exists := payload["ignored"]; exists {
			t.Fatalf("%s row=%#v contains ignored field", column, payload)
		}
	}

	profile, err := input.Lazy().Select(
		DecodeJSON(Col("raw"), jsonDecodePayload{}).Struct().Element("profile").Struct().Element("country").Alias("country"),
	).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer profile.Close()
	profileRows, err := profile.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if profileRows[0]["country"] != "IL" || profileRows[1]["country"] != nil {
		t.Fatalf("profile rows=%#v", profileRows)
	}
}

func TestDecodeJSONProjectsNestedSchemaAndNullsInvalidRows(t *testing.T) {
	if _, err := nativebackend.Open(); err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	raw, err := NewSeries("raw", []string{
		`{"id":1,"name":"alpha","tags":["a","b"]}`,
		`{"id":"2","name":"coerced","tags":[]}`,
		`{"id":"not-an-integer","name":"bad"}`,
		`not-json`,
		`{"name":"missing"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	input, err := NewDataFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	schema := StructOf(
		Field{Name: "id", Type: Int64, Nullable: false},
		Field{Name: "name", Type: String, Nullable: true},
		Field{Name: "tags", Type: ListOf(String), Nullable: true},
	)
	result, err := input.Lazy().Select(DecodeJSON(Col("raw"), schema).Alias("decoded")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()

	rows, err := result.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("row count=%d, want 5", len(rows))
	}
	t.Logf("decoded rows: %#v", rows)
	if rows[0]["decoded"] == nil {
		t.Fatal("valid nested JSON decoded to null")
	}
	coerced, ok := rows[1]["decoded"].(map[string]any)
	if !ok || coerced["id"] != int64(2) {
		t.Fatalf("coerced row=%#v, want id=2", rows[1])
	}
	invalid, ok := rows[2]["decoded"].(map[string]any)
	if !ok || invalid["id"] != nil || invalid["name"] != "bad" {
		t.Fatalf("invalid leaf row=%#v, want only id null", rows[2])
	}
	if rows[3]["decoded"] != nil {
		t.Fatalf("malformed row=%#v, want null decoded value", rows[3])
	}
	if rows[4]["decoded"] == nil {
		t.Fatal("missing optional fields should keep the decoded struct")
	}
}

func TestDecodeJSONStrictAndNonCoercingModes(t *testing.T) {
	if _, err := nativebackend.Open(); err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	raw, err := NewSeries("raw", []string{`{"id":"1"}`, `{"name":"missing"}`})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	input, err := NewDataFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	schema := StructOf(
		Field{Name: "id", Type: Int64, Nullable: false},
		Field{Name: "name", Type: String, Nullable: true},
	)

	nonCoercing, err := input.Lazy().Select(
		DecodeJSON(Col("raw"), schema, WithJSONCoerce(false)).Alias("decoded"),
	).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer nonCoercing.Close()
	rows, err := nonCoercing.Rows()
	if err != nil {
		t.Fatal(err)
	}
	invalid, ok := rows[0]["decoded"].(map[string]any)
	if !ok || invalid["id"] != nil {
		t.Fatalf("non-coercing invalid row=%#v, want only id null", rows[0])
	}
	if rows[1]["decoded"] == nil {
		t.Fatal("missing fields should remain nullable when strict mode is disabled")
	}

	strict := input.Lazy().Select(
		DecodeJSON(
			Col("raw"),
			schema,
			WithJSONErrorMode(JSONErrorOnError),
			WithJSONStrictRequiredFields(true),
		).Alias("decoded"),
	)
	defer strict.Close()
	_, err = strict.Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "required field") {
		t.Fatalf("strict decode error=%v, want required-field error", err)
	}
}

func TestDecodeJSONErrorModeRaisesForMalformedRows(t *testing.T) {
	if _, err := nativebackend.Open(); err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	raw, err := NewSeries("raw", []string{`{"id":1}`, `not-json`})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	input, err := NewDataFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	schema := StructOf(Field{Name: "id", Type: Int64, Nullable: true})
	_, err = input.Lazy().Select(
		DecodeJSON(Col("raw"), schema, WithJSONErrorMode(JSONErrorOnError)).Alias("decoded"),
	).Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "JSON decode failed at row 1") {
		t.Fatalf("malformed JSON error=%v, want row-level decode error", err)
	}
}

func TestStructElementSelectsNestedFields(t *testing.T) {
	if _, err := nativebackend.Open(); err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	raw, err := NewSeries("raw", []string{
		`{"id":1,"profile":{"name":"alpha"}}`,
		`{"id":2,"profile":{"name":"beta"}}`,
		`{"id":3,"profile":null}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	input, err := NewDataFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	schema := StructOf(
		Field{Name: "id", Type: Int64, Nullable: false},
		Field{
			Name:     "profile",
			Type:     StructOf(Field{Name: "name", Type: String, Nullable: true}),
			Nullable: true,
		},
	)
	decoded := input.Lazy().Select(DecodeJSON(Col("raw"), schema).Alias("payload"))
	result, err := decoded.Select(
		Col("payload").Struct().Element("profile").Struct().Element("name").Alias("name"),
	).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	rows, err := result.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if got := rows[0]["name"]; got != "alpha" {
		t.Fatalf("first nested field=%v, want alpha", got)
	}
	if got := rows[1]["name"]; got != "beta" {
		t.Fatalf("second nested field=%v, want beta", got)
	}
	if got := rows[2]["name"]; got != nil {
		t.Fatalf("null nested field=%v, want nil", got)
	}
}
