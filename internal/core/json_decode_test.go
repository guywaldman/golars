package core

import (
	"context"
	"strings"
	"testing"

	nativebackend "github.com/guywaldman/golars/internal/native"
)

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
