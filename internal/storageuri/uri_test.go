package storageuri

import "testing"

func TestScheme(t *testing.T) {
	if got := Scheme("S3://bucket/key"); got != "s3" {
		t.Fatalf("scheme=%q", got)
	}
	if got := Scheme("events/data.parquet"); got != "" {
		t.Fatalf("local path scheme=%q", got)
	}
}

func TestParseAndValidateRange(t *testing.T) {
	bucket, key, err := Parse("gs://bucket/events/data.parquet", "gs")
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "bucket" || key != "events/data.parquet" {
		t.Fatalf("parsed URI=(%q,%q)", bucket, key)
	}
	if err := ValidateRange(4, 8); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRange(-1, 8); err == nil {
		t.Fatal("negative range offset was accepted")
	}
}
