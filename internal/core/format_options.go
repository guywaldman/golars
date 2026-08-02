package core

// ScanOption configures a scan.
type ScanOption func(*scanConfig)

type scanConfig struct {
	hasHeader bool
	delimiter rune
	batchSize int64
}

func defaultScanConfig(engine *Engine) scanConfig {
	return scanConfig{hasHeader: true, delimiter: ',', batchSize: engine.batchSize}
}

// WithHasHeader controls whether delimited input has a header row.
func WithHasHeader(enabled bool) ScanOption { return func(c *scanConfig) { c.hasHeader = enabled } }

// WithDelimiter sets the delimiter for delimited input.
func WithDelimiter(delimiter rune) ScanOption {
	return func(c *scanConfig) {
		if delimiter != 0 {
			c.delimiter = delimiter
		}
	}
}

// WithScanBatchSize sets the preferred scan batch size.
func WithScanBatchSize(rows int64) ScanOption {
	return func(c *scanConfig) {
		if rows > 0 {
			c.batchSize = rows
		}
	}
}

// WriteOption configures a file sink.
type WriteOption func(*writeConfig)

type writeConfig struct {
	overwrite bool
	hasHeader bool
	delimiter rune
	batchSize int64
}

func defaultWriteConfig(engine *Engine) writeConfig {
	return writeConfig{hasHeader: true, delimiter: ',', batchSize: engine.batchSize}
}

// WithOverwrite controls whether an existing output is replaced.
func WithOverwrite(enabled bool) WriteOption { return func(c *writeConfig) { c.overwrite = enabled } }

// WithWriteHeader controls whether delimited output includes a header row.
func WithWriteHeader(enabled bool) WriteOption {
	return func(c *writeConfig) { c.hasHeader = enabled }
}

// WithWriteDelimiter sets the delimiter for delimited output.
func WithWriteDelimiter(delimiter rune) WriteOption {
	return func(c *writeConfig) {
		if delimiter != 0 {
			c.delimiter = delimiter
		}
	}
}

// WithWriteBatchSize sets the preferred sink batch size.
func WithWriteBatchSize(rows int64) WriteOption {
	return func(c *writeConfig) {
		if rows > 0 {
			c.batchSize = rows
		}
	}
}
