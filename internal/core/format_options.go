package core

// ScanOption configures a scan.
type ScanOption func(*scanConfig)

type scanConfig struct {
	hasHeader         bool
	delimiter         rune
	batchSize         int64
	columns           []string
	skipRows          int64
	nRows             int64
	comment           rune
	nullValues        []string
	quote             rune
	quoteSet          bool
	inferSchemaLength int64
}

func defaultScanConfig(engine *Engine) scanConfig {
	return scanConfig{
		hasHeader: true, delimiter: ',', batchSize: engine.batchSize,
		nRows: -1, inferSchemaLength: 100,
	}
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

// WithColumns limits a scan to the named columns before executing the plan.
func WithColumns(columns ...string) ScanOption {
	return func(c *scanConfig) {
		c.columns = append([]string(nil), columns...)
	}
}

// WithSkipRows skips rows before the scan's header/data rows.
func WithSkipRows(rows int64) ScanOption {
	return func(c *scanConfig) {
		if rows >= 0 {
			c.skipRows = rows
		}
	}
}

// WithNRows limits the number of rows read from the source. A negative value
// disables the limit.
func WithNRows(rows int64) ScanOption {
	return func(c *scanConfig) {
		if rows >= 0 {
			c.nRows = rows
		}
	}
}

// WithComment ignores CSV records beginning with comment.
func WithComment(comment rune) ScanOption {
	return func(c *scanConfig) { c.comment = comment }
}

// WithNullValues treats the supplied CSV values as null.
func WithNullValues(values ...string) ScanOption {
	return func(c *scanConfig) { c.nullValues = append([]string(nil), values...) }
}

// WithQuoteChar sets the CSV quote character. Passing zero disables quoting.
func WithQuoteChar(quote rune) ScanOption {
	return func(c *scanConfig) {
		c.quote = quote
		c.quoteSet = true
	}
}

// WithInferSchemaLength sets the number of CSV rows used for type inference.
// A negative value requests a full-file inference scan.
func WithInferSchemaLength(rows int64) ScanOption {
	return func(c *scanConfig) {
		if rows >= 0 {
			c.inferSchemaLength = rows
		} else {
			c.inferSchemaLength = -1
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
