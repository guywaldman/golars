package core

type ScanOption func(*scanConfig)

type scanConfig struct {
	hasHeader bool
	delimiter rune
	batchSize int64
}

func defaultScanConfig(engine *Engine) scanConfig {
	return scanConfig{hasHeader: true, delimiter: ',', batchSize: engine.batchSize}
}
func WithHasHeader(enabled bool) ScanOption { return func(c *scanConfig) { c.hasHeader = enabled } }
func WithDelimiter(delimiter rune) ScanOption {
	return func(c *scanConfig) {
		if delimiter != 0 {
			c.delimiter = delimiter
		}
	}
}
func WithScanBatchSize(rows int64) ScanOption {
	return func(c *scanConfig) {
		if rows > 0 {
			c.batchSize = rows
		}
	}
}

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
func WithOverwrite(enabled bool) WriteOption { return func(c *writeConfig) { c.overwrite = enabled } }
func WithWriteHeader(enabled bool) WriteOption {
	return func(c *writeConfig) { c.hasHeader = enabled }
}
func WithWriteDelimiter(delimiter rune) WriteOption {
	return func(c *writeConfig) {
		if delimiter != 0 {
			c.delimiter = delimiter
		}
	}
}
func WithWriteBatchSize(rows int64) WriteOption {
	return func(c *writeConfig) {
		if rows > 0 {
			c.batchSize = rows
		}
	}
}
