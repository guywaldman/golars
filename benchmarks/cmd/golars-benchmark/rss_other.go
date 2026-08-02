//go:build aix || illumos || ios || js || plan9 || wasip1 || windows || zos

package main

func maxRSSBytes() uint64 { return 0 }
