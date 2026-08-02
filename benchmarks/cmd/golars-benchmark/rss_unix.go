//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package main

import (
	"runtime"
	"syscall"
)

func maxRSSBytes() uint64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	value := uint64(usage.Maxrss)
	if runtime.GOOS != "darwin" {
		value *= 1024
	}
	return value
}
