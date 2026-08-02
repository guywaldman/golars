package native

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"
)

var ErrUnavailable = errors.New("golars: native backend is unavailable for this platform")

type Runtime struct {
	handle         uintptr
	abiVersion     func() uint32
	polarsProbe    func() uint64
	versionBytes   func(output uintptr, capacity uintptr) uintptr
	executeFrame   func(request uintptr, requestLength uintptr, frame uintptr, errorOutput uintptr, errorLength uintptr) int32
	frameFree      func(frame uintptr)
	frameHeight    func(frame uintptr) uintptr
	frameWidth     func(frame uintptr) uintptr
	frameIPC       func(frame uintptr, output uintptr, outputLength uintptr, errorOutput uintptr, errorLength uintptr) int32
	frameSchemaIPC func(frame uintptr, output uintptr, outputLength uintptr, errorOutput uintptr, errorLength uintptr) int32
	freeBuffer     func(buffer uintptr, length uintptr)
}

var (
	runtimeOnce sync.Once
	runtimeInst *Runtime
	runtimeErr  error
)

func Open() (*Runtime, error) {
	runtimeOnce.Do(func() {
		runtimeInst, runtimeErr = open()
	})
	return runtimeInst, runtimeErr
}

func (r *Runtime) ABIVersion() uint32 {
	if r == nil || r.abiVersion == nil {
		return 0
	}
	return r.abiVersion()
}

func (r *Runtime) PolarsVersion() (string, error) {
	if r == nil || r.versionBytes == nil {
		return "", ErrUnavailable
	}

	length := r.versionBytes(0, 0)
	if length == 0 {
		return "", errors.New("golars: native backend returned an empty version")
	}
	buffer := make([]byte, length)
	actual := r.versionBytes(uintptr(unsafe.Pointer(&buffer[0])), uintptr(len(buffer)))
	if actual > uintptr(len(buffer)) {
		return "", fmt.Errorf("golars: native backend version length %d exceeds buffer %d", actual, len(buffer))
	}
	return string(buffer[:actual]), nil
}

func (r *Runtime) Probe() uint64 {
	if r == nil || r.polarsProbe == nil {
		return 0
	}
	return r.polarsProbe()
}

// ExecuteFrame evaluates a request and keeps the result in the native engine.
// The returned frame owns the native allocation and must be closed.
func (r *Runtime) ExecuteFrame(request []byte) (*Frame, error) {
	if r == nil || r.executeFrame == nil || r.frameFree == nil {
		return nil, ErrUnavailable
	}
	var requestPointer uintptr
	if len(request) > 0 {
		requestPointer = uintptr(unsafe.Pointer(&request[0]))
	}
	var handle, errorPointer, errorLength uintptr
	status := r.executeFrame(
		requestPointer,
		uintptr(len(request)),
		uintptr(unsafe.Pointer(&handle)),
		uintptr(unsafe.Pointer(&errorPointer)),
		uintptr(unsafe.Pointer(&errorLength)),
	)
	runtime.KeepAlive(request)
	if status != 0 {
		message := string(r.copyAndFree(errorPointer, errorLength))
		if message == "" {
			message = "native backend execution failed"
		}
		return nil, errors.New(message)
	}
	if handle == 0 {
		return nil, errors.New("golars: native backend returned an empty dataframe handle")
	}
	return &Frame{runtime: r, handle: handle}, nil
}

// Frame is an opaque result retained by the native execution backend.
// Close releases its native allocation deterministically.
type Frame struct {
	runtime *Runtime
	handle  uintptr
	once    sync.Once
}

func (f *Frame) Height() int64 {
	if f == nil || f.handle == 0 || f.runtime == nil || f.runtime.frameHeight == nil {
		return 0
	}
	return int64(f.runtime.frameHeight(f.handle))
}

func (f *Frame) Width() int {
	if f == nil || f.handle == 0 || f.runtime == nil || f.runtime.frameWidth == nil {
		return 0
	}
	return int(f.runtime.frameWidth(f.handle))
}

func (f *Frame) IPC() ([]byte, error) {
	if f == nil || f.handle == 0 || f.runtime == nil || f.runtime.frameIPC == nil {
		return nil, ErrUnavailable
	}
	return f.executeOutput(f.runtime.frameIPC)
}

func (f *Frame) SchemaIPC() ([]byte, error) {
	if f == nil || f.handle == 0 || f.runtime == nil || f.runtime.frameSchemaIPC == nil {
		return nil, ErrUnavailable
	}
	return f.executeOutput(f.runtime.frameSchemaIPC)
}

func (f *Frame) executeOutput(
	execute func(frame uintptr, output uintptr, outputLength uintptr, errorOutput uintptr, errorLength uintptr) int32,
) ([]byte, error) {
	var outputPointer, outputLength, errorPointer, errorLength uintptr
	status := execute(
		f.handle,
		uintptr(unsafe.Pointer(&outputPointer)),
		uintptr(unsafe.Pointer(&outputLength)),
		uintptr(unsafe.Pointer(&errorPointer)),
		uintptr(unsafe.Pointer(&errorLength)),
	)
	if status != 0 {
		message := string(f.runtime.copyAndFree(errorPointer, errorLength))
		if message == "" {
			message = "native backend dataframe export failed"
		}
		return nil, errors.New(message)
	}
	return f.runtime.copyAndFree(outputPointer, outputLength), nil
}

func (f *Frame) Close() {
	if f == nil {
		return
	}
	f.once.Do(func() {
		if f.runtime != nil && f.runtime.frameFree != nil && f.handle != 0 {
			f.runtime.frameFree(f.handle)
		}
		f.handle = 0
	})
}

func (r *Runtime) copyAndFree(pointer, length uintptr) []byte {
	if pointer == 0 || length == 0 {
		if pointer != 0 {
			r.freeBuffer(pointer, length)
		}
		return nil
	}
	header := struct {
		data uintptr
		len  int
		cap  int
	}{data: pointer, len: int(length), cap: int(length)}
	data := *(*[]byte)(unsafe.Pointer(&header))
	copyOfData := append([]byte(nil), data...)
	r.freeBuffer(pointer, length)
	return copyOfData
}

func materialize(asset embeddedLibrary) (string, error) {
	digest := sha256.Sum256(asset.data)
	name := "golars-" + hex.EncodeToString(digest[:8]) + asset.extension
	directory, err := os.MkdirTemp("", "golars-native-")
	if err != nil {
		return "", fmt.Errorf("golars: create native backend directory: %w", err)
	}
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, asset.data, 0o700); err != nil {
		_ = os.RemoveAll(directory)
		return "", fmt.Errorf("golars: materialize native backend: %w", err)
	}
	return path, nil
}
