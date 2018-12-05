package streams

import (
	_ "unsafe"
)

//go:linkname nanotime runtime.nanotime
func nanotime() int64
