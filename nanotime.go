package streams

import (
	_ "unsafe" // Required in order to import nanotime
)

//go:linkname nanotime runtime.nanotime
func nanotime() int64
