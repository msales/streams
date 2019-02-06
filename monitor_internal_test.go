package streams

import (
	"testing"
	"time"
)

func TestNullMonitor_Processed(t *testing.T) {
	mon := nullMonitor{}
	defer mon.Close()

	mon.Processed("test", time.Second, 50)
}

func TestMonitor_Committed(t *testing.T) {
	mon := nullMonitor{}
	defer mon.Close()

	mon.Committed(time.Second)
}
