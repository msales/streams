package diagram_test

import (
	"github.com/msales/streams/v2"
	"github.com/msales/streams/v2/diagram"
	"testing"
	"time"

	"github.com/msales/pkg/v3/httpx"
	"github.com/stretchr/testify/assert"
)

func TestStartServerWithEmptyTopology(t *testing.T) {
	topology, _ := streams.NewTopologyBuilder().Build()
	stat := diagram.NewStat(topology)

	go diagram.StartServer("127.0.0.1:8080", stat)
	defer diagram.StopServer()

	time.Sleep(time.Millisecond)

	resp, err := httpx.Get("http://127.0.0.1:8080/diagram")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode)
}

func TestStartServerWithTopology(t *testing.T) {
	builder := streams.NewStreamBuilder()
	builder.Source("event-source", &sourceMock{"test2"})

	topology, errs := builder.Build()
	assert.Nil(t, errs)
	stat := diagram.NewStat(topology)

	go diagram.StartServer("127.0.0.1:8081", stat)
	defer diagram.StopServer()

	time.Sleep(time.Millisecond)

	resp, err := httpx.Get("http://127.0.0.1:8081/diagram")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}
