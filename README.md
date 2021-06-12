# Streams

[![Go Report Card](https://goreportcard.com/badge/github.com/msales/streams)](https://goreportcard.com/report/github.com/msales/streams)
[![Test](https://github.com/msales/streams/actions/workflows/test.yaml/badge.svg)](https://github.com/msales/streams/actions/workflows/test.yaml)
[![Coverage Status](https://coveralls.io/repos/github/msales/streams/badge.svg?branch=master)](https://coveralls.io/github/msales/streams?branch=master)
[![GoDoc](https://godoc.org/github.com/msales/streams?status.svg)](https://godoc.org/github.com/msales/streams)
[![GitHub release](https://img.shields.io/github/release/msales/streams.svg)](https://github.com/msales/streams/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/msales/streams/master/LICENSE)

Streams is a light weight, simple stream processing library. While Kafka is the main use case for Streams, it is
flexible enough to be used for any form of processing from any source.


## Installation

You can install streams using `go get`

```shell
go get github.com/msales/streams
```

## Concepts

Streams breaks processing into the following basic parts.

* **Message** is a message in the system, consisting of a key, value and context.

* **Sources** reads and handles position from a data source.

* **Processor** processes the data, optionally passing it on or marking the sources position. A sink is just a processor
  the does not forward the data on.
  
* **Pipe** gives processors an abstract view of the current state, allowing Messages to flow through the system.

### Read more here: 
https://medium.com/@rafamnich/getting-started-with-streams-v3-b9ab36fb9d54