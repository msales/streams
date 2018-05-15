# Streams

Streams is a light weight, simple stream processing library. While Kafka is the main use case for Streams, it is
flexible enough to be used for any form of processing from any source.

**Note:** This is currently a work in progress. 

## Installation

You can install streams using `go get`

```shell
go get github.com/msales/streams
```

## Concepts

Streams breaks processing into two basic parts.

* **Sources** reads and handles position from a data source.

* **Processor** processes the data, optionally passing it on or marking the sources position. A sink is just a processor
  the does not forward the data on.
  
* **Context** gives processors an abstract view of the current state.