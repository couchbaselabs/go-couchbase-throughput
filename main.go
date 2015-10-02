package main

import "time"

var SlowServerCallWarningThreshold = time.Duration(200) * time.Millisecond

func main() {

	var storageEngineWriter StorageEngine
	var storageEngineReader StorageEngine

	totalNumDocs := 500000
	chanBufferSize := totalNumDocs
	docSizeBytes := 100
	numDocWriters := 100
	numDocReaders := 100
	storageEngineType := "gocb"
	// storageEngineType := "go-couchbase"

	// create docsToWrite and docsToRead and docsFinished channels
	docsToWrite := make(chan Document, chanBufferSize)
	docsToRead := make(chan Document, chanBufferSize)
	docsFinished := make(chan Document, chanBufferSize)

	// create doc feeder and start it, pass the docsToWrite channel and other
	// params like total number of docs and doc size
	docFeeder := NewDocFeeder(docsToWrite, docsFinished, totalNumDocs, docSizeBytes)
	wg := docFeeder.Start()

	// create the storage engine (either go-couchbase or go-cb)
	url := "http://127.0.0.1:8091"
	bucket := "default"

	switch storageEngineType {
	case "go-couchbase":
		storageEngineWriter = NewGoCouchbaseStorageEngine(url, bucket)
		storageEngineReader = storageEngineWriter
	case "gocb":
		storageEngineWriter = NewGoCBStorageEngine(url, bucket)
		storageEngineReader = NewGoCBStorageEngine(url, bucket)
	case "mock":
		storageEngineWriter = NewMockStorageEngine(url, bucket)
		storageEngineReader = NewMockStorageEngine(url, bucket)
	default:
		panic("Unknown storage engine value")
	}

	// create a bunch of docWriter goroutines and pass the docsToWrite channel
	// and the storage engine
	createDocWriters(docsToWrite, docsToRead, storageEngineReader, numDocWriters)

	// wait until all docs have been written
	blockUntilAllDocsWritten(totalNumDocs, docsToRead)

	// create a bunch of docReader goroutines and pass the docsToRead channel
	// and the storage engine
	createDocReaders(docsToRead, docsFinished, storageEngineWriter, numDocReaders)

	// wait for doc feeder goroutine to finish (wait group)
	wg.Wait()

}
