package main

import (
	"flag"
	"log"
	"time"
)

var SlowServerCallWarningThreshold = time.Duration(2500) * time.Millisecond

var storageEngineWriter StorageEngine
var storageEngineReader StorageEngine
var totalNumDocs = flag.Int("totalNumDocs", 1000, "Total number of docs")
var docSizeBytes = flag.Int("docSizeBytes", 100, "The doc size to use, in bytes")
var numDocWriters = flag.Int("numDocWriters", 1000, "The number of writer goroutines")
var numDocReaders = flag.Int("numDocReaders", 1000, "The number of reader goroutines")
var storageEngineType = flag.String("storageEngineType", "gocb", "gocb or go-couchbase")
var writeAllDocsFirst = flag.Bool("writeAllDocsFirst", false, "Write all docs first, or write + read concurrently")
var cburl = flag.String("couchbaseUrl", "http://127.0.0.1:8091", "Couchbase URL")
var bucket = flag.String("couchbaseBucket", "bucket-1", "Couchbase Bucket")

func main() {

	flag.Parse()
	log.Printf("Num docs: %v", *totalNumDocs)

	chanBufferSize := *totalNumDocs

	// create docsToWrite and docsToRead and docsFinished channels
	docsToWrite := make(chan Document, chanBufferSize)
	docsToRead := make(chan Document, chanBufferSize)
	docsFinished := make(chan Document, chanBufferSize)

	// create doc feeder and start it, pass the docsToWrite channel and other
	// params like total number of docs and doc size
	docFeeder := NewDocFeeder(docsToWrite, docsFinished, *totalNumDocs, *docSizeBytes)
	wg := docFeeder.Start()

	switch *storageEngineType {
	case "go-couchbase":
		storageEngineWriter = NewGoCouchbaseStorageEngine(*cburl, *bucket)
		storageEngineReader = storageEngineWriter
	case "gocb":
		storageEngineWriter = NewGoCBStorageEngine(*cburl, *bucket)
		storageEngineReader = NewGoCBStorageEngine(*cburl, *bucket)
	case "mock":
		storageEngineWriter = NewMockStorageEngine(*cburl, *bucket)
		storageEngineReader = NewMockStorageEngine(*cburl, *bucket)
	default:
		panic("Unknown storage engine value")
	}

	// create a bunch of docWriter goroutines and pass the docsToWrite channel
	// and the storage engine
	createDocWriters(docsToWrite, docsToRead, storageEngineReader, *numDocWriters)

	// wait until all docs have been written
	if *writeAllDocsFirst {
		if chanBufferSize < *totalNumDocs {
			log.Fatalf("ERROR: writeAllDocsFirst is set to true, but chanBufferSize < totalNumDocs.  Fix by making sure chanBufferSize == totalNumDocs or setting writeAllDocsFirst to false")
		}
		blockUntilAllDocsWritten(*totalNumDocs, docsToRead)
	}
	// create a bunch of docReader goroutines and pass the docsToRead channel
	// and the storage engine
	createDocReaders(docsToRead, docsFinished, storageEngineWriter, *numDocReaders)

	// wait for doc feeder goroutine to finish (wait group)
	wg.Wait()

}
