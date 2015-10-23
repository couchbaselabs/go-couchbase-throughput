package main

import (
	_ "expvar"
	"flag"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"time"
)

var SlowServerCallWarningThreshold = time.Duration(2500) * time.Millisecond

var storageEngineWriters []StorageEngine
var storageEngineReaders []StorageEngine
var totalNumDocs = flag.Int("totalNumDocs", 500000, "Total number of docs")
var docSizeBytes = flag.Int("docSizeBytes", 1024, "The doc size to use, in bytes")
var numDocWriters = flag.Int("numDocWriters", 1000, "The number of writer goroutines")
var numDocReaders = flag.Int("numDocReaders", 1000, "The number of reader goroutines")
var storageEngineType = flag.String("storageEngineType", "gocb", "gocb or go-couchbase")
var writeAllDocsFirst = flag.Bool("writeAllDocsFirst", false, "Write all docs first, or write + read concurrently")
var cburl = flag.String("couchbaseUrl", "http://127.0.0.1:8091", "Couchbase URL")
var bucket = flag.String("couchbaseBucket", "bucket-1", "Couchbase Bucket")
var numGoCBStorageEngines = flag.Int("numGoCBStorageEngines", 1, "# of gocb storage engines / couchbase connections")
var cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to given file")
var delayBeforeWriteNs = flag.Int("delayBeforeWriteNs", 1000000, "The number of nanoseconds to sleep before writing a document")

func main() {

	flag.Parse()

	// enable profiling via http
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// record cpu profile to file
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	storageEngineReaders = []StorageEngine{}
	storageEngineWriters = []StorageEngine{}

	chanBufferSize := 500000

	// create docsToWrite and docsToRead and docsFinished channels
	docsToWrite := make(chan Document, chanBufferSize)
	docsToRead := make(chan Document, chanBufferSize)
	docsFinished := make(chan Document, chanBufferSize)

	// create doc feeder and start it, pass the docsToWrite channel and other
	// params like total number of docs and doc size
	docFeeder := NewDocFeeder(
		docsToWrite,
		docsFinished,
		*totalNumDocs,
		*docSizeBytes,
		*delayBeforeWriteNs,
	)
	wg := docFeeder.Start()

	switch *storageEngineType {
	case "go-couchbase":
		storageEngineWriter := NewGoCouchbaseStorageEngine(*cburl, *bucket)
		storageEngineWriters = append(storageEngineWriters, storageEngineWriter)
		storageEngineReader := storageEngineWriter // use same storage engine for read/write
		storageEngineReaders = append(storageEngineReaders, storageEngineReader)
	case "gocb":
		for i := 0; i < *numGoCBStorageEngines; i++ {
			storageEngineWriter := NewGoCBStorageEngine(*cburl, *bucket)
			storageEngineWriters = append(storageEngineWriters, storageEngineWriter)
			storageEngineReader := NewGoCBStorageEngine(*cburl, *bucket)
			storageEngineReaders = append(storageEngineReaders, storageEngineReader)
		}
	case "mock":
		storageEngineWriter := NewMockStorageEngine(*cburl, *bucket)
		storageEngineWriters = append(storageEngineWriters, storageEngineWriter)
		storageEngineReader := NewMockStorageEngine(*cburl, *bucket)
		storageEngineReaders = append(storageEngineReaders, storageEngineReader)
	default:
		panic("Unknown storage engine value")
	}

	// create a bunch of docWriter goroutines and pass the docsToWrite channel
	// and the storage engine
	createDocWriters(docsToWrite, docsToRead, storageEngineReaders, *numDocWriters)

	// wait until all docs have been written
	if *writeAllDocsFirst {
		if chanBufferSize < *totalNumDocs {
			log.Fatalf("ERROR: writeAllDocsFirst is set to true, but chanBufferSize < totalNumDocs.  Fix by making sure chanBufferSize == totalNumDocs or setting writeAllDocsFirst to false")
		}
		blockUntilAllDocsWritten(*totalNumDocs, docsToRead)
	}
	// create a bunch of docReader goroutines and pass the docsToRead channel
	// and the storage engine
	createDocReaders(docsToRead, docsFinished, storageEngineWriters, *numDocReaders)

	// wait for doc feeder goroutine to finish (wait group)
	wg.Wait()

}
