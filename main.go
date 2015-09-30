package main

func main() {

	chanBufferSize := 1000
	totalNumDocs := 1000
	docSizeBytes := 1024 * 100
	numDocWriters := 100
	numDocReaders := 100

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
	storageEngine := NewGoCouchbaseStorageEngine(url)

	// create a bunch of docWriter goroutines and pass the docsToWrite channel
	// and the storage engine
	createDocWriters(docsToWrite, docsToRead, storageEngine, numDocWriters)

	// create a bunch of docReader goroutines and pass the docsToRead channel
	// and the storage engine
	createDocReaders(docsToRead, docsFinished, storageEngine, numDocReaders)

	// wait for doc feeder goroutine to finish (wait group)
	wg.Wait()

}
