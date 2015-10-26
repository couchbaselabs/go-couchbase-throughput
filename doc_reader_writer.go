package main

import (
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type DocWriter struct {
	DocsToWrite   chan Document
	DocsToRead    chan Document
	StorageEngine StorageEngine
}

type DocReader struct {
	DocsToRead    chan Document
	DocsFinished  chan Document
	StorageEngine StorageEngine
}

var numDocsWritten int64
var numDocsFinished int64

func NewDocWriter(docsToWrite, docsToRead chan Document, storageEngine StorageEngine) *DocWriter {
	return &DocWriter{
		DocsToWrite:   docsToWrite,
		DocsToRead:    docsToRead,
		StorageEngine: storageEngine,
	}
}

func NewDocReader(docsToRead, docsFinished chan Document, storageEngine StorageEngine) *DocReader {
	return &DocReader{
		DocsToRead:    docsToRead,
		DocsFinished:  docsFinished,
		StorageEngine: storageEngine,
	}
}

func (dw *DocWriter) Start() {
	go dw.writeDocs()
}

func (dr *DocReader) Start() {
	go dr.readDocs()
}

func (dw *DocWriter) writeDocs() {
	for docToWrite := range dw.DocsToWrite {
		err := dw.StorageEngine.Insert(
			docToWrite.Key,
			docToWrite.Value,
			0,
		)
		if err != nil {
			log.Printf("Error writing doc: %v", err)
			if *delayAfterIOErrNs > 0 {
				log.Printf("Sleeping %v ns due to write error", *delayAfterIOErrNs)
				<-time.After(time.Duration(*delayAfterIOErrNs) * time.Nanosecond)
			}
		}
		docToRead := Document{
			Key: docToWrite.Key,
		}
		dw.DocsToRead <- docToRead
		atomic.AddInt64(&numDocsWritten, 1)
	}
}

func (dr *DocReader) readDocs() {
	for docToRead := range dr.DocsToRead {
		err := dr.StorageEngine.Get(
			docToRead.Key,
			&docToRead.Value,
		)
		atomic.AddInt64(&numDocsFinished, 1)
		if err != nil {
			log.Printf("Error getting doc: %v", err)
			if *delayAfterIOErrNs > 0 {
				log.Printf("Sleeping %v ns due to read error", *delayAfterIOErrNs)
				<-time.After(time.Duration(*delayAfterIOErrNs) * time.Nanosecond)
			}
		}
	}

}

func blockUntilAllDocsWritten(totalNumDocs int, docsToRead chan Document) {
	log.Printf("blockUntilAllDocsWritten")
	for {
		<-time.After(1 * time.Second)
		numDocsWrittenSnapshot := atomic.LoadInt64(&numDocsWritten)
		if int(numDocsWrittenSnapshot) >= totalNumDocs {
			log.Printf("/blockUntilAllDocsWritten")
			return
		}
		log.Printf("numDocsWrittenSnapshot < totalNumDocs, %v < %v", numDocsWrittenSnapshot, totalNumDocs)
	}

}

func createDocWriters(docsToWrite, docsToRead chan Document, storageEngines []StorageEngine, numDocWriters int) {

	for i := 0; i < numDocWriters; i++ {
		storageEngineIndex := i % len(storageEngines)
		storageEngine := storageEngines[storageEngineIndex]
		docWriter := NewDocWriter(docsToWrite, docsToRead, storageEngine)
		docWriter.Start()
	}

}

func createDocReaders(docsToRead, docsFinished chan Document, storageEngines []StorageEngine, numDocReaders int) {
	for i := 0; i < numDocReaders; i++ {
		storageEngineIndex := i % len(storageEngines)
		storageEngine := storageEngines[storageEngineIndex]
		docReader := NewDocReader(docsToRead, docsFinished, storageEngine)
		docReader.Start()
	}
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}
