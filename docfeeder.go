package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type DocFeeder struct {
	DocsToWrite  chan Document
	DocsFinished chan Document
	TotalNumDocs int
	DocSizeBytes int
}

func NewDocFeeder(docsToWrite, docsFinished chan Document, totalNumDocs, docSizeBytes int) *DocFeeder {
	docFeeder := DocFeeder{}
	docFeeder.DocsToWrite = docsToWrite
	docFeeder.DocsFinished = docsFinished
	docFeeder.TotalNumDocs = totalNumDocs
	docFeeder.DocSizeBytes = docSizeBytes
	return &docFeeder
}

func (d *DocFeeder) Start() *sync.WaitGroup {

	wg := sync.WaitGroup{}

	go d.writeDocs(&wg)

	go d.waitForDocsFinished(&wg)

	return &wg

}

func (d *DocFeeder) writeDocs(wg *sync.WaitGroup) {

	wg.Add(d.TotalNumDocs)

	for i := 0; i < d.TotalNumDocs; i++ {

		doc := Document{
			Key:   fmt.Sprintf("key-%v", i),
			Value: d.createDocContent(),
		}
		d.DocsToWrite <- doc

	}

}

func (d DocFeeder) createDocContent() interface{} {
	return randSeq(d.DocSizeBytes)
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (d *DocFeeder) waitForDocsFinished(wg *sync.WaitGroup) {

	for {
		<-time.After(1 * time.Second)
		numDocsFinishedSnapshot := atomic.LoadInt64(&numDocsFinished)
		if int(numDocsFinishedSnapshot) >= d.TotalNumDocs {
			wg.Add(-1 * d.TotalNumDocs)
			log.Printf("/waitForDocsFinished")
			return
		}
		log.Printf("numDocsFinishedSnapshot < totalNumDocs, %v < %v", numDocsFinishedSnapshot, d.TotalNumDocs)
	}

}
