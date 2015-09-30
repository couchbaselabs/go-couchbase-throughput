package main

import (
	"fmt"
	"math/rand"
	"sync"
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

	for i := 0; i < d.TotalNumDocs; i++ {

		doc := Document{
			Key:   fmt.Sprintf("key-%v", i),
			Value: d.createDocContent(),
		}
		d.DocsToWrite <- doc
		wg.Add(1)
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

	numDocsFinished := 0
	for {
		<-d.DocsFinished
		numDocsFinished += 1
		wg.Done()
	}

}
