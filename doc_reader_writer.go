package main

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
		// log.Printf("Writing doc: %v", docToWrite.Key)
		dw.StorageEngine.Insert(
			docToWrite.Key,
			docToWrite.Value,
			0,
		)
		docToRead := Document{
			Key: docToWrite.Key,
		}
		dw.DocsToRead <- docToRead
	}
}

func (dr *DocReader) readDocs() {
	for docToRead := range dr.DocsToRead {
		// log.Printf("Reading doc: %v", docToRead.Key)
		dr.StorageEngine.Get(
			docToRead.Key,
			&docToRead.Value,
		)
		docFinished := Document{
			Key: docToRead.Key,
		}
		dr.DocsFinished <- docFinished
	}

}

func createDocWriters(docsToWrite, docsToRead chan Document, storageEngine StorageEngine, numDocWriters int) {

	for i := 0; i < numDocWriters; i++ {
		docWriter := NewDocWriter(docsToWrite, docsToRead, storageEngine)
		docWriter.Start()
	}

}

func createDocReaders(docsToRead, docsFinished chan Document, storageEngine StorageEngine, numDocReaders int) {

	for i := 0; i < numDocReaders; i++ {
		docReader := NewDocReader(docsToRead, docsFinished, storageEngine)
		docReader.Start()
	}

}
