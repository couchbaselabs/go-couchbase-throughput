package main

type StorageEngine interface {
	Insert(key string, value interface{}, expiry uint32) error
}

type GoCouchbaseStorageEngine struct {
}

type GoCBStorageEngine struct {
}

func NewGoCouchbaseStorageEngine(url string) *GoCouchbaseStorageEngine {
	return &GoCouchbaseStorageEngine{}
}

func NewGoCBStorageEngine() *GoCBStorageEngine {
	return &GoCBStorageEngine{}
}

func (se *GoCBStorageEngine) Insert(key string, value interface{}, expiry uint32) error {
	return nil
}

func (se *GoCouchbaseStorageEngine) Insert(key string, value interface{}, expiry uint32) error {
	return nil
}
