package main

import (
	"net/url"

	"github.com/couchbase/go-couchbase"
)

type StorageEngine interface {
	Insert(key string, value interface{}, expiry uint32) error
	Get(key string, returnVal interface{}) error
}

type GoCouchbaseStorageEngine struct {
	Bucket *couchbase.Bucket
}

type GoCBStorageEngine struct {
}

// couchbaseUrl should have form: http://user:pass@host:8091
func NewGoCouchbaseStorageEngine(couchbaseUrl, bucketName string) *GoCouchbaseStorageEngine {

	u, err := url.Parse(couchbaseUrl)
	if err != nil {
		panic("Invalid url")
	}

	if bucketName == "" && u.User != nil {
		bucketName = u.User.Username()
	}

	client, err := couchbase.Connect(u.String())
	if err != nil {
		panic("Could not create client")
	}

	pool, err := client.GetPool("default")
	if err != nil {
		panic("Could not create pool")
	}

	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		panic("Could not create bucket")
	}

	return &GoCouchbaseStorageEngine{
		Bucket: bucket,
	}

}

func NewGoCBStorageEngine() *GoCBStorageEngine {
	return &GoCBStorageEngine{}
}

func (se *GoCBStorageEngine) Insert(key string, value interface{}, expiry uint32) error {
	return nil
}

func (se *GoCouchbaseStorageEngine) Insert(key string, value interface{}, expiry uint32) error {
	_, err := se.Bucket.Add(key, 0, value)
	return err
}

func (se *GoCBStorageEngine) Get(key string, returnValue interface{}) error {
	return nil
}

func (se *GoCouchbaseStorageEngine) Get(key string, returnValue interface{}) error {
	return se.Bucket.Get(key, returnValue)
}
