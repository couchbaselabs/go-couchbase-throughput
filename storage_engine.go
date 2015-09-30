package main

import (
	"log"
	"net/url"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gocb"
)

type StorageEngine interface {
	Insert(key string, value interface{}, expiry uint32) error
	Get(key string, returnVal interface{}) error
}

type GoCouchbaseStorageEngine struct {
	Bucket *couchbase.Bucket
}

type GoCBStorageEngine struct {
	Bucket *gocb.Bucket
}

// couchbaseUrl should have form: http://user:pass@host:8091
func NewGoCouchbaseStorageEngine(couchbaseUrl, bucketName string) *GoCouchbaseStorageEngine {

	// couchbase.PoolSize = 1
	// couchbase.PoolOverflow = 1

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

func NewGoCBStorageEngine(couchbaseUrl, bucketName string) *GoCBStorageEngine {
	cluster, err := gocb.Connect(couchbaseUrl)
	if err != nil {
		log.Panicf("Could not connect to %v.  Err: %v", couchbaseUrl, err)
	}
	log.Printf("cluster: %v", cluster)
	bucket, err := cluster.OpenBucket(bucketName, "")
	if err != nil {
		log.Panicf("Could not open bucket: %v.  Err: %v", bucket, err)
	}

	return &GoCBStorageEngine{
		Bucket: bucket,
	}
}

func (se *GoCBStorageEngine) Insert(key string, value interface{}, expiry uint32) error {
	_, err := se.Bucket.Insert(key, value, expiry)
	return err
}

func (se *GoCouchbaseStorageEngine) Insert(key string, value interface{}, expiry uint32) error {
	_, err := se.Bucket.Add(key, 0, value)
	return err
}

func (se *GoCBStorageEngine) Get(key string, returnValue interface{}) error {
	_, err := se.Bucket.Get(key, returnValue)
	return err
}

func (se *GoCouchbaseStorageEngine) Get(key string, returnValue interface{}) error {
	return se.Bucket.Get(key, returnValue)
}
