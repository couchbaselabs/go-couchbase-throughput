package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nu7hatch/gouuid"
)

// This is a one-off test to do a lot of updates on a single doc.
// Originally used for bucket shadowing testing
func updateLoop() {

	numUpdates := 10
	sleepDelayBetweenUpdates := 5

	// create a uuid that will make keys for this "run" not collide from future runs
	// or from other machines running go-couchbase-throughput concurrently with this one.
	uuidRaw, err := uuid.NewV4()
	if err != nil {
		log.Panicf("Error creating a UUID: %v", err)
	}
	keyPrefixUUID := uuidRaw.String()

	var storageEngine StorageEngine

	switch *storageEngineType {
	case "go-couchbase":
		storageEngine = NewGoCouchbaseStorageEngine(*cburl, *bucket)
	case "gocb":
		storageEngine = NewGoCBStorageEngine(*cburl, *bucket)
	default:
		panic("Unknown storage engine value")
	}

	key := fmt.Sprintf("key-%v", keyPrefixUUID)
	expiry := 0 // never expires

	for i := 0; i < numUpdates; i++ {

		value := fmt.Sprintf("value-%v", i)

		if i == 0 {
			// insert doc
			log.Printf("Inserting key: %v", key)
			if err := storageEngine.Insert(key, value, uint32(expiry)); err != nil {
				log.Panicf("Error inserting doc with key: %v err: %v", key, err)
			}
		} else {
			// update doc to have new value
			log.Printf("Updating key: %v", key)
			updateFunc := func(current []byte) (updated []byte, err error) {
				return []byte(value), nil

			}
			if err := storageEngine.Update(key, uint32(expiry), updateFunc); err != nil {
				log.Panicf("Error inserting doc with key: %v err: %v", key, err)
			}
		}

		log.Printf("Sleeping: %v", sleepDelayBetweenUpdates)
		<-time.After(time.Second * time.Duration(sleepDelayBetweenUpdates))

	}

}
