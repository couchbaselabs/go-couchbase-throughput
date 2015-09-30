
Find max ops/sec that can be pushed through go-couchbase.

Write 'doc1' through 'doc100000', and then spawn readers to get chunks of that set.

* Ability to swap out go-couchbase and go-cb
* Ability to tune how many concurrent goroutines are using the client
* Ability to tune connection pool size (in the case of go-couchbase)
* Ability to tune the doc size and total number of docs