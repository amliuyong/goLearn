package main

import (
	"log"
	"time"

	"github.com/amliuyong/go-aws-s3/util"
)

type FoundItem struct {
	Rid string
	Key string
}

type QueryInput struct {
	Bucket  string
	Key     string
	EventId string
}

func FindRidForEventIdInS3PrefixParallelWithTimeout(bucket string, prefix string, eventId string) (bool, string, string) {
	objects := util.ListAllObjects(bucket, prefix)

	var ch = make(chan FoundItem)
	var foundItem FoundItem

	var found bool

	for _, key := range objects {
		go FindRidForEventIdInS3ObjectTask(bucket, key, eventId, ch)
	}

	// timeout 10 seconds to get the result from the channel

	select {
	case foundItem = <-ch:
		found = true
	case <-time.After(10 * time.Second):
		log.Println("timeout after 10 seconds")
		found = false
	}

	if found {
		log.Println("found: ", found, "foundRid: ", foundItem.Rid, "foundKey: ", foundItem.Key)
		return true, foundItem.Rid, foundItem.Key

	} else {
		log.Println("not found")
		return false, "", ""
	}
}

func FindRidForEventIdInS3PrefixParallelWithTimeoutAndFixThreadPool(bucket string, prefix string, eventId string) (bool, string, string) {
	objects := util.ListAllObjects(bucket, prefix)
	var poolSize = 10

	var batchInputCh = make(chan QueryInput, poolSize)

	var ch = make(chan FoundItem)
	var foundItem FoundItem

	var found bool

	go func() {
		for _, key := range objects {
			batchInputCh <- QueryInput{Bucket: bucket, Key: key, EventId: eventId}
		}
	}()

	go func() {
		for {
			select {
			case input := <-batchInputCh:
				go FindRidForEventIdInS3ObjectTask(input.Bucket, input.Key, input.EventId, ch)
			}
		}
	}()

	// timeout 10 seconds to get the result from the channel

	select {
	case foundItem = <-ch:
		found = true
	case <-time.After(10 * time.Second):
		log.Println("timeout after 10 seconds")
		found = false
	}

	if found {
		log.Println("found: ", found, "foundRid: ", foundItem.Rid, "foundKey: ", foundItem.Key)
		return true, foundItem.Rid, foundItem.Key

	} else {
		log.Println("not found")
		return false, "", ""
	}
}

func FindRidForEventIdInS3ObjectTask(bucket string, key string, eventId string, ch chan<- FoundItem) {
	log.Printf("search %s\n", key)
	found, findRid := FindRidForEventIdInS3Object(bucket, key, eventId)
	if found {
		ch <- FoundItem{Rid: findRid, Key: key}
	}
}
