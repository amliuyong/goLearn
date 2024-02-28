package main

import (
	"log"
	"sync"
	"time"

	"github.com/amliuyong/go-aws-s3/util"
)

func FindRidForEventIdInS3PrefixParallelUtilAllList(bucket string, prefix string, eventId string) (bool, string, string) {
	objects := util.ListAllObjects(bucket, prefix)

	var ch = make(chan FoundItem)
	var foundItem FoundItem
	var found bool
	wg := sync.WaitGroup{}

	for _, key := range objects {
		wg.Add(1)
		go FindRidForEventIdInS3ObjectTaskWithWaitGroup(bucket, key, eventId, ch, &wg)
	}

	var listDone = make(chan bool)

	go func() {
		wg.Wait()
		listDone <- true
	}()

	select {
	case foundItem = <-ch:
		found = true
	case <-listDone:
		found = false
		log.Println("Iterated all objects, not found")
	case <-time.After(3600 * time.Second):
		log.Println("timeout after 3600 seconds")
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

func FindRidForEventIdInS3PrefixParallelUtilAllListWithFixThreadPool(bucket string, prefix string, eventId string) (bool, string, string) {
	objects := util.ListAllObjects(bucket, prefix)

	var poolSize = 10
	var queryBatchCh = make(chan QueryInput, poolSize)

	var ch = make(chan FoundItem)
	var foundItem FoundItem
	var found bool
	wg := sync.WaitGroup{}
	wg.Add(len(objects))

	go func() {
		for _, key := range objects {
			log.Printf("QueryInput: %s\n", key)
			queryBatchCh <- QueryInput{Bucket: bucket, Key: key, EventId: eventId}
		}
	}()

	go func() {
		for {
			select {
			case input := <-queryBatchCh:
				log.Printf("get input: %s\n", input.Key)
				go FindRidForEventIdInS3ObjectTaskWithWaitGroup(input.Bucket, input.Key, input.EventId, ch, &wg)
			}
		}

	}()

	var listDone = make(chan bool)

	go func() {
		wg.Wait()
		listDone <- true
	}()

	select {
	case foundItem = <-ch:
		found = true
	case <-listDone:
		found = false
		log.Println("Iterated all objects, not found")
	case <-time.After(3600 * time.Second):
		log.Println("timeout after 3600 seconds")
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

func FindRidForEventIdInS3ObjectTaskWithWaitGroup(bucket string, key string, eventId string, ch chan<- FoundItem, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("search %s\n", key)
	found, findRid := FindRidForEventIdInS3Object(bucket, key, eventId)
	if found {
		ch <- FoundItem{Rid: findRid, Key: key}
	}
}
