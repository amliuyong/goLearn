package main

import (
	"log"
	"sync"
	"time"

	"github.com/amliuyong/go-aws-s3/util"
)

func FindRidForEventIdInS3PrefixParallelUtilAllList(bucket string, prefix string, eventId string) (bool, string, string) {
	objects := util.ListAllOjbects(bucket, prefix)

	var ch = make(chan FoundItem)
	var foundItem FoundItem
	var found bool
	wg := sync.WaitGroup{}

	for _, key := range objects {
		wg.Add(1)
		go FindRidForEventIdInS3OjbectTaskWithWaitGroup(bucket, key, eventId, ch, &wg)
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

func FindRidForEventIdInS3OjbectTaskWithWaitGroup(bucket string, key string, eventId string, ch chan<- FoundItem, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("search %s\n", key)
	found, findRid := FindRidForEventIdInS3Object(bucket, key, eventId)
	if found {
		ch <- FoundItem{Rid: findRid, Key: key}
	}
}
