package main

func main() {

	const bucket = "cs-test-data-us-east-1"
	const prefix = "test-data/test001/data/buffer/year=2024/month=02/day=05/hour=09/"
	const rid = "941d6849a4f7b11538311fe3d837a7a4"
	const eventId = "event_id-e1707123713682-9139"

	//FindRidForEventIdInS3Prefix(bucket, prefix, eventId)
	//FindRidForEventIdInS3PrefixParallelWithTimeout(bucket, prefix, eventId)
	FindRidForEventIdInS3PrefixParallelUtilAllList(bucket, prefix, eventId)
}

// export STREAMING_PROCESS=true; go run *.go
