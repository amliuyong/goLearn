package main

import (
	"log"
     "github.com/amliuyong/go-aws-s3/util"
)

const AWS_REGION = "us-east-1"

func main() {

	util.SetAWSRegion(AWS_REGION)

	const bucket = "cs-test-data-us-east-1"
	const prefix = "test-data/test001/data/buffer/year=2024/month=02/day=05/hour=09/"
	const rid = "941d6849a4f7b11538311fe3d837a7a4"
	const eventId = "event_id-e1707123713682-9139"

	log.Println("try to find eventId:", eventId)

	//FindRidForEventIdInS3Prefix(bucket, prefix, eventId)
	//FindRidForEventIdInS3PrefixParallelWithTimeout(bucket, prefix, eventId)
	//FindRidForEventIdInS3PrefixParallelWithTimeoutAndFixThreadPool(bucket, prefix, eventId)
	//FindRidForEventIdInS3PrefixParallelUtilAllList(bucket, prefix, eventId)
	FindRidForEventIdInS3PrefixParallelUtilAllListWithFixThreadPool(bucket, prefix, eventId)
}

// export STREAMING_PROCESS=true; go run *.go
