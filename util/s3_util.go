package util

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BreakContinue struct {
	Break bool
}

func ListAllOjbects(bucket string, prefix string) []string {
	var allObjects []string
	var continuationToken *string
	var isTruncated bool
	var batchObjects []string

	for {
		batchObjects, isTruncated, continuationToken = ListBatchOjbects(bucket, prefix, continuationToken)

		if len(batchObjects) > 0 {
			allObjects = append(allObjects, batchObjects...)
		}

		if !isTruncated {
			break
		}
	}
	log.Println("all objects count: ", len(allObjects))
	return allObjects
}

func ListBatchOjbects(bucket string, prefix string, continuationToken *string) ([]string, bool, *string) {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	var input *s3.ListObjectsV2Input

	if continuationToken != nil {
		input = &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
		}
	} else {
		input = &s3.ListObjectsV2Input{
			Bucket: &bucket,
			Prefix: &prefix,
		}
	}

	result, err := client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("ListBatchOjbects get objects count: ", len(result.Contents), "isTurnTruncated: ", *result.IsTruncated)
	log.Println("NextContinuationToken: ", result.NextContinuationToken)

	objectList := make([]string, len(result.Contents))

	for i, object := range result.Contents {
		objectList[i] = *object.Key
	}
	return objectList, *result.IsTruncated, result.NextContinuationToken
}

func ReadObjectAsString(bucket string, key string) string {
	// downloadObject downloads an object from an S3 bucket.

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	input := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	result, err := client.GetObject(context.TODO(), input)
	if err != nil {
		log.Fatal(err)
	}

	contentBody := result.Body // Type: io.ReadCloser
	buf := new(bytes.Buffer)

	if strings.HasSuffix(key, ".gz") {
		unzipped, err := gzip.NewReader(contentBody)
		if err != nil {
			log.Fatal(err)
		}
		defer unzipped.Close()

		_, err = buf.ReadFrom(unzipped)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		_, err = buf.ReadFrom(contentBody)
		if err != nil {
			log.Fatal(err)
		}
	}

	content := buf.String()
	return content
}

func ProcessS3ObjectLineByLine(bucket string, key string, processFn func(string) BreakContinue) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	input := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	result, err := client.GetObject(context.TODO(), input)
	if err != nil {
		log.Fatal(err)
	}

	contentBody := result.Body // Type: io.ReadCloser

	if strings.HasSuffix(key, ".gz") {
		unzipped, err := gzip.NewReader(contentBody)
		if err != nil {
			log.Fatal(err)
		}
		defer unzipped.Close()
		ProcessStreamLineByLine(unzipped, processFn)
	} else {
		ProcessStreamLineByLine(contentBody, processFn)
	}

}

func ProcessStreamLineByLine(contentReader io.ReadCloser, processFn func(string) BreakContinue) {
	defer contentReader.Close()

	scanner := bufio.NewScanner(contentReader)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			BreakContinue := processFn(line)
			if BreakContinue.Break {
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}
