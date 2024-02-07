package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/amliuyong/go-aws-s3/util"
)

func FindRidForEventIdInS3Prefix(bucket string, prefix string, eventId string) (bool, string, string) {
	objects := util.ListAllOjbects(bucket, prefix)

	var foundRid string
	var found bool
	var foundKey string

	for _, key := range objects {
		log.Printf("search %s\n", key)
		found, foundRid = FindRidForEventIdInS3Object(bucket, key, eventId)
		if found {
			foundKey = key
			break
		}
	}
	log.Println("found: ", found, "foundRid: ", foundRid, "foundKey: ", foundKey)
	return found, foundRid, foundKey
}

func FindRidForEventIdInS3Object(bucket string, key string, eventId string) (bool, string) {
	streamingProcess := false

	streamingProcessEnv := os.Getenv("STREAMING_PROCESS")
	if streamingProcessEnv == "true" {
		streamingProcess = true
	}

	if streamingProcess {
		return FindRidForEventIdInS3ObjectV2(bucket, key, eventId)
	}
	content := util.ReadObjectAsString(bucket, key)
	return FindRIdForEventId(content, eventId)

}

func FindRidForEventIdInS3ObjectV2(bucket string, key string, eventId string) (bool, string) {
	var found bool
	var foundRid string
	var lineCount int = 0

	util.ProcessS3ObjectLineByLine(bucket, key, func(line string) util.BreakContinue {
		ok, event, dataArr := ParseLine(line)
		lineCount++

		if ok {
			for _, dataMap := range dataArr {
				if dataMap["event_id"] == eventId {
					rid, ok := event["rid"].(string)
					if ok {
						found = true
						foundRid = rid
						return util.BreakContinue{Break: true}
					}
				}
			}
		}
		return util.BreakContinue{Break: false}
	})

	log.Println("searched line count: ", lineCount, "key:", key)

	return found, foundRid
}

func FindIpForRid(content string, rid string) string {
	if content == "" {
		return ""
	}

	lines := strings.Split(content, "\n")

	for _, line := range lines {
		var event map[string]interface{}

		json.Unmarshal([]byte(line), &event)

		if event["rid"] == rid {
			ip, ok := event["ip"].(string)
			if ok {
				return ip
			}
		}
	}
	return ""
}

func FindRIdForEventId(content string, eventId string) (bool, string) {
	if content == "" {
		return false, ""
	}

	lines := strings.Split(content, "\n")

	for _, line := range lines {
		ok, event, dataArr := ParseLine(line)

		if !ok {
			log.Println("parse line failed: ", line)
			continue
		}

		for _, dataMap := range dataArr {
			if dataMap["event_id"] == eventId {
				rid, ok := event["rid"].(string)
				if ok {
					return true, rid
				}
			}
		}
	}
	return false, ""
}

func ParseLine(line string) (bool, map[string]interface{}, []map[string]interface{}) {
	var event map[string]interface{}
	var dataArr []map[string]interface{}

	err1 := json.Unmarshal([]byte(line), &event)
	if err1 != nil {
		log.Println("error: ", err1)
		return false, nil, nil
	}

	dataStr := event["data"].(string)

	if dataStr == "" {
		return false, nil, nil
	}

	if strings.HasPrefix(dataStr, "{") {
		dataStr = "[" + dataStr + "]"
	}

	if !strings.HasPrefix(dataStr, "[") {
		err2, decodedDataStr := util.DecodeBase64Gzip(dataStr)
		if err2 != nil {
			return false, nil, nil
		}
		dataStr = decodedDataStr
	}

	err3 := json.Unmarshal([]byte(dataStr), &dataArr)

	if err3 != nil {
		return false, nil, nil
	}

	return true, event, dataArr
}
