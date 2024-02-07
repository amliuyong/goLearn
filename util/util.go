package util

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
)

func DecodeBase64Gzip(dataStr string) (error, string) {
	decoded, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		return err, ""
	}

	unzipped, err := gzip.NewReader(bytes.NewReader(decoded))
	if err != nil {
		return err, ""
	}
	defer unzipped.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(unzipped)
	if err != nil {
		return err, ""
	}

	return nil, buf.String()
}
