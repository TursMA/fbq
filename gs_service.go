package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	storage "google.golang.org/api/storage/v1"
	"log"
	"net/http"
	"strings"
	"time"
)

func getGSService(client *http.Client) *storage.Service {
	service, err := storage.New(client)
	catchError(err)
	return service
}

func GSStore(c interface{}, fileName string) {

	object := &storage.Object{Name: fileName}

	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	bs, err := json.Marshal(c)
	catchError(err)
	w.Write(bs)
	w.Close()

	file := strings.NewReader(b.String())

	if _, err := getGSService(getGoogleHttpClient()).Objects.Insert(bucketName, object).Media(file).Do(); err == nil {
		// fmt.Printf("Created object %v at location %v\n\n", res.Name, res.SelfLink)
	} else {
		log.Printf("Objects.Insert failed: %v\n", err)
		time.Sleep(10 * time.Second)
		GSStore(c, fileName)
	}
}
