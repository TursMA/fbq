package main

import (
	"fmt"
	"log"
	"net/http"
)

func HttpGet(url string) *http.Response {
	fmt.Printf("GET %v\n", url)
	resp, err := http.Get(url)
	catchError(err)
	if resp.StatusCode != 200 {
		log.Fatalf("----- ERROR %v while getting %v -----\n", resp.Status, url)
	}
	return resp
}
