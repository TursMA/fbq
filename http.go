package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

func HttpGet(url string) *http.Response {
	fmt.Printf("GET %v\n", url)
	resp, err := http.Get(url)
	catchError(err)
	if resp.StatusCode != 200 {
		if resp.StatusCode == 400 {
			fmt.Printf("RETRY--- ERROR %v while getting %v -----\n", resp.Status, url)
			time.Sleep(10 * time.Minute)
			return HttpGet(url)
		} else {
			log.Fatalf("----- ERROR %v while getting %v -----\n", resp.Status, url)
		}
	}
	return resp
}
