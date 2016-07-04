package main

import (
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"
	storage "google.golang.org/api/storage/v1"
	"io/ioutil"
	"log"
	"net/http"
)

func catchError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getGoogleHttpClient() *http.Client {
	data, err := ioutil.ReadFile(googleCredPath)
	catchError(err)

	conf, err := google.JWTConfigFromJSON(data, []string{bigquery.BigqueryScope, storage.CloudPlatformScope}...)
	catchError(err)

	client := conf.Client(oauth2.NoContext)
	return client
}
