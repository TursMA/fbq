package main

import (
	"fmt"
	bigquery "google.golang.org/api/bigquery/v2"
	"net/http"
	"time"
)

const (
	bucketName string = "lx-ga"
)

func getBQService(client *http.Client) *bigquery.Service {
	service, err := bigquery.New(client)
	catchError(err)
	return service
}

func generateBQTable(tableId string) *bigquery.TableReference {
	return &bigquery.TableReference{
		DatasetId: datasetId,
		ProjectId: projectId,
		TableId:   tableId,
	}
}

func writeBQTable(gsPath, tableId string, schema *bigquery.TableSchema) {
	jobsService := bigquery.NewJobsService(getBQService(getGoogleHttpClient()))

	job := bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: &bigquery.JobConfigurationLoad{
				DestinationTable: generateBQTable(tableId),
				Schema:           schema,
				SourceFormat:     "NEWLINE_DELIMITED_JSON",
				SourceUris:       []string{fmt.Sprintf("gs://%v/%v", bucketName, gsPath)},
				WriteDisposition: "WRITE_TRUNCATE",
			},
		},
	}

	jobsInsertCall := jobsService.Insert(projectId, &job)
	insertJob, err := jobsInsertCall.Do()
	catchError(err)

	jobsGetCall := jobsService.Get(projectId, insertJob.JobReference.JobId)
	gotJob, err := jobsGetCall.Do()
	catchError(err)

	fmt.Println(gotJob.Status)
	if gotJob.Status.ErrorResult != nil {
		fmt.Println(gotJob.Status.ErrorResult)
		for i := 0; i < len(gotJob.Status.Errors); i++ {
			fmt.Println(gotJob.Status.Errors[i])
		}
	}

	for gotJob.Status.State != "DONE" {
		jobsGetCall = jobsService.Get(projectId, insertJob.JobReference.JobId)
		gotJob, err = jobsGetCall.Do()
		catchError(err)
		fmt.Println(gotJob.Status)
		if gotJob.Status.ErrorResult != nil {
			fmt.Println(gotJob.Status.ErrorResult)
			for i := 0; i < len(gotJob.Status.Errors); i++ {
				fmt.Println(gotJob.Status.Errors[i])
			}
		}
		time.Sleep(time.Second)
	}
}
