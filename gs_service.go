package main

import (
	"fmt"
	"log"
	"sync"

	storage "github.com/luxola/selenium/utils/storage"
)

const (
	cores int = 10
)

type FileToStore struct {
	bs       []byte
	filename string
	wg       *sync.WaitGroup
}

func NewFileToStore(bs []byte, filename string) *FileToStore {
	fts := new(FileToStore)
	fts.bs = bs
	fts.filename = filename
	return fts
}

func (fts *FileToStore) Store(service *storage.StorageClient) {
	fmt.Println(string(fts.bs))
	service.Store("lx-ga", fts.filename, fts.bs)
}

func (fts *FileToStore) Stringer() string {
	return fmt.Sprintf("%v", fts.filename)
}

func (fts *FileToStore) StringerLogger() string {
	return fmt.Sprintf("Stored %v", fts.filename)
}

func (fts *FileToStore) Logger() {
	log.Println(fts.StringerLogger())
}

type StorageService struct {
	ch chan *FileToStore
	wg *sync.WaitGroup
}

func NewStorageService(service *storage.StorageClient) *StorageService {
	ss := new(StorageService)
	ss.ch = make(chan *FileToStore)
	ss.wg = new(sync.WaitGroup)
	for i := 0; i < cores; i++ {
		go func(ss *StorageService) {
			for {
				fts := <-ss.ch
				fts.Store(service)
				fts.Logger()
				ss.wg.Done()
			}
		}(ss)
	}
	return ss
}

func (ss *StorageService) Wait() {
	ss.wg.Wait()
}

func (ss *StorageService) AddFile(fts *FileToStore) {
	ss.wg.Add(1)
	ss.ch <- fts
}
