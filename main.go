package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	storage "github.com/luxola/selenium/utils/storage"
)

var (
	googleCredPath string
	delta          int
	modOrders      bool
	modAds         bool
	modAccount     bool
	modBQ          bool
	confFileName   string
	now            time.Time
	ss             *StorageService
)

func init() {
	flagModOrders := flag.Bool("orders", false, "Orders")
	flagModAds := flag.Bool("ads", false, "Ads")
	flagModBQ := flag.Bool("bq", false, "Send to BigQuery")
	flagModAccount := flag.Bool("account", false, "Add account")
	flagGoogleCredPath := flag.String("g", "", "Google JWT")
	flagConfFilePath := flag.String("c", "conf.json", "Config file")
	flagDelta := flag.Int("d", 10, "Delta")
	flagNow := flag.String("now", time.Now().Format("2006-01-02"), "Date")
	flag.Parse()
	googleCredPath = *flagGoogleCredPath
	delta = *flagDelta
	modOrders = *flagModOrders
	modAds = *flagModAds
	modBQ = *flagModBQ
	modAccount = *flagModAccount
	confFileName = *flagConfFilePath
	n, err := time.Parse("2006-01-02", *flagNow)
	if err != nil {
		log.Fatalln(err)
	} else {
		now = n
	}
}

func Account() {
	var clientId, accountId string

	io := bufio.NewReader(os.Stdin)

	fmt.Println("Client Id:")
	fmt.Fscan(io, &clientId)
	fmt.Println("Account Id:")
	fmt.Fscan(io, &accountId)

	conf := LoadConf()
	conf.AddAccount(clientId, accountId)
	conf.Save()
}

func Run() {
	conf := LoadConf()

	token := newFBAccessToken(conf.Token)
	pixel := newFBPixel(conf.PixelId)
	app := newFBApp(conf.AppId, "")

	dc := newDailyCatch(now.AddDate(0, 0, -1*delta), token, app, pixel)

	wg := new(sync.WaitGroup)

	if modOrders {
		wg.Add(1)
		go func() {
			dc.GetOrders()
			dc.StoreOrders()
			wg.Done()
		}()
	}

	if modAds {
		for _, account := range conf.Accounts {
			a := newFBAdAccount(account.Id)
			as := newFBAdService(dc, a)
			wg.Add(1)
			go func() {
				as.GetAds()
				as.GetAdInsights()
				as.Store()
				wg.Done()
			}()
		}
	}

	wg.Wait()

	ss.Wait()

	if modBQ {
		dc.ToBQ()
	}
}

func main() {
	ss = NewStorageService(storage.NewStorageClient(googleCredPath))
	if modAccount {
		Account()
	} else {
		Run()
	}

	fmt.Println("Done.")
}
