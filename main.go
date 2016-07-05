package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	googleCredPath string
	delta          int
	modToken       bool
	modAccount     bool
	confFileName   string
)

func init() {
	flagModToken := flag.Bool("token", false, "Add token")
	flagModAccount := flag.Bool("account", false, "Add account")
	flagGoogleCredPath := flag.String("g", "", "Google JWT")
	flagConfFilePath := flag.String("c", "conf.json", "Config file")
	flagDelta := flag.Int("d", 10, "Delta")
	flag.Parse()
	googleCredPath = *flagGoogleCredPath
	delta = *flagDelta
	modToken = *flagModToken
	modAccount = *flagModAccount
	confFileName = *flagConfFilePath
}

func Token() {
	var clientId, clientSecret, previousToken string

	io := bufio.NewReader(os.Stdin)

	fmt.Println("Client Id:")
	fmt.Fscan(io, &clientId)
	fmt.Println("Client Secret:")
	fmt.Fscan(io, &clientSecret)
	fmt.Println("Short Token:")
	fmt.Fscan(io, &previousToken)

	app := newFBApp(clientId, clientSecret)
	accessToken := app.RenewToken(previousToken)

	conf := LoadConf()
	conf.AddApp(clientId, clientSecret, accessToken.Token, accessToken.Expires)
	conf.Save()
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

	dc := newDailyCatch(time.Now().AddDate(0, 0, -1*delta))

	wg := new(sync.WaitGroup)

	for _, account := range conf.Accounts {
		a := newFBAdAccount(account.Id)
		t := newFBAccessToken(conf.Apps[account.App].Token)
		as := newFBAdService(dc, a, t)
		wg.Add(1)
		// go func() {
		func() {
			as.GetAds()
			as.GetAdInsights()
			as.Store()
			wg.Done()
		}()
	}

	wg.Wait()

	dc.ToBQ()

}

func main() {
	if modToken {
		Token()
	} else if modAccount {
		Account()
	} else {
		Run()
	}

	fmt.Println("Done.")
}
