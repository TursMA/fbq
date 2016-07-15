package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type ConfAccount struct {
	Id  string `json:"id"`
	App string `json:"client_id"`
}

type ConfApp struct {
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_id"`
	Token        string `json:"token"`
	Expires      int32  `json:"expires_in"`
}

type Conf struct {
	Accounts map[string]*ConfAccount `json:"accounts"`
	Apps     map[string]*ConfApp     `json:"apps"`
	Token    string                  `json:"token"`
	PixelId  string                  `json:"pixel_id"`
	AppId    string                  `json:"app_id"`
}

func (c *Conf) Save() {
	b, err := json.MarshalIndent(c, "", "\t")
	catchError(err)
	catchError(ioutil.WriteFile(confFileName, b, 0644))
}

func LoadConf() *Conf {
	c := new(Conf)
	body, err := ioutil.ReadFile(confFileName)
	if err != nil {
		c.Save()
		return LoadConf()
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	catchError(dec.Decode(&c))

	c.Check()

	return c
}

func (c *Conf) Check() {
	if c.PixelId == "" || c.AppId == "" || c.Token == "" {
		io := bufio.NewReader(os.Stdin)
		if c.PixelId == "" {
			var pixelId string
			fmt.Println("Pixel Id:")
			fmt.Fscan(io, &pixelId)
			c.SetPixelId(pixelId)
		}
		if c.AppId == "" {
			var appId string
			fmt.Println("App Id:")
			fmt.Fscan(io, &appId)
			c.SetAppId(appId)
		}
		if c.Token == "" {
			var token string
			fmt.Println("Token:")
			fmt.Fscan(io, &token)
			c.SetToken(token)
		}
		c.Save()
	}
}

func (c *Conf) AddApp(clientId, clientSecret, token string, expires int32) {
	if c.Apps == nil {
		c.Apps = make(map[string]*ConfApp)
	}
	c.Apps[clientId] = &ConfApp{
		ClientId:     clientId,
		ClientSecret: clientSecret,
		Token:        token,
		Expires:      expires,
	}
}

func (c *Conf) AddAccount(clientId, accountId string) {
	if c.Accounts == nil {
		c.Accounts = make(map[string]*ConfAccount)
	}
	c.Accounts[accountId] = &ConfAccount{
		App: clientId,
		Id:  accountId,
	}
}

func (c *Conf) SetPixelId(pixelId string) {
	c.PixelId = pixelId
}

func (c *Conf) SetAppId(appId string) {
	c.AppId = appId
}

func (c *Conf) SetToken(token string) {
	c.Token = token
}
