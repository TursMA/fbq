package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
)

type ConfAccount struct {
	Id  string `json:"id"`
	App string `json:"client_id"`
}

type ConfApp struct {
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_id"`
	Token        string `json:"token"`
}

type Conf struct {
	Accounts map[string]*ConfAccount `json:"accounts"`
	Apps     map[string]*ConfApp     `json:"apps"`
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
	return c
}

func (c *Conf) AddApp(clientId, clientSecret, token string) {
	if c.Apps == nil {
		c.Apps = make(map[string]*ConfApp)
	}
	c.Apps[clientId] = &ConfApp{
		ClientId:     clientId,
		ClientSecret: clientSecret,
		Token:        token,
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
