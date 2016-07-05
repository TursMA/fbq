package main

import (
	"encoding/json"
	"fmt"
	bigquery "google.golang.org/api/bigquery/v2"
	"net/url"
	"time"
)

const (
	Endpoint  string = "https://graph.facebook.com"
	Version   string = "v2.6"
	projectId string = "luxola.com:luxola-analytics"
	datasetId string = "facebook"
)

type FBAccessToken struct {
	Token     string `json:"access_token"`
	TokenType string `json:"token_type"`
	Expires   int32  `json:"expires_in"`
}

func newFBAccessToken(s string) *FBAccessToken {
	t := new(FBAccessToken)
	t.Token = s
	return t
}

type FBApp struct {
	ClientId     string
	ClientSecret string
}

func newFBApp(clientId, clientSecret string) *FBApp {
	a := new(FBApp)
	a.ClientId = clientId
	a.ClientSecret = clientSecret
	return a
}

func (a *FBApp) getRenewUrl(previousToken string) string {
	path := "oauth/access_token"
	v := url.Values{}
	v.Set("grant_type", "fb_exchange_token")
	v.Set("client_id", a.ClientId)
	v.Set("client_secret", a.ClientSecret)
	v.Set("fb_exchange_token", previousToken)
	return fmt.Sprintf("%v/%v/%v?%v", Endpoint, Version, path, v.Encode())
}

func (a *FBApp) RenewToken(s string) *FBAccessToken {
	resp := HttpGet(a.getRenewUrl(s))
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	d := new(FBAccessToken)
	catchError(dec.Decode(d))
	return d
}

type FBAdAccount struct {
	Id string
}

func newFBAdAccount(id string) *FBAdAccount {
	aa := new(FBAdAccount)
	aa.Id = id
	return aa
}

type DailyCatch struct {
	Day time.Time
}

func newDailyCatch(d time.Time) *DailyCatch {
	dc := new(DailyCatch)
	dc.Day = d
	return dc
}

func (d *DailyCatch) BQAdSchema() *bigquery.TableSchema {
	return &bigquery.TableSchema{
		Fields: []*bigquery.TableFieldSchema{
			{Mode: "NULLABLE", Name: "id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "name", Type: "STRING"},
			{Mode: "NULLABLE", Name: "account_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "bid_amount", Type: "INTEGER"},
			{Mode: "NULLABLE", Name: "bid_info", Type: "RECORD", Fields: []*bigquery.TableFieldSchema{
				{Mode: "NULLABLE", Name: "IMPRESSIONS", Type: "INTEGER"},
				{Mode: "NULLABLE", Name: "CLICKS", Type: "INTEGER"},
				{Mode: "NULLABLE", Name: "ACTIONS", Type: "INTEGER"},
				{Mode: "NULLABLE", Name: "REACH", Type: "INTEGER"},
				{Mode: "NULLABLE", Name: "SOCIAL", Type: "INTEGER"},
			}},
			{Mode: "NULLABLE", Name: "bid_type", Type: "STRING"},
			{Mode: "NULLABLE", Name: "status", Type: "STRING"},
			{Mode: "NULLABLE", Name: "created_time", Type: "STRING"},
			{Mode: "NULLABLE", Name: "updated_time", Type: "STRING"},
			{Mode: "NULLABLE", Name: "adset", Type: "RECORD", Fields: []*bigquery.TableFieldSchema{
				{Mode: "NULLABLE", Name: "name", Type: "STRING"},
				{Mode: "NULLABLE", Name: "id", Type: "STRING"},
			}},
			{Mode: "NULLABLE", Name: "campaign", Type: "RECORD", Fields: []*bigquery.TableFieldSchema{
				{Mode: "NULLABLE", Name: "name", Type: "STRING"},
				{Mode: "NULLABLE", Name: "id", Type: "STRING"},
			}},
			{Mode: "NULLABLE", Name: "creative", Type: "RECORD", Fields: []*bigquery.TableFieldSchema{
				{Mode: "NULLABLE", Name: "name", Type: "STRING"},
				{Mode: "NULLABLE", Name: "id", Type: "STRING"},
			}},
		},
	}
}

func (d *DailyCatch) BQAdInsightSchema() *bigquery.TableSchema {
	return &bigquery.TableSchema{
		Fields: []*bigquery.TableFieldSchema{
			{Mode: "NULLABLE", Name: "date_start", Type: "STRING"},
			{Mode: "NULLABLE", Name: "date_stop", Type: "STRING"},
			{Mode: "NULLABLE", Name: "account_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "account_name", Type: "STRING"},
			{Mode: "NULLABLE", Name: "campaign_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "campaign_name", Type: "STRING"},
			{Mode: "NULLABLE", Name: "adset_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "adset_name", Type: "STRING"},
			{Mode: "NULLABLE", Name: "ad_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "ad_name", Type: "STRING"},
			{Mode: "NULLABLE", Name: "impressions", Type: "STRING"},
			{Mode: "NULLABLE", Name: "unique_impressions", Type: "INTEGER"},
			{Mode: "NULLABLE", Name: "clicks", Type: "INTEGER"},
			{Mode: "NULLABLE", Name: "unique_clicks", Type: "INTEGER"},
			{Mode: "NULLABLE", Name: "spend", Type: "FLOAT"},
		},
	}
}

func (d *DailyCatch) ToBQ() {
	writeBQTable("facebook/ads/*", "ads", d.BQAdSchema())
	writeBQTable("facebook/insights/*", "insights", d.BQAdInsightSchema())
}

type FBAdSet struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

type FBCampaign struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

type FBCreative struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

type FBAd struct {
	Id          string           `json:"id"`
	Name        string           `json:"name"`
	AccountId   string           `json:"account_id"`
	BidAmount   int32            `json:"bid_amount"`
	BidInfo     map[string]int32 `json:"bid_info"`
	BidType     string           `json:"bid_type"`
	Status      string           `json:"status"`
	CreatedTime string           `json:"created_time"`
	UpdatedTime string           `json:"updated_time"`
	AdSet       *FBAdSet         `json:"adset"`
	Campaign    *FBCampaign      `json:"campaign"`
	Creative    *FBCreative      `json:"creative"`
}

func (a *FBAd) getFileName() string {
	objectPrefix := "facebook/ads"
	d, err := time.Parse("2006-01-02", a.CreatedTime[:10])
	catchError(err)
	return fmt.Sprintf("%v/%v/%v.json.gz", objectPrefix, d.Format("2006/01/02"), a.Id)
}

func (a *FBAd) Store() {
	GSStore(a, a.getFileName())
}

type FBAdInsight struct {
	DateStart         string  `json:"date_start"`
	DateStop          string  `json:"date_stop"`
	AccountId         string  `json:"account_id"`
	AccountName       string  `json:"account_name"`
	CampaignId        string  `json:"campaign_id"`
	CampaignName      string  `json:"campaign_name"`
	AdSetId           string  `json:"adset_id"`
	AdSetName         string  `json:"adset_name"`
	AdId              string  `json:"ad_id"`
	AdName            string  `json:"ad_name"`
	Impressions       string  `json:"impressions"`
	UniqueImpressions int32   `json:"unique_impressions"`
	Clicks            int32   `json:"clicks"`
	UniqueClicks      int32   `json:"unique_clicks"`
	Spend             float32 `json:"spend"`
}

func (a *FBAdInsight) getFileName() string {
	objectPrefix := "facebook/insights"
	d, err := time.Parse("2006-01-02", a.DateStart[:10])
	catchError(err)
	return fmt.Sprintf("%v/%v/%v.json.gz", objectPrefix, d.Format("2006/01/02"), a.AdId)
}

func (a *FBAdInsight) Store() {
	GSStore(a, a.getFileName())
}

type FBAdService struct {
	DailyCatch  *DailyCatch
	AdAccount   *FBAdAccount
	AccessToken *FBAccessToken
	Ads         []*FBAd
	AdInsights  []*FBAdInsight
}

func newFBAdService(dc *DailyCatch, a *FBAdAccount, t *FBAccessToken) *FBAdService {
	s := new(FBAdService)
	s.DailyCatch = dc
	s.AdAccount = a
	s.AccessToken = t
	return s
}

func (s *FBAdService) getAdsFields() string {
	return "name,account_id,adset{id,name},bid_amount,bid_info,bid_type,status,created_time,updated_time,campaign{id,name},creative{id,name}"
}

func (s *FBAdService) getAdInsightsFields() string {
	return "date_start,date_stop,account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,unique_impressions,clicks,unique_clicks,spend"
}

func (s *FBAdService) getAdsURL() string {
	path := fmt.Sprintf("act_%v/ads", s.AdAccount.Id)
	return fmt.Sprintf("%v/%v/%v?access_token=%v&fields=%v&updated_since=%v", Endpoint, Version, path, s.AccessToken.Token, s.getAdsFields(), s.DailyCatch.Day.Unix())
}

func (s *FBAdService) getInsightsURL(date time.Time) string {
	path := fmt.Sprintf("act_%v/insights", s.AdAccount.Id)
	d0 := date.Format("2006-01-02")
	d := fmt.Sprintf(`{"since": "%v","until": "%v"}`, d0, d0)
	v := url.Values{}
	v.Set("time_range", d)
	v.Set("access_token", s.AccessToken.Token)
	v.Set("level", "ad")
	v.Set("fields", s.getAdInsightsFields())
	return fmt.Sprintf("%v/%v/%v?%v", Endpoint, Version, path, v.Encode())
}

func (s *FBAdService) GetAdsPage(url string) {
	resp := HttpGet(url)
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	var d struct {
		Ads    []*FBAd `json:"data"`
		Paging struct {
			Next string `json:"next"`
		} `json:"paging"`
	}
	catchError(dec.Decode(&d))
	s.Ads = append(s.Ads, d.Ads...)
	if d.Paging.Next != "" {
		s.GetAdsPage(d.Paging.Next)
	}
}

func (s *FBAdService) GetAdInsightsPage(url string) {
	resp := HttpGet(url)
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	var d struct {
		AdInsights []*FBAdInsight `json:"data"`
		Paging     struct {
			Next string `json:"next"`
		} `json:"paging"`
	}
	catchError(dec.Decode(&d))
	s.AdInsights = append(s.AdInsights, d.AdInsights...)
	if d.Paging.Next != "" {
		s.GetAdInsightsPage(d.Paging.Next)
	}
}

func (s *FBAdService) GetAds() {
	s.Ads = make([]*FBAd, 0)
	s.GetAdsPage(s.getAdsURL())
}

func (s *FBAdService) GetAdInsights() {
	s.AdInsights = make([]*FBAdInsight, 0)
	d := s.DailyCatch.Day
	now := time.Now()
	for now.Sub(d).Nanoseconds() > 0 {
		s.GetAdInsightsPage(s.getInsightsURL(d))
		d = d.AddDate(0, 0, 1)
	}
}

func (s *FBAdService) StoreAds() {
	for i := 0; i < len(s.Ads); i++ {
		ad := s.Ads[i]
		ad.Store()
	}
}

func (s *FBAdService) StoreInsights() {
	for i := 0; i < len(s.AdInsights); i++ {
		ai := s.AdInsights[i]
		ai.Store()
	}
}

func (s *FBAdService) Store() {
	s.StoreAds()
	s.StoreInsights()
}
