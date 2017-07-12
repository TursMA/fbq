package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
)

const (
	Endpoint  string = "https://graph.facebook.com"
	Version   string = "v2.9"
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

type FBPixel struct {
	Id string
}

func newFBPixel(pixelId string) *FBPixel {
	a := new(FBPixel)
	a.Id = pixelId
	return a
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
	Day         time.Time
	Orders      []*FBOrder
	AccessToken *FBAccessToken
	App         *FBApp
	Pixel       *FBPixel
}

func newDailyCatch(d time.Time, t *FBAccessToken, a *FBApp, p *FBPixel) *DailyCatch {
	dc := new(DailyCatch)
	dc.Day = d
	dc.AccessToken = t
	dc.App = a
	dc.Pixel = p
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
			{Mode: "NULLABLE", Name: "ad_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "impressions", Type: "STRING"},
			{Mode: "NULLABLE", Name: "unique_impressions", Type: "INTEGER"},
			{Mode: "NULLABLE", Name: "clicks", Type: "INTEGER"},
			{Mode: "NULLABLE", Name: "unique_clicks", Type: "INTEGER"},
			{Mode: "NULLABLE", Name: "spend", Type: "FLOAT"},
		},
	}
}

func (d *DailyCatch) BQOrderSchema() *bigquery.TableSchema {
	return &bigquery.TableSchema{
		Fields: []*bigquery.TableFieldSchema{
			{Mode: "NULLABLE", Name: "order_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "pixel_id", Type: "STRING"},
			{Mode: "NULLABLE", Name: "conversion_device", Type: "STRING"},
			{Mode: "NULLABLE", Name: "order_timestamp", Type: "STRING"},
			{Mode: "NULLABLE", Name: "attribution_type", Type: "STRING"},
			{Mode: "REPEATED", Name: "attributions", Type: "RECORD", Fields: []*bigquery.TableFieldSchema{
				{Mode: "NULLABLE", Name: "ad_id", Type: "STRING"},
				{Mode: "NULLABLE", Name: "action_type", Type: "STRING"},
				{Mode: "NULLABLE", Name: "impression_cost", Type: "FLOAT"},
				{Mode: "NULLABLE", Name: "click_cost", Type: "FLOAT"},
				{Mode: "NULLABLE", Name: "impression_timestamp", Type: "STRING"},
				{Mode: "NULLABLE", Name: "placement", Type: "STRING"},
				{Mode: "NULLABLE", Name: "device", Type: "STRING"},
			}},
		},
	}
}

func (d *DailyCatch) ToBQ() {
	writeBQTable("facebook/ads/*", "ads", d.BQAdSchema())
	writeBQTable("facebook/insights/*", "insights", d.BQAdInsightSchema())
	writeBQTable("facebook/orders/*", "orders", d.BQOrderSchema())
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
	bs, err := json.Marshal(a)
	catchError(err)
	ss.AddFile(NewFileToStore(bs, a.getFileName()))
}

type FBAdInsight struct {
	DateStart   string `json:"date_start"`
	DateStop    string `json:"date_stop"`
	AccountId   string `json:"account_id"`
	AdId        string `json:"ad_id"`
	Impressions string `json:"impressions"`
	// UniqueImpressions *string `json:"unique_impressions"`
	Clicks       string `json:"clicks"`
	UniqueClicks string `json:"unique_clicks"`
	Spend        string `json:"spend"`
}

func (a *FBAdInsight) getFileName() string {
	objectPrefix := "facebook/insights"
	d, err := time.Parse("2006-01-02", a.DateStart[:10])
	catchError(err)
	return fmt.Sprintf("%v/%v/%v.json.gz", objectPrefix, d.Format("2006/01/02"), a.AdId)
}

func (a *FBAdInsight) Store() {
	bs, err := json.Marshal(a)
	catchError(err)
	ss.AddFile(NewFileToStore(bs, a.getFileName()))
}

type FBOrder struct {
	OrderId          string                `json:"order_id"`
	PixelId          string                `json:"pixel_id"`
	ConversionDevice string                `json:"conversion_device"`
	OrderTimestamp   string                `json:"order_timestamp"`
	AttributionType  string                `json:"attribution_type"`
	Attributions     []*FBOrderAttribution `json:"attributions"`
}

func (a *FBOrder) getFileName() string {
	objectPrefix := "facebook/orders"
	d, err := time.Parse("2006-01-02", a.OrderTimestamp[:10])
	catchError(err)
	return fmt.Sprintf("%v/%v/%v.json.gz", objectPrefix, d.Format("2006/01/02"), a.OrderId)
}

func (a *FBOrder) Store() {
	bs, err := json.Marshal(a)
	catchError(err)
	ss.AddFile(NewFileToStore(bs, a.getFileName()))
}

type FBOrderAttribution struct {
	AdId                string  `json:"ad_id"`
	ActionType          string  `json:"action_type"`
	ImpressionCost      float32 `json:"impression_cost"`
	ClickCost           float32 `json:"click_cost"`
	ImpressionTimestamp string  `json:"impression_timestamp"`
	Placement           string  `json:"placement"`
	Device              string  `json:"device"`
}

type FBAdService struct {
	DailyCatch *DailyCatch
	AdAccount  *FBAdAccount
	Ads        []*FBAd
	AdInsights []*FBAdInsight
}

func newFBAdService(dc *DailyCatch, a *FBAdAccount) *FBAdService {
	s := new(FBAdService)
	s.DailyCatch = dc
	s.AdAccount = a
	return s
}

func (s *FBAdService) getAdsFields() string {
	return "name,account_id,adset{id,name},bid_amount,bid_info,bid_type,status,created_time,updated_time,campaign{id,name},creative{id,name}"
}

func (s *FBAdService) getAdInsightsFields() string {
	// return "date_start,date_stop,account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,unique_impressions,clicks,unique_clicks,spend"
	return "date_start,date_stop,account_id,ad_id,impressions,clicks,unique_clicks,spend"
}

func (s *FBAdService) getAdsURL() string {
	path := fmt.Sprintf("act_%v/ads", s.AdAccount.Id)
	return fmt.Sprintf("%v/%v/%v?access_token=%v&fields=%v&updated_since=%v", Endpoint, Version, path, s.DailyCatch.AccessToken.Token, s.getAdsFields(), s.DailyCatch.Day.Unix())
}

func (s *FBAdService) getInsightsURL(date time.Time) string {
	path := fmt.Sprintf("act_%v/insights", s.AdAccount.Id)
	d0 := date.Format("2006-01-02")
	d := fmt.Sprintf(`{"since": "%v","until": "%v"}`, d0, d0)
	v := url.Values{}
	v.Set("time_range", d)
	v.Set("access_token", s.DailyCatch.AccessToken.Token)
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

func (dc *DailyCatch) getOrdersURL(date time.Time) string {
	path := fmt.Sprintf("931150673581441/order_id_attributions")
	v := url.Values{}
	v.Set("since", fmt.Sprintf("%v", date.Unix()))
	v.Set("until", fmt.Sprintf("%v", date.Unix()+60*60*24))
	v.Set("app_id", dc.App.ClientId)
	v.Set("pixel_id", dc.Pixel.Id)
	v.Set("access_token", dc.AccessToken.Token)
	return fmt.Sprintf("%v/%v/%v?%v", Endpoint, Version, path, v.Encode())
}

func (d *DailyCatch) GetOrdersPage(url string) {
	resp := HttpGet(url)
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	var data struct {
		Orders []*FBOrder `json:"data"`
		Paging struct {
			Next string `json:"next"`
		} `json:"paging"`
	}
	catchError(dec.Decode(&data))
	d.Orders = append(d.Orders, data.Orders...)
	if data.Paging.Next != "" {
		d.GetOrdersPage(data.Paging.Next)
	}
}

func (s *FBAdService) GetAds() {
	s.Ads = make([]*FBAd, 0)
	s.GetAdsPage(s.getAdsURL())
}

func (s *FBAdService) GetAdInsights() {
	s.AdInsights = make([]*FBAdInsight, 0)
	d := s.DailyCatch.Day
	wg := new(sync.WaitGroup)
	for now.Sub(d).Nanoseconds() > 0 {
		wg.Add(1)
		year, month, day := d.Date()
		d0 := time.Date(year, month, day, 0, 0, 0, 0, d.Location())
		go func() {
			s.GetAdInsightsPage(s.getInsightsURL(d0))
			wg.Done()
		}()
		d = d.AddDate(0, 0, 1)
	}
	wg.Wait()
}

func (dc *DailyCatch) GetOrders() {
	dc.Orders = make([]*FBOrder, 0)
	d := dc.Day.AddDate(0, 0, -10)
	// now := now.AddDate(0, 0, -5)
	wg := new(sync.WaitGroup)
	for now.Sub(d).Nanoseconds() > 0 {
		year, month, day := d.Date()
		d0 := time.Date(year, month, day, 0, 0, 0, 0, d.Location())
		wg.Add(1)
		go func() {
			dc.GetOrdersPage(dc.getOrdersURL(d0))
			wg.Done()
		}()
		d = d.AddDate(0, 0, 1)
	}
	wg.Wait()
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

func (d *DailyCatch) StoreOrders() {
	wg := new(sync.WaitGroup)
	for i := 0; i < len(d.Orders); i++ {
		wg.Add(1)
		j := int(i)
		go func() {
			d.Orders[j].Store()
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *FBAdService) Store() {
	s.StoreAds()
	s.StoreInsights()
}
