package opentsdbclient

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/bluebreezecf/opentsdb-goclient/client"
	"github.com/bluebreezecf/opentsdb-goclient/config"
)

type OpenTSDBClient struct {
	client.Client
	host string
}

func NewOpenTSDBClient(host string) OpenTSDBClient {
	c, err := client.NewClient(config.OpenTSDBConfig{OpentsdbHost: host})
	if err != nil {
		log.Fatalf(err.Error())
	}
	return OpenTSDBClient{c, host}
}

type UIDMetaData struct {
	UID  string `json:"uid"`
	Type string `json:"type"`
	Name string `json:"name"`
}

type TSMetaData struct {
	TSUID  string        `json:"tsuid"`
	Metric UIDMetaData   `json:"metric"`
	Tags   []UIDMetaData `json:"tags"`
}

type TSMetaDataQueryResult struct {
	Type    string       `json:"type"`
	Query   string       `json:"Query"`
	Results []TSMetaData `json:"results"`
}

type UIDMetaDataQueryResult struct {
	Type    string        `json:"type"`
	Query   string        `json:"Query"`
	Results []UIDMetaData `json:"results"`
}

func (c *OpenTSDBClient) UIDMetaData(query string) []UIDMetaData {
	apiEP := "api/search/uidmeta"
	url := fmt.Sprintf("http://%s/%s?query=%s", c.host, apiEP, query)
	log.Printf("GET %s", url)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	defer resp.Body.Close()
	var result UIDMetaDataQueryResult
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	log.Printf("%v", result)
	return result.Results
}

func (client *OpenTSDBClient) TSMetaData(query string) []TSMetaData {
	apiEP := "api/search/tsmeta"
	url := fmt.Sprintf("http://%s/%s?query=%s", client.host, apiEP, query)
	log.Printf("GET %s", url)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	defer resp.Body.Close()
	var result TSMetaDataQueryResult
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	log.Printf("%v", result)
	return result.Results
}
