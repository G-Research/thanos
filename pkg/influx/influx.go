package influx

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

type Client struct {
	logger  log.Logger
	baseUrl url.URL
}

func NewClient(logger log.Logger, baseUrl url.URL) *Client {
	return &Client{logger: logger, baseUrl: baseUrl}
}

type QueryResult struct {
	Results []struct {
		StatementId int
		Series      []struct {
			Name    string
			Columns []string
			Values  [][]interface{}
		}
	}
}

func (c *Client) Query(ctx context.Context, db string, query string) (*QueryResult, error) {
	u := c.baseUrl
	u.Path = path.Join(u.Path, "/query")

	q := u.Query()
	q.Add("pretty", "false")
	q.Add("db", db)
	q.Add("epoch", "ms")
	q.Add("q", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "request against %s", u.String())
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	bodyContent := buf.String()

	var d QueryResult

	if err := json.Unmarshal([]byte(bodyContent), &d); err != nil {
		level.Debug(c.logger).Log("msg", "Error processing response", "err", err, "body", bodyContent, "path", u.Path, "query", u.RawQuery)
		return nil, errors.Wrap(err, "decode response")
	}

	return &d, nil

}

func (c *Client) AllMetrics(ctx context.Context, influxDatabase string) (*[]string, error) {
	d, err := c.Query(ctx, influxDatabase, "show measurements;")
	if err != nil {
		return nil, err
	}

	if len(d.Results) == 0 || len(d.Results[0].Series) == 0 {
		ret := make([]string, 0)
		return &ret, nil
	}

	ret := make([]string, len(d.Results[0].Series[0].Values))
	results := d.Results[0].Series[0].Values
	for i, metricArray := range results {
		ret[i] = metricArray[0].(string)
	}

	return &ret, nil
}
