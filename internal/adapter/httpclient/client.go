package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct{ client *http.Client }

func New() *Client { return &Client{client: &http.Client{Timeout: 15 * time.Second}} }
func (c *Client) Post(url string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	res, e := c.client.Post(url, "application/json", bytes.NewReader(b))
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 2048))
		return fmt.Errorf("%s: HTTP %d: %s", url, res.StatusCode, b)
	}
	return nil
}
