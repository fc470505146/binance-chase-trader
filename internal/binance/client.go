package binance

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fc470505146/binance-chase-trader/internal/config"
	"github.com/fc470505146/binance-chase-trader/internal/domain"
)

type Client struct {
	cfg        config.Config
	httpClient *http.Client
	mu         sync.Mutex
	rateUsage  RateUsage
}

type RateUsage struct {
	OrderCount10s int
	OrderCount1m  int
	UsedWeight1m  int
	RetryAfter    string
	StatusCode    int
}

type ExchangeInfo struct {
	RateLimits []RateLimit  `json:"rateLimits"`
	Symbols    []SymbolInfo `json:"symbols"`
}

type RateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
}

type SymbolInfo struct {
	Symbol            string   `json:"symbol"`
	PricePrecision    int      `json:"pricePrecision"`
	QuantityPrecision int      `json:"quantityPrecision"`
	OrderTypes        []string `json:"orderTypes"`
	TimeInForce       []string `json:"timeInForce"`
}

type OrderResponse struct {
	Symbol        string `json:"symbol"`
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Status        string `json:"status"`
}

type OpenOrder struct {
	Symbol        string `json:"symbol"`
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Price         string `json:"price"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	Status        string `json:"status"`
}

type PositionRisk struct {
	Symbol        string `json:"symbol"`
	PositionAmt   string `json:"positionAmt"`
	EntryPrice    string `json:"entryPrice"`
	MarkPrice     string `json:"markPrice"`
	PositionSide  string `json:"positionSide"`
	UnRealizedPNL string `json:"unRealizedProfit"`
}

func NewClient(cfg config.Config) *Client {
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) IsDryRun() bool {
	return c.cfg.IsDryRun()
}

func (c *Client) LastRateUsage() RateUsage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rateUsage
}

func (c *Client) ExchangeInfo(ctx context.Context) (ExchangeInfo, error) {
	var out ExchangeInfo
	err := c.public(ctx, http.MethodGet, "/fapi/v1/exchangeInfo", nil, &out)
	return out, err
}

func (c *Client) PlaceQueueLimit(ctx context.Context, task *domain.ChaseTask) (OrderResponse, error) {
	if c.cfg.IsDryRun() {
		return OrderResponse{
			Symbol:        task.Symbol,
			OrderID:       time.Now().UnixNano(),
			ClientOrderID: task.ClientOrderID,
			Status:        "DRY_RUN",
		}, nil
	}

	params := url.Values{}
	params.Set("symbol", task.Symbol)
	params.Set("side", string(task.Side))
	params.Set("type", "LIMIT")
	params.Set("quantity", formatFloat(task.RemainingQty))
	params.Set("positionSide", string(task.PositionSide))
	params.Set("priceMatch", "QUEUE")
	params.Set("timeInForce", "GTC")
	if task.ClientOrderID != "" {
		params.Set("newClientOrderId", task.ClientOrderID)
	}

	var out OrderResponse
	err := c.signed(ctx, http.MethodPost, "/fapi/v1/order", params, &out)
	return out, err
}

func (c *Client) CancelOrder(ctx context.Context, symbol string, orderID int64) error {
	if c.cfg.IsDryRun() {
		return nil
	}
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("orderId", strconv.FormatInt(orderID, 10))
	var out map[string]any
	return c.signed(ctx, http.MethodDelete, "/fapi/v1/order", params, &out)
}

func (c *Client) OpenOrders(ctx context.Context, symbol string) ([]OpenOrder, error) {
	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", strings.ToUpper(symbol))
	}
	var out []OpenOrder
	err := c.signed(ctx, http.MethodGet, "/fapi/v1/openOrders", params, &out)
	return out, err
}

func (c *Client) Positions(ctx context.Context, symbol string) ([]PositionRisk, error) {
	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", strings.ToUpper(symbol))
	}
	var out []PositionRisk
	err := c.signed(ctx, http.MethodGet, "/fapi/v2/positionRisk", params, &out)
	return out, err
}

func (c *Client) StartListenKey(ctx context.Context) (string, error) {
	if c.cfg.APIKey == "" {
		return "", fmt.Errorf("BINANCE_API_KEY 未配置")
	}
	var out struct {
		ListenKey string `json:"listenKey"`
	}
	err := c.apiKeyOnly(ctx, http.MethodPost, "/fapi/v1/listenKey", nil, &out)
	return out.ListenKey, err
}

func (c *Client) KeepaliveListenKey(ctx context.Context, listenKey string) error {
	params := url.Values{}
	params.Set("listenKey", listenKey)
	var out map[string]any
	return c.apiKeyOnly(ctx, http.MethodPut, "/fapi/v1/listenKey", params, &out)
}

func (c *Client) public(ctx context.Context, method string, path string, params url.Values, out any) error {
	return c.do(ctx, method, path, params, false, false, out)
}

func (c *Client) apiKeyOnly(ctx context.Context, method string, path string, params url.Values, out any) error {
	return c.do(ctx, method, path, params, true, false, out)
}

func (c *Client) signed(ctx context.Context, method string, path string, params url.Values, out any) error {
	if c.cfg.APIKey == "" || c.cfg.SecretKey == "" {
		return fmt.Errorf("BINANCE_API_KEY/BINANCE_SECRET_KEY 未配置")
	}
	if params == nil {
		params = url.Values{}
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", "5000")
	qs := encodeValues(params)
	sig := sign(qs, c.cfg.SecretKey)
	params.Set("signature", sig)
	return c.do(ctx, method, path, params, true, true, out)
}

func (c *Client) do(ctx context.Context, method string, path string, params url.Values, apiKey bool, signed bool, out any) error {
	if params == nil {
		params = url.Values{}
	}
	u := c.cfg.RESTBaseURL + path
	body := io.Reader(nil)
	if method == http.MethodGet || method == http.MethodDelete {
		if len(params) > 0 {
			u += "?" + encodeValues(params)
		}
	} else {
		if len(params) > 0 {
			u += "?" + encodeValues(params)
		}
		body = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	if apiKey || signed {
		req.Header.Set("X-MBX-APIKEY", c.cfg.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c.captureRateUsage(resp)

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("Binance API %d %s: %s", resp.StatusCode, path, strings.TrimSpace(string(data)))
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (c *Client) captureRateUsage(resp *http.Response) {
	usage := RateUsage{
		OrderCount10s: headerInt(resp.Header, "X-MBX-ORDER-COUNT-10S"),
		OrderCount1m:  headerInt(resp.Header, "X-MBX-ORDER-COUNT-1M"),
		UsedWeight1m:  headerInt(resp.Header, "X-MBX-USED-WEIGHT-1M"),
		RetryAfter:    resp.Header.Get("Retry-After"),
		StatusCode:    resp.StatusCode,
	}
	c.mu.Lock()
	c.rateUsage = usage
	c.mu.Unlock()
}

func headerInt(header http.Header, name string) int {
	raw := strings.TrimSpace(header.Get(name))
	if raw == "" {
		return 0
	}
	v, _ := strconv.Atoi(raw)
	return v
}

func encodeValues(values url.Values) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		for _, val := range values[key] {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(val))
		}
	}
	return strings.Join(parts, "&")
}

func sign(query string, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
