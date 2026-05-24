package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fc470505146/binance-chase-trader/internal/config"
	"github.com/fc470505146/binance-chase-trader/internal/domain"
	"github.com/gorilla/websocket"
)

type MarketHandler func(domain.MarketSnapshot)
type OrderHandler func(domain.OrderUpdate)

type Streams struct {
	cfg    config.Config
	client *Client
}

func NewStreams(cfg config.Config, client *Client) *Streams {
	return &Streams{cfg: cfg, client: client}
}

func (s *Streams) RunMarket(ctx context.Context, symbols []string, handler MarketHandler) {
	go s.runMarket(ctx, symbols, handler)
}

func (s *Streams) RunUser(ctx context.Context, handler OrderHandler) {
	if s.cfg.IsDryRun() {
		return
	}
	go s.runUser(ctx, handler)
}

func (s *Streams) runMarket(ctx context.Context, symbols []string, handler MarketHandler) {
	streams := make([]string, 0, len(symbols)*2)
	for _, symbol := range symbols {
		lower := strings.ToLower(symbol)
		streams = append(streams, lower+"@bookTicker")
		streams = append(streams, lower+"@markPrice@1s")
	}
	endpoint := s.cfg.WSBaseURL + "/stream?streams=" + url.QueryEscape(strings.Join(streams, "/"))

	for ctx.Err() == nil {
		conn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
		if err != nil {
			log.Printf("market stream 连接失败: %v", err)
			sleepCtx(ctx, 5*time.Second)
			continue
		}
		log.Printf("market stream 已连接: %s", strings.Join(symbols, ","))
		s.readMarket(ctx, conn, handler)
		_ = conn.Close()
		sleepCtx(ctx, 2*time.Second)
	}
}

func (s *Streams) readMarket(ctx context.Context, conn *websocket.Conn, handler MarketHandler) {
	for ctx.Err() == nil {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("market stream 断开: %v", err)
			return
		}
		var msg combinedMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.Data == nil {
			continue
		}
		var base struct {
			Event string `json:"e"`
		}
		if err := json.Unmarshal(msg.Data, &base); err != nil {
			continue
		}
		switch base.Event {
		case "bookTicker":
			var bt bookTickerMessage
			if json.Unmarshal(msg.Data, &bt) == nil {
				handler(bt.snapshot())
			}
		case "markPriceUpdate":
			var mp markPriceMessage
			if json.Unmarshal(msg.Data, &mp) == nil {
				handler(mp.snapshot())
			}
		}
	}
}

func (s *Streams) runUser(ctx context.Context, handler OrderHandler) {
	for ctx.Err() == nil {
		listenKey, err := s.client.StartListenKey(ctx)
		if err != nil {
			log.Printf("user stream listenKey 创建失败: %v", err)
			sleepCtx(ctx, 10*time.Second)
			continue
		}
		keepCtx, cancelKeepalive := context.WithCancel(ctx)
		go s.keepalive(keepCtx, listenKey)

		endpoint := fmt.Sprintf("%s/ws/%s", s.cfg.WSBaseURL, listenKey)
		conn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
		if err != nil {
			cancelKeepalive()
			log.Printf("user stream 连接失败: %v", err)
			sleepCtx(ctx, 5*time.Second)
			continue
		}
		log.Printf("user stream 已连接")
		s.readUser(ctx, conn, handler)
		cancelKeepalive()
		_ = conn.Close()
		sleepCtx(ctx, 2*time.Second)
	}
}

func (s *Streams) keepalive(ctx context.Context, listenKey string) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.client.KeepaliveListenKey(ctx, listenKey); err != nil {
				log.Printf("listenKey keepalive 失败: %v", err)
			}
		}
	}
}

func (s *Streams) readUser(ctx context.Context, conn *websocket.Conn, handler OrderHandler) {
	for ctx.Err() == nil {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("user stream 断开: %v", err)
			return
		}
		var msg userEvent
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.Event != "ORDER_TRADE_UPDATE" {
			continue
		}
		handler(msg.toUpdate())
	}
}

type combinedMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type bookTickerMessage struct {
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Bid       string `json:"b"`
	Ask       string `json:"a"`
}

func (m bookTickerMessage) snapshot() domain.MarketSnapshot {
	bid, _ := strconv.ParseFloat(m.Bid, 64)
	ask, _ := strconv.ParseFloat(m.Ask, 64)
	return domain.MarketSnapshot{
		Symbol:    strings.ToUpper(m.Symbol),
		Bid:       bid,
		Ask:       ask,
		Mid:       (bid + ask) / 2,
		EventTime: millis(m.EventTime),
		UpdatedAt: time.Now(),
	}
}

type markPriceMessage struct {
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
}

func (m markPriceMessage) snapshot() domain.MarketSnapshot {
	price, _ := strconv.ParseFloat(m.Price, 64)
	return domain.MarketSnapshot{
		Symbol:    strings.ToUpper(m.Symbol),
		MarkPrice: price,
		EventTime: millis(m.EventTime),
		UpdatedAt: time.Now(),
	}
}

type userEvent struct {
	Event     string `json:"e"`
	EventTime int64  `json:"E"`
	Order     struct {
		Symbol        string `json:"s"`
		ClientOrderID string `json:"c"`
		Side          string `json:"S"`
		OrderType     string `json:"o"`
		TimeInForce   string `json:"f"`
		OrigQty       string `json:"q"`
		Price         string `json:"p"`
		AvgPrice      string `json:"ap"`
		StopPrice     string `json:"sp"`
		ExecutionType string `json:"x"`
		Status        string `json:"X"`
		OrderID       int64  `json:"i"`
		LastQty       string `json:"l"`
		FilledQty     string `json:"z"`
		PositionSide  string `json:"ps"`
	} `json:"o"`
}

func (e userEvent) toUpdate() domain.OrderUpdate {
	o := e.Order
	origQty, _ := strconv.ParseFloat(o.OrigQty, 64)
	filledQty, _ := strconv.ParseFloat(o.FilledQty, 64)
	lastQty, _ := strconv.ParseFloat(o.LastQty, 64)
	price, _ := strconv.ParseFloat(o.Price, 64)
	avgPrice, _ := strconv.ParseFloat(o.AvgPrice, 64)
	return domain.OrderUpdate{
		Symbol:        strings.ToUpper(o.Symbol),
		ClientOrderID: o.ClientOrderID,
		OrderID:       o.OrderID,
		Side:          domain.Side(o.Side),
		PositionSide:  domain.PositionSide(o.PositionSide),
		Status:        o.Status,
		ExecutionType: o.ExecutionType,
		OrigQty:       origQty,
		FilledQty:     filledQty,
		LastQty:       lastQty,
		Price:         price,
		AvgPrice:      avgPrice,
		EventTime:     millis(e.EventTime),
	}
}

func millis(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
