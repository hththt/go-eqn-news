// Package eqnnews 是 eqn.news 公開 WebSocket 新聞串流的 Go SDK。
//
// 連線 wss://api.eqn.news/public/ws 後伺服器即主動推送主頻道新聞（無需金鑰 無訂閱訊息），
// SDK 負責心跳（每 30 秒送純文字 ping）、斷線偵測與指數退避重連。
//
// 用法：
//
//	c := eqnnews.New(eqnnews.Config{})
//	err := c.Run(ctx, func(n eqnnews.News) { fmt.Println(n.Title) })
package eqnnews

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// DefaultURL 是官方 WebSocket 端點。
const DefaultURL = "wss://api.eqn.news/public/ws"

// News 是伺服器推送的一則新聞（一個 JSON 文字框）。
type News struct {
	SourceName string   `json:"source_name"`    // 來源名稱
	Title      string   `json:"news_title"`     // 標題 多數新聞只有一行即全文
	Coins      []string `json:"coins_included"` // 相關幣種代號 可能為空
	URL        string   `json:"url"`            // 原文連結 伺服器給 null 時為空字串
	Timestamp  int64    `json:"timestamp"`      // 推送時間 Unix 秒 UTC

	ReceivedAt time.Time       `json:"-"` // 本機收到此框的時間 供量測延遲
	Raw        json.RawMessage `json:"-"` // 原始 JSON 未來欄位擴充時可自行解析
}

// Time 回傳推送時間。
func (n News) Time() time.Time { return time.Unix(n.Timestamp, 0) }

// Config 設定 Client 行為 零值欄位使用預設值。
type Config struct {
	URL          string        // 預設 DefaultURL
	PingInterval time.Duration // 心跳間隔 預設 30s（官方規定）
	ReadTimeout  time.Duration // 超過此時間沒收到任何框即判定斷線 預設 45s
	DialTimeout  time.Duration // 握手逾時 預設 10s
	MaxBackoff   time.Duration // 重連退避上限 預設 30s（官方建議）
	Header       http.Header   // 握手額外標頭 可留空

	OnConnect    func()          // 每次連線成功時呼叫
	OnDisconnect func(err error) // 每次斷線時呼叫 err 為斷線原因
	OnError      func(err error) // 非致命錯誤（如單框 JSON 解析失敗）
}

// Client 維持一條自動重連的 WebSocket 連線。
type Client struct {
	cfg    Config
	dialer websocket.Dialer
}

// New 建立 Client。
func New(cfg Config) *Client {
	if cfg.URL == "" {
		cfg.URL = DefaultURL
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 30 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 45 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	return &Client{
		cfg: cfg,
		dialer: websocket.Dialer{
			Proxy:             http.ProxyFromEnvironment,
			HandshakeTimeout:  cfg.DialTimeout,
			EnableCompression: false, // 不壓縮 省去解壓延遲
		},
	}
}

// Handler 處理每則新聞 於讀取 goroutine 中同步呼叫以求最低延遲。
// 必須盡快返回：讀太慢伺服器會以 1013 斷線；耗時工作請自行丟到 channel 或 goroutine。
type Handler func(News)

// Run 連線並持續接收新聞 斷線自動重連 直到 ctx 取消才返回 ctx.Err()。
func (c *Client) Run(ctx context.Context, h Handler) error {
	if h == nil {
		return errors.New("eqnnews: handler 不可為 nil")
	}
	backoff := time.Second
	for {
		gotFrame, err := c.runOnce(ctx, h)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.cfg.OnDisconnect != nil {
			c.cfg.OnDisconnect(err)
		}
		if gotFrame { // 連線曾正常收發 重置退避
			backoff = time.Second
		}
		wait := c.nextWait(err, backoff)
		if !gotFrame {
			backoff = min(backoff*2, c.cfg.MaxBackoff)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// nextWait 依關閉碼決定本次重連等待時間。
func (c *Client) nextWait(err error, backoff time.Duration) time.Duration {
	if ce, ok := errors.AsType[*websocket.CloseError](err); ok {
		switch ce.Code {
		case websocket.CloseServiceRestart: // 1012 服務重啟 1~5 秒後重連
			return time.Second + rand.N(4*time.Second)
		case websocket.ClosePolicyViolation, websocket.CloseTryAgainLater: // 1008 太頻繁 / 1013 連線過多或讀太慢
			backoff = max(backoff, 5*time.Second)
		}
	}
	return min(backoff, c.cfg.MaxBackoff) + rand.N(backoff/4+1)
}

var pongMsg = []byte("pong")

// runOnce 建立單次連線並讀到斷線為止 gotFrame 表示是否收過任何框。
func (c *Client) runOnce(ctx context.Context, h Handler) (gotFrame bool, err error) {
	dctx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	conn, resp, err := c.dialer.DialContext(dctx, c.cfg.URL, c.cfg.Header)
	cancel()
	if err != nil {
		if resp != nil {
			return false, fmt.Errorf("eqnnews: 握手失敗 HTTP %d: %w", resp.StatusCode, err)
		}
		return false, fmt.Errorf("eqnnews: 握手失敗: %w", err)
	}
	if c.cfg.OnConnect != nil {
		c.cfg.OnConnect()
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { // ctx 取消時主動關閉 並負責心跳
		t := time.NewTicker(c.cfg.PingInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
				_ = conn.Close()
				return
			case <-t.C:
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if werr := conn.WriteMessage(websocket.TextMessage, []byte("ping")); werr != nil {
					_ = conn.Close() // 寫失敗 讓讀端立即返回
					return
				}
			}
		}
	})
	defer func() {
		close(done)
		_ = conn.Close()
		wg.Wait()
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
		mt, data, rerr := conn.ReadMessage()
		if rerr != nil {
			return gotFrame, rerr
		}
		now := time.Now()
		gotFrame = true
		if mt != websocket.TextMessage || bytes.Equal(data, pongMsg) {
			continue
		}
		var n News
		if jerr := json.Unmarshal(data, &n); jerr != nil {
			if c.cfg.OnError != nil {
				c.cfg.OnError(fmt.Errorf("eqnnews: 解析失敗 %q: %w", data, jerr))
			}
			continue
		}
		n.ReceivedAt = now
		n.Raw = data // ReadMessage 每次配置新切片 可安全保留
		h(n)
	}
}
