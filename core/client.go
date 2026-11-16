// Package eqnnews 是 eqn.news 公開 WebSocket 新聞串流的 Go SDK。
//
// 連線 wss://api.eqn.news/public/ws 後伺服器即主動推送主頻道新聞（無需金鑰 無訂閱訊息），
// SDK 負責心跳（每 30 秒送純文字 ping）、斷線偵測與指數退避重連。
//
// 用法：
//
//	c := eqnnews.New(eqnnews.Config{
//		OnDisconnect: func(err error) { log.Println("斷線:", err) }, // 握手失敗也只從這裡回報
//	})
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
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// DefaultURL 是官方 WebSocket 端點。
const DefaultURL = "wss://api.eqn.news/public/ws"

const (
	minPingInterval = 200 * time.Millisecond // 官方限制客戶端每秒最多 5 則訊息
	minBackoff      = time.Second            // 官方限制每秒最多 2 個新連線
	limitedWait     = 5 * time.Second        // 1008/1013 最少等待
	readLimit       = 1 << 20                // 單框上限 防異常超大框吃光記憶體
)

// News 是伺服器推送的一則新聞（一個 JSON 文字框）。
type News struct {
	SourceName string   `json:"source_name"`    // 來源名稱
	Title      string   `json:"news_title"`     // 標題 多數新聞只有一行即全文
	Coins      []string `json:"coins_included"` // 相關幣種代號 可能為空
	URL        string   `json:"url"`            // 原文連結 伺服器給 null 時為空字串
	Timestamp  int64    `json:"timestamp"`      // 推送時間 Unix 秒 UTC

	ReceivedAt time.Time       `json:"-"` // 本機收到此框的時間 供量測延遲（timestamp 只有秒精度）
	Raw        json.RawMessage `json:"-"` // 原始 JSON 未來欄位擴充時可自行解析
}

// Time 回傳推送時間。
func (n News) Time() time.Time { return time.Unix(n.Timestamp, 0) }

// Config 設定 Client 行為 零值欄位使用預設值 負值或互相矛盾的設定由 Run 回錯。
//
// 所有回呼與 Handler 都在讀取 goroutine 上同步執行 阻塞會拖住讀取。
type Config struct {
	URL          string        // 預設 DefaultURL 必須是 ws:// 或 wss://
	PingInterval time.Duration // 心跳間隔 預設 30s（官方規定） 最小 200ms
	ReadTimeout  time.Duration // 超過此時間沒收到任何資料框即判定斷線 預設 45s 必須大於 PingInterval
	DialTimeout  time.Duration // 握手逾時 預設 10s 握手期間取消 ctx 最久要等這麼久才返回
	MaxBackoff   time.Duration // 重連退避上限 預設 30s（官方建議） 最小 1s
	Header       http.Header   // 握手額外標頭 可留空

	OnConnect    func()          // 每次握手成功時呼叫
	OnDisconnect func(err error) // 每次連線結束或握手失敗時呼叫（ctx 取消導致的結束不呼叫）
	OnError      func(err error) // 非致命錯誤（如單框 JSON 解析失敗或缺必要欄位）
}

// Client 維持一條自動重連的 WebSocket 連線 請用 New 建立。
type Client struct {
	cfg    Config
	dialer websocket.Dialer
}

// New 建立 Client 零值欄位補預設值 設定錯誤延到 Run 回報。
func New(cfg Config) *Client {
	if cfg.URL == "" {
		cfg.URL = DefaultURL
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 30 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 45 * time.Second
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.MaxBackoff == 0 {
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

// validate 檢查設定 不合法直接報錯 不以預設值替代。
func (c *Client) validate() error {
	cfg := c.cfg
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return fmt.Errorf("eqnnews: URL 必須是 ws:// 或 wss://（得到 %q；零值 Client 請改用 New）", cfg.URL)
	}
	switch {
	case cfg.PingInterval < minPingInterval:
		return fmt.Errorf("eqnnews: PingInterval %v 小於 %v（官方每秒最多 5 則訊息）", cfg.PingInterval, minPingInterval)
	case cfg.ReadTimeout <= cfg.PingInterval:
		return fmt.Errorf("eqnnews: ReadTimeout %v 必須大於 PingInterval %v 否則安靜時段會假斷線", cfg.ReadTimeout, cfg.PingInterval)
	case cfg.DialTimeout <= 0:
		return fmt.Errorf("eqnnews: DialTimeout %v 必須為正", cfg.DialTimeout)
	case cfg.MaxBackoff < minBackoff:
		return fmt.Errorf("eqnnews: MaxBackoff %v 小於 %v（官方每秒最多 2 個新連線）", cfg.MaxBackoff, minBackoff)
	}
	return nil
}

// Handler 處理每則新聞 於讀取 goroutine 中同步呼叫以求最低延遲。
// 必須盡快返回：讀太慢伺服器會以 1013 斷線 且取消 ctx 要等 Handler 返回才生效；
// 耗時工作請自行丟到 channel 或 goroutine。Handler panic 會直接穿出 Run。
type Handler func(News)

// Run 連線並持續接收新聞 斷線自動重連 直到 ctx 取消才返回 ctx.Err()。
// 設定不合法時立即返回錯誤；連線與握手錯誤不會讓 Run 返回 只經 OnDisconnect 回報。
func (c *Client) Run(ctx context.Context, h Handler) error {
	if h == nil {
		return errors.New("eqnnews: handler 不可為 nil")
	}
	if err := c.validate(); err != nil {
		return err
	}
	backoff := minBackoff
	for {
		gotFrame, err := c.runOnce(ctx, h)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.cfg.OnDisconnect != nil {
			c.cfg.OnDisconnect(err)
		}
		if gotFrame { // 連線曾正常收發 重置退避
			backoff = minBackoff
		}
		wait := nextWait(err, backoff, c.cfg.MaxBackoff)
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
func nextWait(err error, backoff, maxBackoff time.Duration) time.Duration {
	backoff = min(backoff, maxBackoff)
	wait := backoff + rand.N(backoff/4+1) // 抖動 避免多個客戶端同時重連
	if ce, ok := errors.AsType[*websocket.CloseError](err); ok {
		switch ce.Code {
		case websocket.CloseServiceRestart: // 1012 服務重啟 1~5 秒後重連
			return time.Second + rand.N(4*time.Second)
		case websocket.ClosePolicyViolation, websocket.CloseTryAgainLater: // 1008 太頻繁 / 1013 連線過多或讀太慢
			wait = max(wait, limitedWait+rand.N(limitedWait/4)) // 下限放在上限之後 不被 MaxBackoff 夾掉
		}
	}
	return wait
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
	conn.SetReadLimit(readLimit)

	done := make(chan struct{})
	pingErr := make(chan error, 1) // 心跳寫失敗的原始原因 避免被讀端的 closed connection 蓋掉
	var wg sync.WaitGroup
	defer func() {
		close(done)
		_ = conn.Close()
		wg.Wait()
	}()
	if c.cfg.OnConnect != nil {
		c.cfg.OnConnect()
	}

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
					pingErr <- fmt.Errorf("eqnnews: 心跳寫入失敗: %w", werr)
					_ = conn.Close() // 讓讀端立即返回
					return
				}
			}
		}
	})

	for {
		if ctx.Err() != nil { // 取消後不再把緩衝中的框交給 handler
			return gotFrame, ctx.Err()
		}
		_ = conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
		mt, data, rerr := conn.ReadMessage()
		if rerr != nil {
			select {
			case perr := <-pingErr:
				return gotFrame, errors.Join(perr, rerr)
			default:
				return gotFrame, rerr
			}
		}
		now := time.Now()
		gotFrame = true
		if bytes.Equal(data, pongMsg) {
			continue
		}
		if mt != websocket.TextMessage {
			c.reportError(fmt.Errorf("eqnnews: 收到非文字框 type=%d len=%d", mt, len(data)))
			continue
		}
		var n News
		if jerr := json.Unmarshal(data, &n); jerr != nil {
			c.reportError(fmt.Errorf("eqnnews: 解析失敗 %q: %w", data, jerr))
			continue
		}
		if n.Title == "" && n.SourceName == "" { // {} null 或未知訊息類型 不交付空新聞
			c.reportError(fmt.Errorf("eqnnews: 缺 news_title 與 source_name %q", data))
			continue
		}
		n.ReceivedAt = now
		n.Raw = data // ReadMessage 每次配置新切片 可安全保留
		h(n)
	}
}

func (c *Client) reportError(err error) {
	if c.cfg.OnError != nil {
		c.cfg.OnError(err)
	}
}
