package eqnnews

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsURL(srv *httptest.Server) string { return "ws" + strings.TrimPrefix(srv.URL, "http") }

// fakeServer 模擬官方行為：連上即推 1 則新聞＋1 個壞 JSON＋1 個空物件 收到 ping 回 pong
// 第一條連線推完後以 1012 關閉。
func fakeServer(t *testing.T, conns, pings *atomic.Int32) *httptest.Server {
	up := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		n := conns.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"source_name":"BWENEWS","news_title":"hello","coins_included":["BTC"],"url":null,"timestamp":1792676405}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`not json`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{}`))
		if n == 1 {
			_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseServiceRestart, "restart"))
			return
		}
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			if string(msg) == "ping" {
				pings.Add(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte("pong"))
			}
		}
	}))
}

func TestRunReconnectAndPing(t *testing.T) {
	var conns, pings, news, parseErrs atomic.Int32
	var mu sync.Mutex
	var discErrs []error
	srv := fakeServer(t, &conns, &pings)
	defer srv.Close()

	c := New(Config{
		URL:          wsURL(srv),
		PingInterval: minPingInterval,
		OnError:      func(error) { parseErrs.Add(1) },
		OnDisconnect: func(err error) { mu.Lock(); discErrs = append(discErrs, err); mu.Unlock() },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() { // 第二條連線送過 2 次 ping（伺服器已回 2 次 pong）後結束
		for ctx.Err() == nil {
			if conns.Load() >= 2 && pings.Load() >= 2 {
				time.Sleep(100 * time.Millisecond) // 讓第 2 個 pong 送達客戶端
				cancel()
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	err := c.Run(ctx, func(n News) {
		if n.Title != "hello" || n.SourceName != "BWENEWS" || len(n.Coins) != 1 || n.URL != "" || n.Timestamp != 1792676405 {
			t.Errorf("解析錯誤: %+v", n)
		}
		if n.ReceivedAt.IsZero() || len(n.Raw) == 0 {
			t.Errorf("缺 ReceivedAt/Raw")
		}
		news.Add(1)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run 應回 context.Canceled 得到 %v", err)
	}
	if conns.Load() != 2 {
		t.Fatalf("conns=%d 預期 2", conns.Load())
	}
	// 每條連線 1 則新聞 2 個錯誤（壞 JSON 空物件）；pong 若沒被過濾會讓錯誤數變多
	if news.Load() != 2 || parseErrs.Load() != 4 {
		t.Fatalf("news=%d parseErrs=%d 預期 2/4", news.Load(), parseErrs.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(discErrs) != 1 || !websocket.IsCloseError(discErrs[0], websocket.CloseServiceRestart) {
		t.Fatalf("OnDisconnect 應恰好收到 1012 得到 %v", discErrs)
	}
}

// 伺服器不回 pong 也不推送 看門狗應在 ReadTimeout 後判定斷線。
func TestWatchdog(t *testing.T) {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	var elapsed time.Duration
	var discErr error
	c := New(Config{
		URL:          wsURL(srv),
		PingInterval: minPingInterval,
		ReadTimeout:  300 * time.Millisecond,
		OnDisconnect: func(err error) {
			if discErr == nil {
				discErr, elapsed = err, time.Since(start)
				cancel()
			}
		},
	})
	_ = c.Run(ctx, func(News) {})
	var ne net.Error
	if !errors.As(discErr, &ne) || !ne.Timeout() {
		t.Fatalf("應為逾時錯誤 得到 %v", discErr)
	}
	if elapsed < 300*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("看門狗觸發時間 %v 不合理", elapsed)
	}
}

func TestNextWait(t *testing.T) {
	ce := func(code int) error { return &websocket.CloseError{Code: code} }
	cases := []struct {
		name          string
		err           error
		backoff, maxB time.Duration
		lo, hi        time.Duration
	}{
		{"一般錯誤 1s", errors.New("x"), time.Second, 30 * time.Second, time.Second, 1250 * time.Millisecond},
		{"一般錯誤 4s", errors.New("x"), 4 * time.Second, 30 * time.Second, 4 * time.Second, 5 * time.Second},
		{"上限 30s", errors.New("x"), 60 * time.Second, 30 * time.Second, 30 * time.Second, 37500 * time.Millisecond},
		{"1012", ce(1012), 30 * time.Second, 30 * time.Second, time.Second, 5 * time.Second},
		{"1008 至少 5s", ce(1008), time.Second, 30 * time.Second, 5 * time.Second, 6250 * time.Millisecond},
		{"1013 至少 5s", ce(1013), time.Second, 30 * time.Second, 5 * time.Second, 6250 * time.Millisecond},
		{"1013 不被小上限夾掉", ce(1013), time.Second, 2 * time.Second, 5 * time.Second, 6250 * time.Millisecond},
		{"1013 退避較大時沿用", ce(1013), 16 * time.Second, 30 * time.Second, 16 * time.Second, 20 * time.Second},
	}
	for _, tc := range cases {
		for range 200 {
			w := nextWait(tc.err, tc.backoff, tc.maxB)
			if w < tc.lo || w > tc.hi {
				t.Fatalf("%s: %v 不在 [%v, %v]", tc.name, w, tc.lo, tc.hi)
			}
		}
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]*Client{
		"零值 Client":            {},
		"https scheme":         New(Config{URL: "https://api.eqn.news/public/ws"}),
		"ping 太快":              New(Config{PingInterval: 100 * time.Millisecond}),
		"ReadTimeout<=Ping":    New(Config{PingInterval: time.Minute}),
		"負 DialTimeout":        New(Config{DialTimeout: -1}),
		"MaxBackoff<1s":        New(Config{MaxBackoff: 100 * time.Millisecond}),
		"負 MaxBackoff 不得換成預設值": New(Config{MaxBackoff: -time.Second}),
	}
	for name, c := range cases {
		if err := c.Run(context.Background(), func(News) {}); err == nil {
			t.Errorf("%s: 應報錯", name)
		}
	}
	if err := New(Config{}).validate(); err != nil {
		t.Fatalf("預設設定不應報錯: %v", err)
	}
	if err := New(Config{}).Run(context.Background(), nil); err == nil {
		t.Fatal("nil handler 應報錯")
	}
}
