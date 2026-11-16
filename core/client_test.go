package eqnnews

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeServer 模擬官方行為：連上即推新聞 收到 ping 回 pong 第一條連線推完後以 1012 關閉。
func fakeServer(t *testing.T, conns *atomic.Int32, pings *atomic.Int32) *httptest.Server {
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
	var conns, pings, news, parseErrs, disc atomic.Int32
	srv := fakeServer(t, &conns, &pings)
	defer srv.Close()

	c := New(Config{
		URL:          "ws" + strings.TrimPrefix(srv.URL, "http"),
		PingInterval: 50 * time.Millisecond,
		OnError:      func(error) { parseErrs.Add(1) },
		OnDisconnect: func(error) { disc.Add(1) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	go func() {
		// 等到第二條連線且收過 pong 後結束
		for ctx.Err() == nil {
			if conns.Load() >= 2 && pings.Load() >= 2 {
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
	if err != context.Canceled {
		t.Fatalf("Run 應回 context.Canceled 得到 %v", err)
	}
	if conns.Load() < 2 {
		t.Fatalf("未重連 conns=%d", conns.Load())
	}
	if news.Load() < 2 || parseErrs.Load() < 2 {
		t.Fatalf("news=%d parseErrs=%d", news.Load(), parseErrs.Load())
	}
	if disc.Load() < 1 {
		t.Fatalf("OnDisconnect 未觸發")
	}
}

func TestNilHandler(t *testing.T) {
	if err := New(Config{}).Run(context.Background(), nil); err == nil {
		t.Fatal("nil handler 應報錯")
	}
}
