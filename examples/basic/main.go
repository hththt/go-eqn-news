// 範例：連上 eqn.news 即時新聞串流並印出每則新聞與延遲 Ctrl+C 結束。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	eqnnews "github.com/hththt/go-eqn-news/core"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	tz, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalf("載入時區失敗: %v", err)
	}

	c := eqnnews.New(eqnnews.Config{
		OnConnect:    func() { log.Println("已連線") },
		OnDisconnect: func(err error) { log.Printf("斷線: %v", err) },
		OnError:      func(err error) { log.Printf("錯誤: %v", err) },
	})
	err = c.Run(ctx, func(n eqnnews.News) {
		lag := n.ReceivedAt.Sub(n.Time())
		log.Printf("[%s] %s | %s | 幣:%s | 延遲≈%v | %s",
			n.Time().In(tz).Format("15:04:05"), n.SourceName, n.Title,
			strings.Join(n.Coins, ","), lag.Round(time.Millisecond), n.URL)
	})
	log.Printf("結束: %v", err)
}
