// 範例：連上 eqn.news 即時新聞串流並印出每則新聞與延遲 Ctrl+C 或 SIGTERM 結束。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // 內嵌時區資料 精簡容器或未裝 Go 的 Windows 也能載入 Asia/Taipei

	eqnnews "github.com/hththt/go-eqn-news/core"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tz, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalf("載入時區失敗: %v", err)
	}

	news := make(chan eqnnews.News, 1024) // handler 只丟 channel 輸出在另一個 goroutine 避免主控台卡住讀取
	go func() {
		for n := range news {
			// timestamp 只有秒精度 延遲約略到秒
			lag := n.ReceivedAt.Sub(n.Time()).Round(time.Second)
			log.Printf("[%s] %s | %s | 幣:%s | 延遲≈%v | %s",
				n.Time().In(tz).Format("15:04:05"), n.SourceName, n.Title,
				strings.Join(n.Coins, ","), lag, n.URL)
		}
	}()

	c := eqnnews.New(eqnnews.Config{
		OnConnect:    func() { log.Println("已連線") },
		OnDisconnect: func(err error) { log.Printf("斷線: %v", err) },
		OnError:      func(err error) { log.Printf("錯誤: %v", err) },
	})
	err = c.Run(ctx, func(n eqnnews.News) {
		select {
		case news <- n:
		default:
			log.Printf("輸出佇列已滿 丟棄: %s", n.Title)
		}
	})
	log.Printf("結束: %v", err)
}
