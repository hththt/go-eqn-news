# go-eqn-news

[eqn.news](https://eqn.news/api-docs) 即時新聞的 Go SDK，只實作延遲最低的 **WebSocket** 推送（`wss://api.eqn.news/public/ws`，免金鑰）。

| 目錄 | 說明 |
|---|---|
| `core/` | SDK 主程式（package `eqnnews`） |
| `examples/basic/` | 最小可執行範例 |

## 安裝

推上 GitHub 後：`go get github.com/hththt/go-eqn-news`

推上前，其他專案用 `replace` 指向本機目錄：

```bash
go mod edit -require=github.com/hththt/go-eqn-news@v0.0.0 -replace=github.com/hththt/go-eqn-news=../go-eqn-news
go mod tidy
```

## 使用

```go
import eqnnews "github.com/hththt/go-eqn-news/core"

c := eqnnews.New(eqnnews.Config{
    OnConnect:    func() { log.Println("已連線") },
    OnDisconnect: func(err error) { log.Println("斷線:", err) },
})
err := c.Run(ctx, func(n eqnnews.News) { // ctx 取消才返回
    fmt.Println(n.SourceName, n.Title, n.Coins, n.URL, n.Time())
})
```

- handler 在讀取 goroutine **同步**呼叫（零 channel 跳轉 延遲最低），必須快速返回；耗時工作請自行丟 goroutine，否則伺服器會以 1013 斷線。
- `News.ReceivedAt` 為本機收到時間，`News.Raw` 為原始 JSON。

## 內建行為（依官方文件）

- 每 30 秒送純文字 `ping`，伺服器回 `pong`（SDK 自動略過）。
- 45 秒沒收到任何框視為斷線並重連。
- 指數退避 1→2→4…→30 秒加抖動；1012 服務重啟改 1–5 秒；1008/1013 至少等 5 秒。
- 斷線期間的新聞**不會補發**，需要補齊請另用官方 MCP `list_news`。
- 只推主頻道；交易所公告與行情監控不在 WS 內。

## 測試

```bash
go test -race ./...
go run ./examples/basic   # 實連官方伺服器
```
