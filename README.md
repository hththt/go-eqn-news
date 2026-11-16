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

- handler 與所有回呼都在讀取 goroutine **同步**呼叫（零 channel 跳轉 延遲最低），必須快速返回；耗時工作請自行丟 goroutine，否則伺服器會以 1013 斷線，取消 ctx 也要等 handler 返回。
- `Run` 只在設定不合法（URL 非 ws/wss、`ReadTimeout <= PingInterval`、`PingInterval < 200ms`、`MaxBackoff < 1s`、負值）或 ctx 取消時返回；握手與連線錯誤只經 `OnDisconnect` 回報，**務必設定它**。
- 解析失敗、缺 `news_title` 與 `source_name`、非文字框都不交付 handler，改走 `OnError`。
- `News.ReceivedAt` 為本機收到時間，`News.Raw` 為原始 JSON；`timestamp` 只有秒精度。

## 內建行為（依官方文件）

- 連上立即送一次純文字 `ping`，之後每 30 秒一次，伺服器回 `pong`（SDK 自動略過）。
- 45 秒沒收到任何框視為斷線並重連；單框上限 1 MB。
- 指數退避 1→2→4…→30 秒加抖動；1012 服務重啟改 1–5 秒；1008/1013 至少等 5 秒（不受 `MaxBackoff` 夾限）；握手被 HTTP 拒絕時回 `*HandshakeError`（含狀態碼、內文、`Retry-After`），429 至少等 `Retry-After`。
- 斷線期間的新聞**不會補發**，需要補齊請另用官方 MCP `list_news`。
- 只推主頻道；交易所公告與行情監控不在 WS 內。

## 測試

```bash
go test -race ./...
go run ./examples/basic   # 實連官方伺服器
```
