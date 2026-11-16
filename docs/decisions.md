# 決策紀錄

日期 | 決策 | 原因 | 否決的替代方案
---|---|---|---
2026-11-16 | 只實作 WebSocket 推送 | 官方三種接入中唯一即時推送 延遲最低 | MCP（輪詢）、RSS（60 秒快取）
2026-11-16 | 用 gorilla/websocket | 成熟 低開銷 對讀寫 deadline 控制直接 | coder/websocket、自寫 RFC6455
2026-11-16 | handler 在讀取 goroutine 同步呼叫 | 省一次 channel 跳轉 延遲最低 | 回傳 buffered channel（多一次排程 且滿時需丟棄策略）
2026-11-16 | module 路徑 github.com/hththt/go-eqn-news | 無點路徑被 Go 視為標準庫 外部專案無法引用 | go-eqn-news、虛擬網域
