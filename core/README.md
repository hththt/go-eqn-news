# core

SDK 主程式 package `eqnnews`。

- `client.go`：`Client`、`Config`、`News` 與連線／心跳／重連邏輯
- `client_test.go`：以本機假伺服器驗證解析、立即 ping、429 Retry-After、pong 過濾、空物件拒收、1012 重連與錯誤回報、心跳、看門狗逾時、退避區間、設定驗證、ctx 取消
