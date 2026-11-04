# trueput

輕量、方法透明的網路下載速度測試工具，用 Rust 撰寫。

## 使用

```bash
cargo build --release
./target/release/trueput                 # 預設：Cloudflare，8 條連線，15 秒
./target/release/trueput -b ndt7         # M-Lab ndt7
./target/release/trueput -c 16 -d 20     # 16 條連線、測 20 秒
./target/release/trueput --json          # JSON 輸出（含所有樣本）
```

## 測量方法

所有後端共用同一套方法（`src/engine.rs`），結果可以直接互相比較：

1. 開 N 條平行串流（避免單條 TCP 被視窗大小限制），收到資料就計數並丟掉，不寫入硬碟。
2. 每 200ms 取樣一次，用「該段位元組數 ÷ 實際經過時間」計算速度。
3. 前 3 秒視為暖機（TCP 慢啟動），不列入統計。
4. 報告**中位數**、P10–P90 範圍，以及暖機後的整體平均。
5. 數值是應用層 goodput，不含 TCP/IP/TLS 標頭，約比線路速度低 3–6%。

## 後端

| 後端 | 協定 | 連線數 | 備註 |
|---|---|---|---|
| `cloudflare` | HTTP/1.1 GET `speed.cloudflare.com/__down` | 可多條 | 非正式 API，可能被修改或限流 |
| `ndt7` | WebSocket，M-Lab 開放協定 | 固定 1 條 | 測試長度約 10 秒，由伺服器決定 |

新增後端只要實作 `src/backend/mod.rs` 裡的 `Backend` trait。

## 專案結構

```
src/
├── main.rs            CLI 參數與流程
├── engine.rs          測速引擎：平行連線、取樣、暖機
├── stats.rs           中位數、百分位數、單位換算
├── report.rs          人類可讀 / JSON 輸出
└── backend/
    ├── mod.rs         Backend trait
    ├── cloudflare.rs
    └── ndt7.rs
```

## 待辦

- [ ] 負載下延遲（bufferbloat）：下載期間同時量 RTT
- [ ] 閒置延遲
- [ ] 動態調整連線數（速度還在上升就加連線）
- [ ] 速度穩定後提前結束
- [ ] 上傳測試
- [ ] 解析 ndt7 伺服器端量測（TCPInfo）做交叉驗證
- [ ] 一次測多台伺服器並比較
