# trueput

輕量、方法透明的網路下載速度測試工具，用 Rust 撰寫。

## 使用

```bash
cargo build --release
./target/release/trueput                 # 預設：Cloudflare，8 條連線，15 秒
./target/release/trueput -b ndt7         # M-Lab ndt7
./target/release/trueput -c 16 -d 20     # 16 條連線、測 20 秒
./target/release/trueput --json          # JSON 輸出（含所有樣本）
./target/release/trueput --no-latency    # 不量延遲
```

輸出範例（數字僅為示意）：

```
下載速度：940.6 Mbps（中位數）
  範圍 P10–P90：806.9 – 962.2 Mbps，最高 967.4 Mbps
  暖機後平均：909.3 Mbps（21 個樣本）
  連線數 4，總時長 6.0s（暖機 2.0s），共收到 684.9 MB

延遲（TCP 交握，104.16.x.x:443）：
  閒置      12.3 ms（最低 11.8，P90 13.0，抖動 0.5 ms）
  下載中    85.4 ms（最低 40.2，P90 120.0，抖動 9.1 ms）
  增加   +73.1 ms → Bufferbloat 等級 C
```

## 測量方法

所有後端共用同一套方法（`src/engine.rs`），結果可以直接互相比較：

1. 開 N 條平行串流（避免單條 TCP 被視窗大小限制），收到資料就計數並丟掉，不寫入硬碟。
2. 每 200ms 取樣一次，用「該段位元組數 ÷ 實際經過時間」計算速度。
3. 前 3 秒視為暖機（TCP 慢啟動），不列入統計。
4. 報告**中位數**、P10–P90 範圍，以及暖機後的整體平均。
5. 數值是應用層 goodput，不含 TCP/IP/TLS 標頭，約比線路速度低 3–6%。

### 延遲與 bufferbloat

- **量法**：對測速伺服器建立 TCP 連線，量三向交握（SYN → SYN-ACK）的時間。只量網路來回時間，不含 TLS 與伺服器處理；DNS 事先解析，不算在內。
- **閒置延遲**：下載前量 10 次，間隔 100ms。
- **負載下延遲**：下載期間每 250ms 量一次（`--latency-interval-ms`），暖機期的量測不列入統計。
- **Bufferbloat 等級**：依「下載中延遲中位數 − 閒置延遲中位數」評分：<5ms A+、<30 A、<60 B、<200 C、<400 D、其餘 F。
- 若閒置量測全部失敗（例如防火牆擋掉額外的連線），會略過負載下量測，下載測速照常進行。
- 量測與下載跑在同一個 runtime 上；CPU 很弱的機器跑高速下載時，排程延遲可能讓延遲數值略微偏高。

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
├── latency.rs         延遲量測（TCP 交握）
├── stats.rs           中位數、百分位數、單位換算
├── report.rs          人類可讀 / JSON 輸出
└── backend/
    ├── mod.rs         Backend trait
    ├── cloudflare.rs
    └── ndt7.rs
```

## 待辦

- [x] 閒置延遲與負載下延遲（bufferbloat）
- [ ] ndt7：用伺服器端回報的 TCPInfo RTT 交叉驗證負載下延遲
- [ ] 動態調整連線數（速度還在上升就加連線）
- [ ] 速度穩定後提前結束
- [ ] 上傳測試
- [ ] 一次測多台伺服器並比較
