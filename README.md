# trueput

輕量、方法透明的網路下載速度測試工具，用 Rust 撰寫。

## 使用

```bash
cargo build --release
./target/release/trueput                 # 預設：Cloudflare，自動調整連線數，15 秒
./target/release/trueput -b ndt7         # M-Lab ndt7
./target/release/trueput -c 4 -d 20      # 固定 4 條連線、測 20 秒
./target/release/trueput --max-connections 32  # 自動模式最多開到 32 條
./target/release/trueput --json          # JSON 輸出（含所有樣本）
./target/release/trueput --no-latency    # 不量延遲
```

輸出範例（數字僅為示意）：

```
下載速度：940.6 Mbps（中位數）
  範圍 P10–P90：806.9 – 962.2 Mbps，最高 967.4 Mbps
  暖機後平均：909.3 Mbps（21 個樣本）
  連線數 8（自動：2 → 4 → 8），總時長 15.0s（暖機 3.0s），共收到 1705.2 MB

延遲（TCP 交握，104.16.x.x:443）：
  閒置      12.3 ms（最低 11.8，P90 13.0，抖動 0.5 ms）
  下載中    85.4 ms（最低 40.2，P90 120.0，抖動 9.1 ms）
  增加   +73.1 ms → Bufferbloat 等級 C
```

## 測量方法

所有後端共用同一套方法（`src/engine.rs`），結果可以直接互相比較：

1. 開多條平行串流（避免單條 TCP 被視窗大小限制），收到資料就計數並丟掉，不寫入硬碟。連線數預設自動調整，見下方。
2. 每 200ms 取樣一次，用「該段位元組數 ÷ 實際經過時間」計算速度。
3. 暖機期間（TCP 慢啟動、連線數調整中）的樣本不列入統計。暖機要「至少 3 秒」**而且**「連線數已鎖定」才結束。
4. 報告**中位數**、P10–P90 範圍，以及暖機後的整體平均。
5. 數值是應用層 goodput，不含 TCP/IP/TLS 標頭，約比線路速度低 3–6%。

### 動態調整連線數

單一條連線常被 TCP 視窗、伺服器限速或路徑設備卡住，跑不滿頻寬；但連線開太多也是浪費。做法（`src/autoscale.rs`）：

1. 從 2 條開始，每 1 秒評估一次。
2. 第一次評估直接把連線數加倍試試看；之後只要速度比上一秒高 **10% 以上**，就繼續加倍（2 → 4 → 8 → 16）。
3. 速度沒有明顯成長，或到達上限（`--max-connections`，預設 16），就鎖定連線數。
4. 測試時間過半還沒鎖定，就強制鎖定，確保有足夠的正式樣本。

用 `-c <N>` 可以改成固定連線數；ndt7 依規範固定 1 條。

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
├── autoscale.rs       動態調整連線數
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
- [x] 動態調整連線數
- [ ] 依閒置延遲調整評估窗格（高延遲路徑的慢啟動較久，1 秒可能不夠）
- [ ] 速度穩定後提前結束
- [ ] 上傳測試
- [ ] 一次測多台伺服器並比較
