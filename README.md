# binance-chase-trader

Binance USD-M Futures 同向价追单工具。当前版本已重写为 Go：本地服务维护 order window，CLI 提交开仓/查询/取消任务，成交后由本地保护计划监控固定 TP/SL，并在触发时使用同向价追单平仓。

## 核心语义

- 同向价挂单：`BUY` 使用买侧排队，`SELL` 使用卖侧排队。
- 下单继续使用 Binance `priceMatch=QUEUE`，不显式传入 `price`。
- TP/SL 首版只支持固定价格，触发源使用 `markPrice`。
- TP/SL 不使用 Binance 条件单，由本地服务实时监控，触发后创建只平仓追单。
- 首版只支持 Hedge Mode 的 `LONG` / `SHORT`。
- 同一 `symbol + positionSide` 支持多笔本地保护计划，每笔可有不同 TP/SL。
- 同一 `symbol + positionSide` 默认只允许一个开仓追单任务正在 chasing，避免多个同向任务同时撤单重挂放大限频风险。
- 默认 `dry-run`，不会真实下单；实盘必须显式启动 `--env live`。

## 构建

```powershell
go mod tidy
go build -o .\bin\chaser.exe .\cmd\chaser
```

## 配置

复制 `.env.example` 为 `.env`，按需填写：

```powershell
Copy-Item .env.example .env
notepad .env
```

常用环境变量：

| 变量 | 默认值 | 说明 |
|:--|:--|:--|
| `BINANCE_API_KEY` | 空 | testnet/live 模式必填 |
| `BINANCE_SECRET_KEY` | 空 | testnet/live 模式必填 |
| `CHASER_ENV` | `dry-run` | `dry-run` / `testnet` / `live` |
| `BINANCE_DAEMON_HOST` | `127.0.0.1` | 本地控制服务地址 |
| `BINANCE_DAEMON_PORT` | `8765` | 本地控制服务端口 |
| `CHASER_SYMBOLS` | `XAGUSDT,XAUUSDT` | 逗号分隔的订阅交易对 |
| `CHASER_STATE_DIR` | `~/.binance-chase-trader/state` | JSON 快照和 JSONL 事件日志目录 |
| `CHASER_REPLACE_MIN_INTERVAL_MS` | `1000` | 同一订单最小重挂间隔 |
| `CHASER_ORDER_BUDGET_RATIO` | `0.2` | 使用 Binance 订单限频的预算比例 |

## 快速开始

启动本地服务：

```powershell
.\bin\chaser.exe serve --env dry-run --symbols XAGUSDT,XAUUSDT
```

提交一笔做多追单，固定止盈 80、止损 76：

```powershell
.\bin\chaser.exe order XAGUSDT BUY 3 LONG --tp 80 --sl 76
```

查询窗口：

```powershell
.\bin\chaser.exe window XAGUSDT
```

查询任务和保护计划：

```powershell
.\bin\chaser.exe tasks
.\bin\chaser.exe plans
```

取消任务：

```powershell
.\bin\chaser.exe cancel <taskId>
```

## 运行模型

```text
bookTicker + markPrice streams
          |
          v
   order window
   - bid / ask / mid / markPrice
   - open chase tasks
   - local protection plans
   - recent tick history
          |
          +--> chase engine
          |    priceMatch=QUEUE
          |    cancel-replace with rate limit guard
          |
          +--> protection watcher
               fixed TP/SL by markPrice
               trigger -> close-only chase task
```

订单状态由 Binance user data stream 的 `ORDER_TRADE_UPDATE` 驱动；盘口是否仍在同向最优价由 `bookTicker` 驱动；本地服务还会周期性 reconcile 持仓和本地保护计划。

## 多笔本地保护计划

Binance 交易所侧同一个 `symbol + positionSide` 是聚合持仓。本工具在本地维护多笔逻辑保护计划：

```text
XAGUSDT LONG 聚合持仓：10

plan A：3 张，TP=80，SL=76
plan B：2 张，TP=82，SL=77
plan C：5 张，TP=85，SL=75
```

如果 plan B 的 TP 先触发，工具只对 plan B 的 2 张发起只平仓追单。交易所看到的是 `XAGUSDT LONG` 减少 2 张，本地工具将 plan B 标记为关闭。

如果用户在 Binance 前端手工改仓，导致交易所聚合持仓和本地保护数量不一致，服务会进入 `NeedsReconcile` / `NeedsProtection` 类状态，不自动猜 TP/SL。

## 限频保护

服务启动时读取 Binance `exchangeInfo` 中的 `ORDERS` 限频，并按 `CHASER_ORDER_BUDGET_RATIO` 设置本地 token bucket。下单/撤单后还会读取响应头：

- `X-MBX-ORDER-COUNT-10S`
- `X-MBX-ORDER-COUNT-1M`
- `X-MBX-USED-WEIGHT-1M`

当订单计数接近阈值，服务会自动降速；遇到 `429` / `418` 会把交易请求预算降到最低。

## 状态文件

默认状态目录：

```text
~/.binance-chase-trader/state/
```

文件：

- `snapshot.json`：当前窗口、任务、保护计划快照。
- `events.jsonl`：事件日志，用于排障和后续 UI/回放。

## 需求确认稿

需求确认 HTML 保存在：

```text
docs/order-window-go-rewrite-requirements.html
```

## 风险说明

本地 TP/SL 不是 Binance 托管条件单。程序退出、断网、机器休眠时，本地保护不会自动执行。使用 live 模式前必须理解这一点，并先用 dry-run/testnet 验证。

本项目仅供学习和研究。加密货币期货交易具有高风险，可能导致全部资金损失。
