# 🍗 binance-chase-trader

**Binance USDⓈ-M Futures 最优价格下单工具**

QUEUE 限价 + 实时追单 + 自动 ATR 止盈止损。纯 Python 标准库，零外部依赖。

## 工作原理

```
┌─────────────────────────────────┐
│   ws-data-daemon (常驻后台)     │
│  ─────────────────────────────  │
│  🔗 Binance WS @bookTicker      │
│  📦 内存：最新盘口 + 滑动窗口    │
│  🏠 Unix socket 对外提供查询     │
└────────────┬────────────────────┘
             │ DEPTH <sym> → b1/a1
             ▼
┌─────────────────────────────────┐
│   chase_order.py (按需调用)      │
│  ─────────────────────────────  │
│  1️⃣ socket 查当前排队价          │
│  2️⃣ REST 挂 QUEUE 限价单 (maker) │
│  3️⃣ 每 0.5s socket 查价 → 变了   │
│     就撤单重挂                   │
│  4️⃣ 成交 → ATR 止盈止损          │
└─────────────────────────────────┘
```

**为什么用 QUEUE 限价？**
- Maker 费率 0.02% vs Taker 0.04%，省一半手续费
- `priceMatch: "QUEUE"` 自动挂在同向最优价
- 5 分钟 K 线震荡期内大概率成交

## 安装

### 1. 克隆

```bash
git clone https://github.com/YOUR_USERNAME/binance-chase-trader.git
cd binance-chase-trader
```

### 2. 配置 API Key

```bash
cp .env.example .env
# 编辑 .env，填入你的 Binance API Key
```

**安全提醒：** `.env` 已被 `.gitignore` 排除，永远不会提交到 Git。

### 3. 安装

```bash
pip install -e .
```

## 快速开始

### 启动数据守护进程

```bash
# 前台启动（调试用）
python -m binance_chase daemon

# 后台 systemd 服务
sudo bash scripts/install_service.sh
sudo systemctl start binance-chase-daemon
```

### 查守护进程状态

```bash
python -m binance_chase status
# {
#   "symbols": {
#     "XAGUSDT": {"bid": 78.29, "ask": 78.30, "connected": true, ...},
#     "XAUUSDT": {"bid": 4555.10, "ask": 4555.11, ...}
#   },
#   "uptime": 1234.56
# }
```

### 查实时盘口

```bash
python -m binance_chase depth XAGUSDT
# XAGUSDT: bid=78.29 ask=78.30
```

### 开仓（QUEUE限价 + 追单 + 自动止盈止损）

```bash
# 做空 XAGUSDT，1张，SHORT
python -m binance_chase trade XAGUSDT SELL 1 SHORT

# 做多 XAUUSDT，0.079张，LONG
python -m binance_chase trade XAUUSDT BUY 0.079 LONG
```

### 平仓

```bash
python -m binance_chase trade XAGUSDT SELL 1 LONG --close
# 平多仓：自动清除存量条件单，成交即结束
```

### 查持仓

```bash
python -m binance_chase pos
# XAGUSDT SHORT: 1张 @78.50 标记价78.30 盈亏+0.20 USDT

python -m binance_chase pos XAGUSDT  # 只看特定品种
```

## Python API

```python
from binance_chase import daemon, trader, api

# 启动守护进程
daemon.run(symbols=["XAGUSDT", "XAUUSDT"])

# 开仓
result = trader.place_limit(
    symbol="XAGUSDT",
    side="SELL",
    qty=1,
    pos_side="SHORT",
    verbose=True,
)

# 平仓
result = trader.place_limit(
    symbol="XAGUSDT",
    side="SELL",
    qty=1,
    pos_side="LONG",
    close=True,
)

# 查持仓
pos = api.has_position("XAGUSDT")
if pos:
    print(f"{pos['side']} {pos['amt']}张 @${pos['entry']} 盈亏{pos['pnl']}")

# 手动查盘口
bid, ask = trader.get_book_top_ws("XAGUSDT")
```

## 配置

| 变量 | 默认值 | 说明 |
|:----|:------|:-----|
| `BINANCE_API_KEY` | — | Binance API Key (必填) |
| `BINANCE_SECRET_KEY` | — | Binance Secret Key (必填) |
| `BINANCE_SOCKET_PATH` | `/tmp/ws_data.sock` | Unix socket 路径 |
| 交易对 | `XAGUSDT`, `XAUUSDT` | 可在 `daemon.run(symbols=[...])` 中自定义 |

**`.env` 查找优先级：** 当前目录 → `~/.binance-chase/.env` → `~/.hermes/.env`

## 项目结构

```
binance-chase-trader/
├── src/binance_chase/
│   ├── __init__.py       # 包信息
│   ├── __main__.py       # CLI 入口
│   ├── config.py          # .env 读取 + 配置
│   ├── api.py             # Binance API 封装 (签名/公开)
│   ├── daemon.py          # WS 数据守护进程
│   └── trader.py          # QUEUE 限价追单 + 止盈止损
├── scripts/
│   └── install_service.sh # systemd 服务安装
├── .env.example           # 配置模板
├── .gitignore
├── pyproject.toml
├── requirements.txt
├── LICENSE
└── README.md
```

## 依赖

**零外部依赖。** 只用 Python 标准库：
- `asyncio` — WebSocket + Unix socket 服务
- `hmac` / `hashlib` — Binance API 签名
- `urllib` — REST API 请求
- `json` / `socket` / `struct` — 数据序列化与传输

## 免责声明

本项目仅供学习和研究。加密货币期货交易具有高风险，可能导致全部资金损失。使用前请充分了解风险。
