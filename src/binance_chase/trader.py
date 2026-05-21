"""
🍗 QUEUE限价追单工具
- 通过 ws_data_daemon Unix socket 查实时排队价
- 价格变动时撤单重挂（追单）
- 成交后自动补 ATR 动态止盈止损（R:R ≥ 1:2）
"""

import json
import socket
import time

from . import api, config


# ─── WS Data Daemon 查询 ───

def _ws_query(cmd: str, arg: str = ""):
    """通过 Unix socket 查 daemon"""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(config.SOCKET_PATH)
        s.sendall(f"{cmd} {arg}".strip().encode())
        resp = s.recv(65536).decode().strip()
        s.close()
        return json.loads(resp) if resp else None
    except (socket.error, json.JSONDecodeError, ConnectionRefusedError):
        return None


def get_book_top_ws(symbol: str) -> tuple[float | None, float | None]:
    """从 daemon 获取实时 b1/a1，失败时 fallback 到 REST"""
    data = _ws_query("DEPTH", symbol)
    if data and "bid" in data and "ask" in data:
        return data["bid"], data["ask"]
    return api.get_book_top(symbol)


# ─── 核心追单函数 ───

def place_limit(
    symbol: str,
    side: str,
    qty: float,
    pos_side: str,
    atr_mult_sl: float = 1.5,
    atr_mult_tp: float = 3.0,
    chase_interval: float = 0.5,
    max_wait: int = 600,
    close: bool = False,
    verbose: bool = True,
) -> dict:
    """
    QUEUE限价下单 + 实时追单 + 自动止盈止损

    参数:
        symbol:     交易对 (XAGUSDT / XAUUSDT)
        side:       BUY / SELL
        qty:        数量
        pos_side:   LONG / SHORT (HEDGE模式必传)
        atr_mult_sl: 止损 ATR 倍数 (默认 1.5)
        atr_mult_tp: 止盈 ATR 倍数 (默认 3.0 → R:R=1:2)
        chase_interval: 追单检查间隔 (秒)
        max_wait:     最长等待时间 (秒, 默认 10分钟)
        close:       平仓模式 — 成交后不挂止盈止损
        verbose:     是否打印进度

    返回:
        {"status": "filled"|"cancelled"|"timeout",
         "order_id": int,
         "fill_price": float | None,
         "sl": float, "tp": float, "rr": float}  # 仅开仓模式
    """
    log = print if verbose else lambda *a: None

    # 1) 挂单
    log(f"📋 {symbol} {side} {qty}张 {pos_side} | QUEUE限价")
    r = api.place_limit_queue(symbol, side, qty, pos_side)
    order_id = r["orderId"]
    log(f"   #{order_id} 已挂")

    # 2) 实时追单
    start = time.time()
    last_queue = None
    filled = False

    while time.time() - start < max_wait:
        try:
            orders = api.get_open_orders(symbol)
            my_order = next(
                (o for o in orders if str(o["orderId"]) == str(order_id)),
                None,
            )
        except Exception:
            my_order = None

        if my_order is None:
            filled = True
            break

        exec_qty = float(my_order["executedQty"])
        if exec_qty >= float(my_order["origQty"]):
            filled = True
            break

        # 从 daemon 查当前排队价
        bid, ask = get_book_top_ws(symbol)
        if bid is None:
            time.sleep(chase_interval)
            continue

        queue_price = ask if side == "BUY" else bid
        order_price = float(my_order["price"])

        if last_queue is None:
            last_queue = queue_price
        elif abs(queue_price - last_queue) >= 0.01:
            last_queue = queue_price
            log(f"  🔄 追单 ${order_price:.2f} → ${queue_price:.2f}")
            try:
                api.cancel_order(symbol, order_id)
                time.sleep(0.05)
                remaining = qty - exec_qty if exec_qty > 0 else qty
                r2 = api.place_limit_queue(symbol, side, remaining, pos_side)
                order_id = r2["orderId"]
                log(f"   #{order_id} 重挂")
            except Exception as e:
                log(f"  ⚠️ 追单失败: {e}")

        time.sleep(chase_interval)

    # 3) 超时撤单
    if not filled:
        try:
            api.cancel_order(symbol, order_id)
            log(f"⏰ 超时 {max_wait}s，撤单 #{order_id}")
        except Exception:
            pass
        return {"status": "timeout", "order_id": order_id}

    # 4) 平仓模式 — 清条件单，不挂新单
    if close:
        try:
            existing = api.get_open_algos(symbol)
            for a in existing:
                api.cancel_algo(a["algoId"])
                log(f"  🗑️ 撤条件单 #{a['algoId']}")
        except Exception:
            pass
        log(f"✅ 平仓成交")
        return {"status": "filled", "order_id": order_id, "fill_price": None}

    # 5) 开仓模式 — 查持仓 → 补止盈止损
    pos = api.has_position(symbol, pos_side)
    if not pos:
        log("⚠️ 成交后查不到持仓")
        return {"status": "filled", "order_id": order_id, "fill_price": None}

    entry = pos["entry"]
    log(f"✅ 成交 {symbol} {pos['side']} {pos['amt']}张 @ ${entry:.2f}")

    atr = api.calc_atr(symbol)
    if atr <= 0:
        log("⚠️ 无法计算 ATR，跳过止盈止损")
        return {"status": "filled", "order_id": order_id, "fill_price": entry}

    # 计算 SL / TP
    decimals = 3 if "XAG" in symbol else 1

    if pos["side"] == "LONG":
        sl = round(entry - atr * atr_mult_sl, decimals)
        tp = round(entry + atr * atr_mult_tp, decimals)
        sl_side = tp_side = "SELL"
    else:
        sl = round(entry + atr * atr_mult_sl, decimals)
        tp = round(entry - atr * atr_mult_tp, decimals)
        sl_side = tp_side = "BUY"

    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = reward / risk if risk > 0 else 0

    log(f"  📍 止损 ${sl} | 止盈 ${tp} | R:R 1:{rr:.1f}")

    try:
        # TP 用 closePosition, SL 用 quantity 防 -4130
        api.place_algo(symbol, tp_side, pos["side"],
                       "TAKE_PROFIT_MARKET", tp)
        api.place_algo(symbol, sl_side, pos["side"],
                       "STOP_MARKET", sl, qty=pos["amt"])
        log("  ✅ 止盈止损已挂")
    except Exception as e:
        log(f"  ⚠️ 止盈止损失败: {e}")

    return {
        "status": "filled",
        "order_id": order_id,
        "fill_price": entry,
        "sl": sl,
        "tp": tp,
        "rr": round(rr, 1),
    }
