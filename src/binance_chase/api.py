"""Binance USDⓈ-M Futures API 封装
签名请求 + 公开请求 + ATR计算
"""

import hmac
import hashlib
import json
import time
import urllib.parse
import urllib.request

from . import config


# ─── HMAC 签名 ───
def _ts() -> str:
    return str(int(time.time() * 1000))


def _sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


# ─── 网络请求 ───
def _req(method: str, path: str, params: dict) -> dict:
    """带签名的 fapi 请求"""
    params["timestamp"] = _ts()
    params["recvWindow"] = "5000"
    qs = urllib.parse.urlencode(sorted(params.items()))
    sig = _sign(qs, config.SECRET_KEY)
    data = b"" if method in ("POST", "DELETE") else None
    url = f"{config.FAPI_BASE}{path}?{qs}&signature={sig}"
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("X-MBX-APIKEY", config.API_KEY)
    try:
        return json.loads(urllib.request.urlopen(r, timeout=5).read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise Exception(f"Binance API {e.code} {path}: {body}")


def _public(path: str, params: dict = None) -> dict:
    """无签名公开请求"""
    qs = urllib.parse.urlencode(sorted((params or {}).items()))
    url = f"{config.FAPI_BASE}{path}?{qs}"
    return json.loads(urllib.request.urlopen(url, timeout=5).read())


# ─── 账户 & 持仓 ───
def get_account() -> dict:
    return _req("GET", "/fapi/v2/account", {})


def get_position_risk(symbol: str = None) -> list:
    params = {}
    if symbol:
        params["symbol"] = symbol
    return _req("GET", "/fapi/v2/positionRisk", params)


def has_position(symbol: str, side: str = None) -> dict | None:
    """查指定品种是否有持仓，返回持仓详情或 None"""
    pos = get_position_risk(symbol)
    for p in pos:
        amt = float(p["positionAmt"])
        if amt != 0 and (not side or p["positionSide"] == side):
            return {
                "amt": abs(amt),
                "side": p["positionSide"],
                "entry": float(p["entryPrice"]),
                "mark": float(p["markPrice"]),
                "pnl": float(p["unRealizedProfit"]),
            }
    return None


# ─── 订单 ───
def place_limit_queue(symbol: str, side: str, qty: float, pos_side: str) -> dict:
    """QUEUE限价单，享受 maker 费率"""
    return _req("POST", "/fapi/v1/order", {
        "symbol": symbol,
        "side": side,
        "type": "LIMIT",
        "quantity": str(qty),
        "positionSide": pos_side,
        "priceMatch": "QUEUE",
        "timeInForce": "GTC",
    })


def cancel_order(symbol: str, order_id: int | str) -> dict:
    return _req("DELETE", "/fapi/v1/order", {
        "symbol": symbol,
        "orderId": str(order_id),
    })


def get_open_orders(symbol: str = None) -> list:
    params = {}
    if symbol:
        params["symbol"] = symbol
    return _req("GET", "/fapi/v1/openOrders", params)


# ─── Algo Order (止盈止损) ───
def place_algo(symbol: str, side: str, pos_side: str,
               order_type: str, trigger_price: float,
               qty: float = None) -> dict:
    """创建条件单 (止盈/止损)
    
    order_type: "TAKE_PROFIT_MARKET" | "STOP_MARKET"
    止盈用 closePosition=true
    止损用 quantity 模式避免 -4130 冲突
    """
    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "triggerPrice": str(trigger_price),
        "workingType": "MARK_PRICE",
        "positionSide": pos_side,
    }
    if order_type == "TAKE_PROFIT_MARKET":
        params["closePosition"] = "true"
    elif qty is not None:
        params["quantity"] = str(qty)
        params["closePosition"] = "false"
    return _req("POST", "/fapi/v1/algoOrder", params)


def get_open_algos(symbol: str = None) -> list:
    params = {}
    if symbol:
        params["symbol"] = symbol
    return _req("GET", "/fapi/v1/openAlgoOrders", params)


def cancel_algo(algo_id: str) -> dict:
    return _req("DELETE", "/fapi/v1/algoOrder", {
        "algoId": algo_id,
    })


# ─── K线 / ATR ───
def get_klines(symbol: str, interval: str = "5m", limit: int = 16) -> list:
    """获取K线数据"""
    return _public("/fapi/v1/klines", {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    })


def calc_atr(symbol: str, period: int = 14, interval: str = "5m") -> float:
    """计算 ATR (Average True Range)"""
    data = get_klines(symbol, interval, period + 2)
    trs = []
    for i in range(1, len(data)):
        hl = float(data[i][2]) - float(data[i][3])
        hc = abs(float(data[i][2]) - float(data[i - 1][4]))
        lc = abs(float(data[i][3]) - float(data[i - 1][4]))
        trs.append(max(hl, hc, lc))
    return sum(trs[-period:]) / period if trs else 0


# ─── 盘口 ───
def get_book_top(symbol: str) -> tuple[float | None, float | None]:
    """REST 获取当前 b1/a1"""
    data = _public("/fapi/v1/depth", {"symbol": symbol, "limit": 5})
    if data and data.get("bids") and data.get("asks"):
        return float(data["bids"][0][0]), float(data["asks"][0][0])
    return None, None
