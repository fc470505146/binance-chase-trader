"""
🍗 binance-chase-trader CLI

用法:
    # 启动数据守护进程
    python -m binance_chase daemon

    # 开仓 (QUEUE限价+追单+自动止盈止损)
    python -m binance_chase trade XAGUSDT SELL 1 SHORT

    # 平仓
    python -m binance_chase trade XAGUSDT SELL 1 LONG --close

    # 查持仓
    python -m binance_chase pos
    python -m binance_chase pos XAGUSDT

    # 查 daemon 状态
    python -m binance_chase status

    # 查 daemon 实时盘口
    python -m binance_chase depth XAGUSDT
"""

import json
import sys

from . import config


def cmd_daemon():
    """启动 WS 数据守护进程"""
    from .daemon import run
    run()


def cmd_trade():
    """执行追单交易"""
    from .trader import place_limit
    ok, msg = config.validate()
    if not ok:
        print(f"❌ {msg}")
        sys.exit(1)

    if len(sys.argv) < 5:
        print("用法: python -m binance_chase trade <symbol> <side> <qty> <pos_side> [--close]")
        sys.exit(1)

    _, _, symbol, side, qty, pos_side = sys.argv[:6]
    close = "--close" in sys.argv[3:]

    result = place_limit(
        symbol=symbol.upper(),
        side=side.upper(),
        qty=float(qty),
        pos_side=pos_side.upper(),
        close=close,
    )
    print(json.dumps(result, indent=2))


def cmd_pos():
    """查持仓"""
    from .api import get_position_risk
    ok, _ = config.validate()
    if not ok:
        print("⚠️ 无API密钥，仅显示公开数据")

    symbol = sys.argv[2] if len(sys.argv) > 2 else None
    try:
        pos = get_position_risk(symbol) if ok else []
    except Exception as e:
        print(f"❌ {e}")
        return

    has_pos = False
    for p in pos:
        amt = float(p["positionAmt"])
        if amt != 0:
            has_pos = True
            print(f"{p['symbol']} {p['positionSide']}: {amt}张 "
                  f"@{p['entryPrice']} 标记价{p['markPrice']} "
                  f"盈亏{p['unRealizedProfit']} USDT")

    if not has_pos:
        print("无持仓")


def cmd_status():
    """查 daemon 状态"""
    from .trader import _ws_query
    data = _ws_query("STATUS")
    if data:
        print(json.dumps(data, indent=2))
    else:
        print("❌ daemon 未运行")


def cmd_depth():
    """查实时盘口"""
    from .trader import get_book_top_ws
    if len(sys.argv) < 3:
        print("用法: python -m binance_chase depth <symbol>")
        sys.exit(1)
    symbol = sys.argv[2].upper()
    bid, ask = get_book_top_ws(symbol)
    if bid:
        print(f"{symbol}: bid={bid} ask={ask}")
    else:
        print(f"❌ 无法获取 {symbol} 盘口")


CMD_MAP = {
    "daemon": cmd_daemon,
    "trade": cmd_trade,
    "pos": cmd_pos,
    "status": cmd_status,
    "depth": cmd_depth,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    fn = CMD_MAP.get(cmd)
    if not fn:
        print(f"未知命令: {cmd}")
        print(__doc__)
        sys.exit(1)

    fn()


if __name__ == "__main__":
    main()
