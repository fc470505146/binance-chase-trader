"""配置管理 — 读取 .env + 全局常量"""

import os
from pathlib import Path

# ─── 查找 .env 文件 ───
# 优先级: 当前目录 .env → ~/.binance-chase/.env → ~/.hermes/.env
_ENV_CANDIDATES = [
    Path.cwd() / ".env",
    Path.home() / ".binance-chase" / ".env",
    Path.home() / ".hermes" / ".env",
]


def _load_env():
    """扫描候选路径，加载第一个找到的 .env"""
    env = {}
    for p in _ENV_CANDIDATES:
        if p.exists():
            with open(p) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        env[k.strip()] = v.strip()
            break
    return env


_env = _load_env()

# ─── Binance API ───
API_KEY = _env.get("BINANCE_API_KEY", "")
SECRET_KEY = _env.get("BINANCE_SECRET_KEY", "")
FAPI_BASE = "https://fapi.binance.com"

# ─── Socket ───
SOCKET_PATH = _env.get("BINANCE_SOCKET_PATH", "/tmp/ws_data.sock")

# ─── 默认交易对 ───
DEFAULT_SYMBOLS = ["XAGUSDT", "XAUUSDT"]

# ─── 滑动窗口大小 ───
SLIDE_WINDOW = 200


def validate():
    """验证必要配置是否存在，返回 (ok, msg)"""
    if not API_KEY:
        return False, "BINANCE_API_KEY 未配置"
    if not SECRET_KEY:
        return False, "BINANCE_SECRET_KEY 未配置"
    return True, "ok"
