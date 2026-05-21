"""
🍗 实时盘口数据守护进程
常驻 WebSocket bookTicker 连接，通过 Unix socket 提供实时盘口查询。
价格变化存入滑动窗口供分析。
"""
import asyncio
import json
import os
import signal
import socket
import struct
import time
from collections import deque

from . import config


class BookTickerDaemon:
    """WS数据守护进程
    - 常驻 Binance bookTicker 连接
    - Unix socket 对外提供盘口查询
    """

    def __init__(self, symbols=None, socket_path=None, window=None):
        self.symbols = symbols or config.DEFAULT_SYMBOLS
        self.socket_path = socket_path or config.SOCKET_PATH
        self.window_size = window or config.SLIDE_WINDOW
        self._depth = {}       # {symbol: {bid, ask, updated}}
        self._history = {}     # {symbol: deque}
        self._reconnects = {}  # {symbol: {count, last, backoff}}
        self._start_time = 0.0
        self._init_state()

    def _init_state(self):
        for s in self.symbols:
            self._depth[s] = {"bid": 0.0, "ask": 0.0, "updated": 0}
            self._history[s] = deque(maxlen=self.window_size)
            self._reconnects[s] = {"count": 0, "last": 0.0, "backoff": 0}

    # ── WS 连接管理 ──

    async def _ws_connect(self, symbol):
        stream = f"{symbol.lower()}@bookTicker"
        stat = self._reconnects[symbol]

        while True:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection("fstream.binance.com", 443, ssl=True),
                    timeout=10,
                )

                key = os.urandom(16).hex()
                handshake = (
                    f"GET /ws/{stream} HTTP/1.1\r\n"
                    f"Host: fstream.binance.com\r\n"
                    f"Upgrade: websocket\r\n"
                    f"Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Key: {key}\r\n"
                    f"Sec-WebSocket-Version: 13\r\n"
                    f"\r\n"
                )
                writer.write(handshake.encode())
                await writer.drain()

                header = b""
                while b"\r\n\r\n" not in header:
                    header += await reader.readuntil(b"\n")
                    if len(header) > 4096:
                        raise Exception("Handshake header too large")

                if b"101" not in header:
                    raise Exception(f"Handshake failed: {header[:200]}")

                if stat["backoff"] > 0:
                    print(f"  🔄 {symbol} 重连成功")
                stat["backoff"] = 0

                while True:
                    frame = await self._read_ws_frame(reader, writer)
                    if frame is None:
                        break
                    await self._on_ticker(frame, symbol)

            except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                now = time.localtime()
                is_midnight = now.tm_hour == 8 and now.tm_min <= 5
                tag = "🌙 0点维护" if is_midnight else "⚠️"
                print(f"  {tag} {symbol}: {e}")
            except Exception as e:
                print(f"  ⚠️ {symbol}: {e}")

            stat["count"] += 1
            stat["last"] = time.time()
            delay = min(5 * (2 ** stat["backoff"]), 60) if stat["backoff"] < 5 else 60
            stat["backoff"] = min(stat["backoff"] + 1, 6)
            print(f"  ⏳ {symbol} {delay}s后重连 (#{stat['count']})")
            await asyncio.sleep(delay)

    @staticmethod
    async def _read_ws_frame(reader, writer):
        try:
            header = await asyncio.wait_for(reader.readexactly(2), timeout=60)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError):
            return None
        if not header or len(header) < 2:
            return None

        b0, b1 = header[0], header[1]
        opcode = b0 & 0x0F
        masked = (b1 & 0x80) != 0
        length = b1 & 0x7F

        if length == 126:
            length = struct.unpack(">H", await reader.readexactly(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", await reader.readexactly(8))[0]

        if masked:
            mask = await reader.readexactly(4)
            payload = await reader.readexactly(length)
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        else:
            payload = await reader.readexactly(length)

        if opcode == 0x9:  # ping
            await self._send_pong(writer, payload)
            return None
        elif opcode in (0x8, 0xA):  # close / pong
            return None
        elif opcode == 0x1:  # text
            return payload.decode("utf-8")
        return None

    @staticmethod
    async def _send_pong(writer, payload):
        try:
            frame = bytearray()
            frame.append(0x8A)
            if len(payload) < 126:
                frame.append(len(payload))
            else:
                frame.append(126)
                frame.extend(struct.pack(">H", len(payload)))
            frame.extend(payload)
            writer.write(bytes(frame))
            await writer.drain()
        except Exception:
            pass

    async def _on_ticker(self, raw: str, symbol: str):
        try:
            data = json.loads(raw).get("data", json.loads(raw))
            sym = data.get("s", symbol).upper()
            bid = float(data.get("b", 0))
            ask = float(data.get("a", 0))
            now = time.time()

            prev = self._depth.get(sym, {})
            prev_bid, prev_ask = prev.get("bid", 0), prev.get("ask", 0)

            self._depth[sym] = {"bid": bid, "ask": ask, "updated": now}

            if bid != prev_bid or ask != prev_ask:
                self._history[sym].append((now, bid, ask))
        except Exception:
            pass

    # ── Unix Socket 服务 ──

    async def _serve_socket(self):
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

        server = await asyncio.start_unix_server(
            self._handle_client, self.socket_path
        )
        os.chmod(self.socket_path, 0o666)
        print(f"  🏠 Unix socket: {self.socket_path}")
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader, writer):
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=10)
            raw = data.decode().strip()
        except (asyncio.TimeoutError, ConnectionError):
            writer.close()
            return

        parts = raw.split(maxsplit=1)
        cmd = parts[0].upper() if parts else ""
        arg = parts[1] if len(parts) > 1 else ""

        try:
            resp = self._dispatch(cmd, arg)
        except Exception as e:
            resp = json.dumps({"error": str(e)})

        writer.write(resp.encode() + b"\n")
        await writer.drain()
        writer.close()

    def _dispatch(self, cmd: str, arg: str) -> str:
        arg_upper = arg.upper()

        if cmd == "PING":
            return json.dumps({"pong": time.time()})

        if cmd == "DEPTH":
            if arg_upper not in self.symbols:
                return json.dumps({"error": f"unknown symbol {arg}"})
            d = self._depth.get(arg_upper, {})
            return json.dumps({
                "symbol": arg_upper,
                "bid": d.get("bid", 0),
                "ask": d.get("ask", 0),
                "updated": d.get("updated", 0),
                "age": time.time() - d.get("updated", time.time()),
            })

        if cmd == "HISTORY":
            if arg_upper not in self.symbols:
                return json.dumps({"error": f"unknown symbol {arg}"})
            h = list(self._history.get(arg_upper, []))
            return json.dumps({"symbol": arg_upper, "count": len(h), "entries": h})

        if cmd == "STATUS":
            return json.dumps({
                "symbols": {
                    s: {
                        "bid": self._depth.get(s, {}).get("bid", 0),
                        "ask": self._depth.get(s, {}).get("ask", 0),
                        "age": time.time() - self._depth.get(s, {}).get("updated", time.time()),
                        "connected": time.time() - self._depth.get(s, {}).get("updated", time.time()) < 10,
                        "history": len(self._history.get(s, [])),
                        "reconnects": self._reconnects.get(s, {}).get("count", 0),
                        "last_reconnect": self._reconnects.get(s, {}).get("last", 0),
                        "backoff": self._reconnects.get(s, {}).get("backoff", 0),
                    }
                    for s in self.symbols
                },
                "uptime": time.time() - self._start_time,
            })

        return json.dumps({"error": f"unknown cmd {cmd}"})

    # ── 主入口 ──

    async def run(self):
        """启动 daemon：WS连接 + Unix socket"""
        self._start_time = time.time()
        print(f"🍗 {', '.join(self.symbols)} @bookTicker")
        print(f"  滑动窗口: {self.window_size}条/symbol")
        print(f"  PID: {os.getpid()}")

        tasks = [asyncio.create_task(self._ws_connect(s)) for s in self.symbols]
        tasks.append(asyncio.create_task(self._serve_socket()))

        loop = asyncio.get_running_loop()
        stop = asyncio.Future()

        def _shutdown():
            print("\n👋 关闭...")
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)
            stop.set_result(True)

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _shutdown)

        await stop
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def run(symbols=None, socket_path=None, window=None):
    """快捷启动函数"""
    daemon = BookTickerDaemon(symbols, socket_path, window)
    asyncio.run(daemon.run())
