"""Read-only MetaTrader 5 market data through a local MCP HTTP server."""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlparse


PROTOCOL_VERSION = "2025-06-18"
DEFAULT_URL = "http://127.0.0.1:22346/mcp"


def validate_mcp_url(url):
    parsed = urlparse(str(url or DEFAULT_URL))
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("MT5 MCP 地址必须是本机 HTTP 地址")
    if not parsed.port:
        raise ValueError("MT5 MCP 地址必须包含端口")
    return parsed.geturl()


class McpClient:
    """Small Streamable HTTP MCP client supporting JSON and SSE responses."""
    def __init__(self, url=DEFAULT_URL, token="", timeout=12):
        self.url = validate_mcp_url(url)
        self.token = str(token or os.environ.get("MT5_TERMINAL_MCP_TOKEN", "")).strip()
        self.timeout = timeout
        self.session_id = ""
        self.counter = 0
        self.lock = threading.Lock()
        self.initialized = False

    @staticmethod
    def _decode(raw, content_type):
        text = raw.decode("utf-8", errors="replace").strip()
        if not text:
            return None
        if "text/event-stream" in content_type:
            messages = []
            for line in text.splitlines():
                if line.startswith("data:"):
                    value = line[5:].strip()
                    if value and value != "[DONE]":
                        messages.append(json.loads(value))
            return messages[-1] if messages else None
        return json.loads(text)

    def _post(self, payload):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
        }
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        request = urllib.request.Request(
            self.url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                session = response.headers.get("Mcp-Session-Id")
                if session:
                    self.session_id = session
                return self._decode(response.read(), response.headers.get("Content-Type", ""))
        except urllib.error.HTTPError as exc:
            detail = exc.read(1000).decode("utf-8", errors="replace")
            if exc.code in (401, 403):
                raise ValueError("MT5 MCP 认证失败，请输入新 Token 或设置 MT5_TERMINAL_MCP_TOKEN") from None
            raise ValueError(f"MT5 MCP HTTP {exc.code}：{detail or exc.reason}") from None
        except urllib.error.URLError as exc:
            raise ValueError("无法连接 MT5 MCP：" + str(exc.reason)) from None

    def _rpc(self, method, params=None, notification=False):
        with self.lock:
            payload = {"jsonrpc": "2.0", "method": method}
            if params is not None:
                payload["params"] = params
            if not notification:
                self.counter += 1
                payload["id"] = self.counter
            result = self._post(payload)
            if notification:
                return None
            if not isinstance(result, dict):
                raise ValueError("MT5 MCP 返回了无效响应")
            if result.get("error"):
                error = result["error"]
                raise ValueError("MT5 MCP：" + str(error.get("message", error)))
            return result.get("result")

    def initialize(self):
        if self.initialized:
            return
        self._rpc("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "GoldPairLocal", "version": "0.2"},
        })
        self._rpc("notifications/initialized", notification=True)
        self.initialized = True

    def call_tool(self, name, arguments=None):
        self.initialize()
        result = self._rpc("tools/call", {"name": name, "arguments": arguments or {}})
        if not isinstance(result, dict):
            raise ValueError("MT5 MCP 工具返回无效")
        if result.get("isError"):
            raise ValueError("MT5 MCP 工具执行失败")
        for block in result.get("content", []):
            if block.get("type") == "text":
                try:
                    return json.loads(block.get("text", ""))
                except json.JSONDecodeError:
                    raise ValueError("MT5 MCP 工具未返回 JSON") from None
        structured = result.get("structuredContent")
        if isinstance(structured, dict):
            return structured
        raise ValueError("MT5 MCP 工具没有可读数据")

    def close(self):
        # Streamable HTTP sessions expire server-side. No trading state is held.
        self.session_id = ""
        self.initialized = False


def _timestamp_ms(value):
    try:
        stamp = datetime.strptime(value, "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(stamp.timestamp() * 1000)
    except (TypeError, ValueError):
        return int(time.time() * 1000)


def _gold_symbols(rows):
    return [row for row in rows if str(row.get("currency_base", "")).upper() == "XAU"
            and str(row.get("currency_profit", "")).upper() == "USD"
            and int(row.get("trade_mode", 0) or 0) != 0]


def _symbol_result(row):
    stamp = _timestamp_ms(row.get("update_time"))
    bid, ask = float(row.get("bid", 0)), float(row.get("ask", 0))
    if bid <= 0 or ask < bid:
        raise ValueError("MT5 MCP 黄金报价无效")
    return {
        "name": str(row["symbol"]),
        "contract_size_oz": float(row.get("contract_size", 0)),
        "volume_min": float(row.get("volume_min", 0)),
        "volume_step": float(row.get("volume_step", 0)),
        "volume_max": float(row.get("volume_max", 0)),
        "digits": int(row.get("digits", 0)),
        "trade_mode": int(row.get("trade_mode", 0)),
        "currency_base": str(row.get("currency_base", "")),
        "currency_profit": str(row.get("currency_profit", "")),
    }, {
        "bid": bid, "ask": ask, "time_ms": stamp,
        "age_ms": max(0, int(time.time() * 1000) - stamp),
    }


def inspect_mcp_terminal(settings, token="", client=None):
    own_client = client is None
    client = client or McpClient(settings.get("mcp_url", DEFAULT_URL), token)
    try:
        account_data = client.call_tool("get_trading_account_info")
        requested = str(settings.get("symbol") or "").strip()
        symbol_data = client.call_tool("get_marketwatch_symbols", {
            "include_hidden": True, "limit": 10, **({"symbol": requested} if requested else {})
        })
        rows = symbol_data.get("symbols", [])
        if not rows:
            rows = client.call_tool("get_marketwatch_symbols", {
                "include_hidden": True, "limit": 1000
            }).get("symbols", [])
        candidates = _gold_symbols(rows)
        selected = next((row for row in candidates if row.get("symbol", "").lower() == requested.lower()), None)
        if not selected and not requested and len(candidates) == 1:
            selected = candidates[0]
        account = account_data.get("account", {})
        terminal = account_data.get("terminal", {})
        identity = {
            "account": str(account.get("login", "")), "server": str(account.get("server", "")),
            "currency": str(account.get("currency", "")), "margin_mode": account.get("margin_mode"),
        }
        expected_account, expected_server = str(settings.get("account") or ""), str(settings.get("server") or "")
        identity_matches = (not expected_account or expected_account == identity["account"]) and (
            not expected_server or expected_server == identity["server"])
        blockers = []
        if not terminal.get("server_connected"):
            blockers.append("MT5 未连接交易服务器")
        if not identity_matches:
            blockers.append("MT5 实际账户或服务器与目标不一致")
        if not selected:
            names = [str(row.get("symbol")) for row in candidates]
            return {
                "connected": bool(terminal.get("server_connected")), "code": "SELECT_SYMBOL",
                "message": "MT5 MCP 已连接，请选择实际黄金品种。",
                "real_order_enabled": False, "checked_at_ms": int(time.time() * 1000),
                "identity_matches": identity_matches, "identity": identity, "terminal_path": "MCP",
                "candidates": names, "symbol": None, "quote": None, "blockers": blockers,
            }
        spec, quote = _symbol_result(selected)
        for key in ("contract_size_oz", "volume_min", "volume_step", "volume_max"):
            if spec[key] <= 0:
                blockers.append("MT5 MCP 合约规格不完整：" + key)
        return {
            "connected": bool(terminal.get("server_connected")), "code": "MCP_CONNECTED",
            "message": "已连接 MT5 MCP 真实行情；本适配器只读，纸面模式不会向 MT5 下单。",
            "real_order_enabled": False, "checked_at_ms": int(time.time() * 1000),
            "identity_matches": identity_matches, "identity": identity, "terminal_path": "MCP",
            "candidates": [str(row.get("symbol")) for row in candidates],
            "symbol": spec, "quote": quote, "blockers": blockers,
        }
    finally:
        if own_client:
            client.close()


class McpTerminal:
    """Persistent read-only terminal used only with the paper broker."""
    def __init__(self, settings, token=""):
        self.settings = dict(settings)
        self.client = McpClient(settings.get("mcp_url", DEFAULT_URL), token)
        checked = inspect_mcp_terminal(settings, token, self.client)
        if not checked.get("connected") or not checked.get("identity_matches") or not checked.get("symbol"):
            raise ValueError(checked.get("message", "MT5 MCP 检查失败"))
        if checked.get("blockers"):
            raise ValueError("；".join(checked["blockers"]))
        self.spec, self.account = checked["symbol"], checked["identity"]
        self.last_account_check = time.monotonic()
        self.last_snapshot = None
        self.last_snapshot_at = 0.0

    def call(self, command, **_args):
        if command != "snapshot":
            raise ValueError("MT5 MCP 适配器只允许读取行情")
        if self.last_snapshot and time.monotonic() - self.last_snapshot_at < 0.1:
            return self.last_snapshot
        if time.monotonic() - self.last_account_check >= 5:
            account = self.client.call_tool("get_trading_account_info").get("account", {})
            if str(account.get("login", "")) != self.account["account"] or str(account.get("server", "")) != self.account["server"]:
                raise ValueError("MT5 MCP 账户发生变化，已暂停策略")
            self.last_account_check = time.monotonic()
        rows = self.client.call_tool("get_marketwatch_symbols", {
            "symbol": self.spec["name"], "include_hidden": True, "limit": 10
        }).get("symbols", [])
        if not rows:
            raise ValueError("MT5 MCP 当前品种不可用")
        spec, quote = _symbol_result(rows[0])
        if spec["name"] != self.spec["name"] or spec["contract_size_oz"] != self.spec["contract_size_oz"]:
            raise ValueError("MT5 MCP 合约规格发生变化，请重新检查")
        self.last_snapshot = {"quote": quote, "spec": spec, "account": self.account,
                              "allowed": True, "positions": []}
        self.last_snapshot_at = time.monotonic()
        return self.last_snapshot

    def close(self):
        self.client.close()
