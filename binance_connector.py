"""Small Binance USD-M Futures testnet client.

The module only reads account/market data. It does not place orders.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


TESTNET_BASE = "https://demo-fapi.binance.com"
PRODUCTION_BASE = "https://fapi.binance.com"
ALLOWED_BASES = {TESTNET_BASE, PRODUCTION_BASE}


def _number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _request(base_url, path, params=None, headers=None, timeout=8):
    params = params or {}
    query = urlencode(params)
    url = base_url.rstrip("/") + path + (("?" + query) if query else "")
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8")), dict(response.headers)
    except HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            message = payload.get("msg") or payload.get("message") or str(exc)
        except Exception:
            message = str(exc)
        raise ValueError(f"币安返回错误：{message}") from None
    except URLError as exc:
        raise ValueError(f"无法连接币安测试网：{exc.reason}") from None


def _signed_params(secret, params):
    query = urlencode(params)
    signature = hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return query + "&signature=" + signature


def _signed_request(base_url, path, api_key, api_secret, params=None, timeout=8):
    params = params or {}
    query = _signed_params(api_secret, params)
    url = base_url.rstrip("/") + path + "?" + query
    request = Request(url, headers={"X-MBX-APIKEY": api_key})
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8")), dict(response.headers)
    except HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            message = payload.get("msg") or payload.get("message") or str(exc)
        except Exception:
            message = str(exc)
        raise ValueError(f"币安签名检查失败：{message}") from None
    except URLError as exc:
        raise ValueError(f"无法连接币安测试网：{exc.reason}") from None


def public_snapshot(settings):
    symbol = str(settings.get("symbol") or "XAUUSDT").upper()
    base_url = str(settings.get("base_url") or TESTNET_BASE).rstrip("/")
    if base_url not in ALLOWED_BASES:
        raise ValueError("币安地址只允许官方 USD-M Futures 地址")
    info, _ = _request(base_url, "/fapi/v1/exchangeInfo", {"symbol": symbol})
    symbols = [item for item in info.get("symbols", []) if item.get('symbol') == symbol]
    if not symbols:
        raise ValueError(f"测试网未返回 {symbol} 合约信息")
    spec = symbols[0]
    filters = {item.get("filterType"): item for item in spec.get("filters", [])}
    lot = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE") or {}
    notional = filters.get("MIN_NOTIONAL") or {}
    ticker, _ = _request(base_url, "/fapi/v1/ticker/bookTicker", {"symbol": symbol})
    bid = _number(ticker.get("bidPrice"))
    ask = _number(ticker.get("askPrice"))
    if bid <= 0 or ask <= 0 or bid > ask:
        raise ValueError("币安测试网返回的 Bid/Ask 无效")
    return {
        "base_url": base_url,
        "symbol": symbol,
        "status": spec.get("status"),
        "contract_type": spec.get("contractType"),
        "quantity_precision": spec.get("quantityPrecision"),
        "price_precision": spec.get("pricePrecision"),
        "lot_size": {
            "min_qty": _number(lot.get("minQty")),
            "max_qty": _number(lot.get("maxQty")),
            "step_size": _number(lot.get("stepSize")),
        },
        "min_notional": _number(notional.get("notional") or notional.get("minNotional")),
        "quote": {
            "bid": bid,
            "ask": ask,
            "bid_qty": _number(ticker.get("bidQty")),
            "ask_qty": _number(ticker.get("askQty")),
            "time_ms": int(ticker.get("time") or time.time() * 1000),
        },
    }


def check_account(settings, api_key="", api_secret=""):
    snapshot = public_snapshot(settings)
    api_key, api_secret = str(api_key or "").strip(), str(api_secret or "").strip()
    result = {
        "ok": True,
        "network": "testnet" if snapshot["base_url"] == TESTNET_BASE else "production",
        "public": snapshot,
        "private_checked": False,
        "real_order_enabled": False,
        "message": "已读取币安 USD-M 行情和合约过滤器；未执行订单。",
    }
    if not api_key and not api_secret:
        result["message"] += " 未填写 API Key，跳过账户签名检查。"
        return result
    if not api_key or not api_secret:
        raise ValueError("API Key 和 Secret Key 需要同时填写")
    server_time, _ = _request(snapshot["base_url"], "/fapi/v1/time")
    recv_window = int(float(settings.get("recv_window_ms") or 1000))
    account, _ = _signed_request(snapshot["base_url"], "/fapi/v3/account", api_key, api_secret, {
        "timestamp": int(server_time.get("serverTime") or time.time() * 1000),
        "recvWindow": recv_window,
    })
    result["private_checked"] = True
    result["account"] = {
        "can_trade": bool(account.get("canTrade")),
        "multi_assets_margin": bool(account.get("multiAssetsMargin")),
        "total_wallet_balance": account.get("totalWalletBalance"),
        "available_balance": account.get("availableBalance"),
        "assets_count": len(account.get("assets") or []),
        "positions_count": len(account.get("positions") or []),
    }
    result["message"] = "已完成币安测试网签名账户检查；未执行订单。"
    return result
