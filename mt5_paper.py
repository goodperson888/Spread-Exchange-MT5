"""Local MT5-like simulator for Mac/source development."""
from __future__ import annotations

import time


def inspect_paper_terminal(settings):
    symbol = str(settings.get("symbol") or "XAUUSD.paper")
    contract = float(settings.get("contract_size_oz") or 100)
    volume_min = float(settings.get("volume_min") or 0.01)
    volume_step = float(settings.get("volume_step") or 0.01)
    volume_max = float(settings.get("volume_max") or 100)
    bid = float(settings.get("paper_bid") or 4300.0)
    ask = float(settings.get("paper_ask") or (bid + 0.3))
    if bid <= 0 or ask < bid:
        return {
            "connected": False,
            "code": "PAPER_QUOTE_INVALID",
            "message": "本地 MT5 模拟器报价无效，请保持 Ask 大于等于 Bid。",
            "real_order_enabled": False,
            "checked_at_ms": int(time.time() * 1000),
        }
    return {
        "connected": True,
        "code": "PAPER_CONNECTED",
        "message": "已启用本地 MT5 模拟器；用于 Mac 开发验证，不连接真实 MT5 账户。",
        "real_order_enabled": False,
        "checked_at_ms": int(time.time() * 1000),
        "identity_matches": True,
        "identity": {
            "account": str(settings.get("account") or "PAPER-MT5"),
            "server": str(settings.get("server") or "local-paper"),
            "currency": "USD",
            "margin_mode": 2,
        },
        "terminal_path": "",
        "candidates": [symbol],
        "symbol": {
            "name": symbol,
            "contract_size_oz": contract,
            "volume_min": volume_min,
            "volume_step": volume_step,
            "volume_max": volume_max,
            "digits": 2,
            "trade_mode": 4,
            "currency_base": "XAU",
            "currency_profit": "USD",
        },
        "quote": {
            "bid": bid,
            "ask": ask,
            "time_ms": int(time.time() * 1000),
            "age_ms": 0,
        },
        "blockers": [],
    }
