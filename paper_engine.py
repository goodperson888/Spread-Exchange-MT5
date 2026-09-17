"""Deterministic paper pairing engine.

Direction for the first stage: sell Binance XAUUSDT, buy MT5 gold.
"""
from __future__ import annotations

import time
import math
import copy


def default_state():
    return {"running": False, "groups": [], "last_action": "idle", "updated_ms": int(time.time() * 1000)}


def _num(value, name):
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} 必须是数字") from None
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} 必须大于 0")
    return result


def quote_metrics(quote):
    b_bid = _num(quote.get("binance_bid"), "币安 Bid")
    b_ask = _num(quote.get("binance_ask"), "币安 Ask")
    m_bid = _num(quote.get("mt5_bid"), "MT5 Bid")
    m_ask = _num(quote.get("mt5_ask"), "MT5 Ask")
    if b_bid > b_ask or m_bid > m_ask:
        raise ValueError("Bid 不能大于 Ask")
    return {
        "entry_spread": b_bid - m_ask,
        "exit_spread": b_ask - m_bid,
        "binance_bid": b_bid,
        "binance_ask": b_ask,
        "mt5_bid": m_bid,
        "mt5_ask": m_ask,
    }


def step(state, config, plan, quote):
    state = copy.deepcopy(state or default_state())
    if not state.get('running'):
        raise ValueError('请先启动纸面引擎，再推进模拟撮合。')
    state.setdefault("groups", [])
    metrics = quote_metrics(quote)
    strategy = config["strategy"]
    threshold = float(strategy["entry_spread_usd"])
    target = float(strategy["take_contraction_usd"])
    max_groups = int(strategy.get("max_groups") or 1)
    qty = float(plan["gold_qty_oz"])
    now = int(time.time() * 1000)
    action = "hold"
    if len(state["groups"]) < max_groups and metrics["entry_spread"] >= threshold and not state["groups"]:
        group = {
            "id": now,
            "opened_ms": now,
            "mt5_lots": plan["mt5_lots"],
            "gold_qty_oz": qty,
            "entry_spread": metrics["entry_spread"],
            "open_quote": metrics,
            "direction": plan["direction"],
        }
        state["groups"].append(group)
        action = "open_paper_group"
    closed = []
    remaining = []
    for group in state["groups"]:
        contraction = float(group["entry_spread"]) - metrics["exit_spread"]
        pnl = contraction * float(group["gold_qty_oz"])
        if contraction >= target and pnl > 0:
            done = dict(group)
            done.update(closed_ms=now, close_quote=metrics, contraction=contraction, gross_pnl_usdt=pnl)
            closed.append(done)
            action = "close_paper_group"
        else:
            current = dict(group)
            current.update(current_contraction=contraction, gross_pnl_usdt=pnl)
            remaining.append(current)
    state["groups"] = remaining
    state["last_action"] = action
    state["last_metrics"] = metrics
    state["last_closed"] = closed
    state["updated_ms"] = now
    return {"state": state, "action": action, "metrics": metrics, "closed": closed}
