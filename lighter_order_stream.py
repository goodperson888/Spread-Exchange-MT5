"""Private terminal-order evidence on the SDK loop; no submission or logging.

Protocol: https://docs.lighter.xyz/api/websocket.md (account_all_orders).
Only explicitly armed intents are retained. A snapshot, acceptance, missing
order, or open/partial update never establishes terminal execution.
"""
import asyncio
import json
import logging
import time
from urllib.parse import urlsplit, urlunsplit


def stream_url(rest_url):
    parts = urlsplit(rest_url)
    if parts.scheme not in ('https', 'http') or not parts.netloc:
        raise ValueError('Lighter 账户推送地址无效')
    return urlunsplit(('wss' if parts.scheme == 'https' else 'ws',
                       parts.netloc, parts.path.rstrip('/') + '/stream', '', ''))


class LighterOrderStream:
    def __init__(self, url, account_index, auth, validate, proxy_url=None):
        self.url, self.account_index = url, int(account_index)
        self.auth, self.validate, self.proxy_url = auth, validate, proxy_url
        self.ready = False
        self._intents = {}
        self._task = None
        # WebSocket DEBUG logs may contain the auth frame. Keep this private
        # connection's logger isolated even if application debugging is enabled.
        self._logger = logging.Logger(__name__ + '.private', level=logging.CRITICAL)
        self._logger.addHandler(logging.NullHandler())

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    def arm(self, plan, client_index):
        now = time.monotonic()
        self._intents = {key: value for key, value in self._intents.items()
                         if now - value['at'] < 120}
        if len(self._intents) >= 128:
            self._intents.pop(next(iter(self._intents)))
        key = (plan['market_index'], client_index)
        self._intents[key] = dict(plan=dict(plan), at=now, row=None, event=asyncio.Event())

    def order(self, market, client_index):
        entry = self._intents.get((market, client_index))
        if entry and time.monotonic() - entry['at'] < 120 and entry['row'] is not None:
            return dict(entry['row'])
        return None

    async def wait_order(self, market, client_index):
        entry = self._intents.get((market, client_index))
        if entry is None:
            return None
        while True:
            row = self.order(market, client_index)
            if row is not None or not self.ready:
                return row
            entry['event'].clear()
            await entry['event'].wait()

    def _reset(self):
        self.ready = False
        for entry in self._intents.values():
            entry['row'] = None
            entry['event'].set()

    def handle(self, message):
        if message.get('error') or message.get('type') == 'error':
            raise ValueError('Lighter 账户订单推送订阅失败')
        if message.get('type') not in ('subscribed/account_all_orders', 'update/account_all_orders'):
            return
        if message.get('channel') != f'account_all_orders:{self.account_index}':
            return
        orders = message.get('orders')
        if not isinstance(orders, dict):
            return
        self.ready = True
        for market, rows in orders.items():
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict) or str(row.get('market_index')) != market:
                    continue
                if any(type(row.get(field)) is not int for field in
                       ('owner_account_index', 'market_index', 'client_order_index')):
                    continue
                key = (row.get('market_index'), row.get('client_order_index'))
                entry = self._intents.get(key)
                if entry is None or time.monotonic() - entry['at'] >= 120:
                    continue
                try:
                    # Do not fill missing identity/amount fields from the intent.
                    required = ('owner_account_index', 'market_index', 'client_order_index',
                                'is_ask', 'reduce_only', 'initial_base_amount', 'status',
                                'filled_base_amount', 'filled_quote_amount', 'order_id', 'order_index')
                    if (any(field not in row for field in required) or
                            type(row['is_ask']) is not bool or type(row['reduce_only']) is not bool or
                            type(row['order_index']) is not int or row['order_index'] <= 0 or
                            not isinstance(row['order_id'], str) or not row['order_id'] or
                            not isinstance(row['status'], str)):
                        continue
                    if not self.validate(row, entry['plan'], self.account_index, key[1]):
                        continue
                except (ValueError, TypeError, KeyError, ArithmeticError):
                    continue
                # A terminal row is immutable evidence. Later open updates must
                # not replace it while the submit HTTP call is still returning.
                if entry['row'] is None:
                    entry['row'] = dict(row)
                    entry['event'].set()

    async def _run(self):
        delay = 1
        while True:
            try:
                from websockets.asyncio.client import connect
                async with connect(self.url, proxy=self.proxy_url, open_timeout=5,
                                   ping_interval=20, ping_timeout=20, close_timeout=1,
                                   max_size=4_000_000, logger=self._logger) as ws:
                    self._reset()
                    await ws.send(json.dumps(dict(type='subscribe',
                        channel=f'account_all_orders/{self.account_index}', auth=self.auth())))
                    # Renew short-lived authorization with a fresh connection;
                    # never persist the token or put it in a URL/error message.
                    expires = time.monotonic() + 480
                    while time.monotonic() < expires:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=45 if self.ready else 5)
                        except asyncio.TimeoutError:
                            if not self.ready:
                                raise
                            # A private account can have no orders for hours.
                            # Idle traffic is not a disconnect; control pings
                            # enforce liveness and this elicits an app pong.
                            await ws.send(json.dumps({'type': 'ping'}))
                            continue
                        message = json.loads(raw)
                        if message.get('type') == 'ping':
                            await ws.send(json.dumps({'type': 'pong'}))
                        self.handle(message)
                        if self.ready:
                            delay = 1
            except asyncio.CancelledError:
                raise
            except Exception:
                # Private errors can contain credentials; REST remains recovery.
                pass
            finally:
                self._reset()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def close(self):
        self._reset()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
