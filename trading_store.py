"""Durable order intents and state. Never store credentials in the journal."""
import json
import sqlite3
import threading
import time
from pathlib import Path


def same_chart_market(left, right):
    """A changed terminal path does not change this account's quote history."""
    if left == right: return True
    a,b=str(left).split('|'),str(right).split('|')
    return len(a)==len(b)==9 and a[:4]+a[5:]==b[:4]+b[5:]


class Store:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, time INTEGER, kind TEXT, data TEXT);
            CREATE TABLE IF NOT EXISTS quotes (time INTEGER PRIMARY KEY, symbol TEXT, data TEXT);
            CREATE TABLE IF NOT EXISTS quote_minutes (
                time INTEGER, symbol TEXT, data TEXT, PRIMARY KEY(time, symbol)
            );
            CREATE INDEX IF NOT EXISTS quote_symbol_time ON quotes(symbol, time);
        ''')
        self.last_cleanup = 0

    def load(self):
        with self.lock:
            row = self.db.execute('SELECT value FROM state WHERE id=1').fetchone()
            return json.loads(row[0]) if row else None

    def commit(self, state, kind, data=None):
        with self.lock, self.db:
            self.db.execute('INSERT OR REPLACE INTO state VALUES (1,?)', (json.dumps(state, allow_nan=False),))
            self.db.execute('INSERT INTO events(time,kind,data) VALUES (?,?,?)',
                            (int(time.time()*1000), kind, json.dumps(data or {}, allow_nan=False)))

    def sample(self, quote):
        with self.lock, self.db:
            # The timestamp is the sample identity. A duplicate timestamp must
            # not mutate an already-rendered historical point.
            self.db.execute('INSERT OR IGNORE INTO quotes VALUES (?,?,?)',
                            (quote['time_ms'], quote['key'], json.dumps(quote, allow_nan=False)))
            bucket = quote['time_ms'] - quote['time_ms'] % 60000
            # Archive points are immutable. High-frequency rows remain in
            # `quotes` for the recent window; never rewrite a prior minute.
            self.db.execute('INSERT OR IGNORE INTO quote_minutes VALUES (?,?,?)',
                            (bucket, quote['key'], json.dumps(quote, allow_nan=False)))
            now = int(time.time()*1000)
            if now - self.last_cleanup >= 60000:
                self.db.execute('DELETE FROM quotes WHERE time < ?', (now-86400000,))
                self.db.execute('DELETE FROM quote_minutes WHERE time < ?', (now-30*86400000,))
                self.last_cleanup = now

    def _bucketed(self, db, table, keys, start, end, limit):
        if end <= start or limit <= 0:
            return []
        keys=list(dict.fromkeys(str(x) for x in (keys if isinstance(keys, (list, tuple, set)) else [keys]) if x))
        if not keys:
            return []
        bucket=max(1,(end-start+limit-1)//limit)
        marks=','.join('?' for _ in keys)
        return db.execute(f'''
            SELECT data FROM {table}
            WHERE symbol IN ({marks}) AND time>=? AND time<? AND time IN (
                SELECT MAX(time) FROM {table}
                WHERE symbol IN ({marks}) AND time>=? AND time<?
                GROUP BY CAST((time-?)/? AS INTEGER)
            ) ORDER BY time
        ''',(*keys,start,end,*keys,start,end,start,bucket)).fetchall()

    def _history_keys(self, db, key):
        """Never let another broker/account crowd this chart out of its buckets."""
        key=str(key or '')
        if not key:
            return []
        parts=key.split('|')
        if len(parts)<3 or not parts[2]:
            return [key]
        symbol=parts[2]
        pattern=f'%|{symbol}|%'
        rows=db.execute('''
            SELECT DISTINCT symbol FROM quote_minutes WHERE symbol=? OR symbol LIKE ?
            UNION
            SELECT DISTINCT symbol FROM quotes WHERE symbol=? OR symbol LIKE ?
        ''',(key,pattern,key,pattern)).fetchall()
        return list(dict.fromkeys([key]+[str(row[0]) for row in rows if same_chart_market(key,str(row[0]))]))

    def samples(self, key, since=0, limit=20000, until=None):
        with self.lock:
            return self._samples(self.db,key,since,limit,until)

    def read_samples(self, key, since=0, limit=20000, until=None):
        # A chart reload must not take the order-intent writer's lock.
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True,timeout=1)
        try:
            db.execute('BEGIN')
            return self._samples(db,key,since,limit,until)
        finally:
            db.close()

    def _samples(self, db, key, since, limit, until):
        now=int(time.time()*1000); end=min(now+1,int(until)+1) if until is not None else now+1
        cutoff=now-86400000; limit=min(20000,max(100,int(limit)))
        if since>=end: return []
        keys=self._history_keys(db,key)
        old_end=min(end,cutoff); new_start=max(since,cutoff)
        old_duration=max(0,old_end-since); new_duration=max(0,end-new_start)
        total=max(1,old_duration+new_duration)
        old_limit=int(limit*old_duration/total) if old_duration else 0
        rows=self._bucketed(db,'quote_minutes',keys,since-since%60000,old_end,old_limit)
        rows+=self._bucketed(db,'quotes',keys,new_start,end,limit-old_limit)
        samples=(json.loads(x[0]) for x in rows)
        return sorted((row for row in samples if since<=row.get('time_ms',0)<end),key=lambda row:row.get('time_ms',0))

    def events(self, limit=100):
        with self.lock:
            return [dict(id=r[0], time=r[1], kind=r[2], data=json.loads(r[3])) for r in self.db.execute(
                'SELECT id,time,kind,data FROM events ORDER BY id DESC LIMIT ?', (limit,))]

    def close(self):
        with self.lock:
            self.db.close()
