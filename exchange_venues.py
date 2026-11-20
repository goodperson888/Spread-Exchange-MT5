"""Exchange identity and factories; legacy Binance journals retain their keys."""
VENUES = {'binance': {'name':'币安','currency':'USDT','symbol':'XAUUSDT'},
          'lighter': {'name':'Lighter','currency':'USDC','symbol':'XAU'}}


def venue(config):
    value=config.get('pair',{}).get('exchange','binance')
    if value not in VENUES: raise ValueError('暂不支持该交易所，请选择币安或 Lighter')
    return value


def metadata(config): return {'id':venue(config),**VENUES[venue(config)]}


def client(config,key='',secret='',binance_class=None):
    if venue(config)=='lighter':
        from lighter_broker import Lighter
        return Lighter(config.get('lighter',{}))
    if binance_class is None:
        from trading_brokers import Binance
        binance_class=Binance
    return binance_class(production=True,key=key,secret=secret,
        recv_window_ms=config['binance']['recv_window_ms'],proxy_url=config['binance'].get('proxy_url',''))


def stream(config,exchange,binance_class=None):
    if venue(config)=='lighter':
        from lighter_broker import LighterBookTicker
        return LighterBookTicker(exchange.spec(config['symbol'])['market_id'],exchange.proxy_url)
    if binance_class is None:
        from trading_brokers import BinanceBookTicker
        binance_class=BinanceBookTicker
    return binance_class(config['symbol'],production=True,proxy_url=config['binance'].get('proxy_url',''))
