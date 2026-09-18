"""Windows executable entry point, also usable from source on other platforms."""
import json
import sys
import threading
import urllib.request
import webbrowser


def main():
    if '--mt5-probe' in sys.argv:
        from mt5_connector import probe_main
        probe_main()
        return
    if '--mt5-worker' in sys.argv:
        from trading_brokers import worker_main
        worker_main()
        return
    from server import DATA, HOST, PORT, Handler, ThreadingHTTPServer, stop_legacy_paper_on_boot
    url = f'http://{HOST}:{PORT}/'
    # Explicit self-check for packaging; never connects MT5 or starts a server.
    if '--self-test' in sys.argv:
        from server import ROOT
        import MetaTrader5
        for name in ('index.html', 'app.js', 'trading-app.js', 'quote-stream.js', 'mt5/GoldPairQuotes.mq5', 'mt5/GoldPairQuotes.ex5', 'echarts.min.js', 'style.css', 'config.example.json'):
            if not (ROOT / name).is_file():
                raise RuntimeError('Missing bundled asset: ' + name)
        print('Bundled assets and MetaTrader5 import: OK')
        return
    stop_legacy_paper_on_boot()
    try:
        server = ThreadingHTTPServer((HOST, PORT), Handler)
    except OSError:
        try:
            with urllib.request.urlopen(url + 'api/status', timeout=2) as response:
                status = json.load(response)
            if status.get('app') == 'GoldPairLocal':
                webbrowser.open(url)
                return
        except Exception:
            pass
        print(f'Port {PORT} is occupied. Close the conflicting app and retry.')
        if sys.stdin and sys.stdin.isatty():
            input('Press Enter to exit...')
        return
    print(f'GoldPairLocal: {url}\nLocal data: {DATA}\nKeep this window open. Ctrl+C exits.')
    threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
