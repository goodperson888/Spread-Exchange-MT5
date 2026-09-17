import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mt5_mcp import McpClient


class McpHandler(BaseHTTPRequestHandler):
    methods = []

    def log_message(self, *_args): pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
        self.__class__.methods.append(body['method'])
        if body['method'] == 'notifications/initialized':
            self.send_response(202); self.end_headers(); return
        if body['method'] == 'initialize':
            payload = {'jsonrpc':'2.0','id':body['id'],'result':{
                'protocolVersion':'2025-06-18','capabilities':{},
                'serverInfo':{'name':'test','version':'1'}}}
            raw = json.dumps(payload).encode()
            self.send_response(200); self.send_header('Content-Type','application/json')
            self.send_header('Mcp-Session-Id','test-session'); self.send_header('Content-Length',str(len(raw)))
            self.end_headers(); self.wfile.write(raw); return
        content = {'account':{'login':123}}
        payload = {'jsonrpc':'2.0','id':body['id'],'result':{
            'content':[{'type':'text','text':json.dumps(content)}], 'isError':False}}
        raw = ('data: '+json.dumps(payload)+'\n\n').encode()
        self.send_response(200); self.send_header('Content-Type','text/event-stream')
        self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)


class McpProtocolTests(unittest.TestCase):
    def test_streamable_http_json_and_sse(self):
        McpHandler.methods = []
        http = ThreadingHTTPServer(('127.0.0.1', 0), McpHandler)
        thread = threading.Thread(target=http.serve_forever, daemon=True); thread.start()
        try:
            client = McpClient(f'http://127.0.0.1:{http.server_port}/mcp', 'session-token')
            self.assertEqual(client.call_tool('get_trading_account_info')['account']['login'], 123)
            self.assertEqual(client.session_id, 'test-session')
            self.assertEqual(McpHandler.methods,
                             ['initialize','notifications/initialized','tools/call'])
        finally:
            http.shutdown(); http.server_close(); thread.join()


if __name__ == '__main__': unittest.main()
