import json, os, queue, socket, subprocess, threading, time, tomllib, urllib.request
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
ROOT = Path(__file__).resolve().parents[2]
WORK = ROOT / 'target/host-validation/codex-run'
WORK.mkdir(parents=True, exist_ok=True)

class Provider(BaseHTTPRequestHandler):

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
        item = {'id': 'msg_fixture', 'type': 'message', 'role': 'assistant', 'status': 'completed', 'content': [{'type': 'output_text', 'text': 'OK', 'annotations': []}]}
        response = {'id': 'resp_fixture', 'object': 'response', 'status': 'completed', 'output': [item], 'usage': {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2}}
        events = [('response.created', {'response': {**response, 'status': 'in_progress', 'output': []}}), ('response.output_item.added', {'output_index': 0, 'item': {**item, 'status': 'in_progress', 'content': []}}), ('response.output_text.delta', {'output_index': 0, 'content_index': 0, 'item_id': item['id'], 'delta': 'OK'}), ('response.output_item.done', {'output_index': 0, 'item': item}), ('response.completed', {'response': response})]
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.end_headers()
        for kind, event in events:
            self.wfile.write(('event: ' + kind + '\ndata: ' + json.dumps({'type': kind, **event}) + '\n\n').encode())
        self.wfile.flush()

def toml(v):
    if isinstance(v, dict):
        return '{' + ','.join((json.dumps(k) + '=' + toml(x) for k, x in v.items())) + '}'
    if isinstance(v, list):
        return '[' + ','.join(map(toml, v)) + ']'
    return json.dumps(v)

class Server:

    def __init__(self, config, label):
        self.q = queue.Queue()
        self.i = 0
        self.events = []
        args = ['codex', 'app-server']
        for k, v in config.items():
            args += ['-c', k + '=' + toml(v)]
        self.log = open(WORK / (label + '.stderr'), 'w')
        config_home = WORK / 'config'
        config_home.mkdir(exist_ok=True)
        env = {**os.environ, 'CODEX_HOME': str(config_home)}
        self.p = subprocess.Popen(args, cwd=WORK, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True)
        threading.Thread(target=self.reader, daemon=True).start()
        self.call('initialize', {'clientInfo': {'name': 'interlink-host-validation', 'version': '1'}, 'capabilities': {'experimentalApi': True}})
        self.send({'jsonrpc': '2.0', 'method': 'initialized'})

    def reader(self):
        for line in self.p.stdout:
            try:
                self.q.put(json.loads(line))
            except ValueError:
                pass

    def send(self, v):
        self.p.stdin.write(json.dumps(v) + '\n')
        self.p.stdin.flush()

    def call(self, method, params):
        self.i += 1
        i = self.i
        self.send({'jsonrpc': '2.0', 'id': i, 'method': method, 'params': params})
        end = time.monotonic() + 25
        while time.monotonic() < end:
            v = self.q.get(timeout=max(0.1, end - time.monotonic()))
            if v.get('id') == i:
                if 'error' in v:
                    raise RuntimeError((method, v['error']))
                return v['result']
            self.events.append(v)
        raise TimeoutError(method)

    def close(self):
        self.p.stdin.close()
        try:
            self.p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.p.terminate()
            self.p.wait(timeout=5)
        self.log.close()
provider = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
threading.Thread(target=provider.serve_forever, daemon=True).start()
s = socket.socket()
s.bind(('127.0.0.1', 0))
port = s.getsockname()[1]
s.close()
url = f'http://127.0.0.1:{port}'
key = WORK / 'id.key'
if not key.exists():
    subprocess.run([str(ROOT / 'target/debug/interlink-keygen'), '--out', str(key)], check=True, stdout=subprocess.DEVNULL)
(WORK / 'peers.json').write_text('{}')
buslog = open(WORK / 'bus.stderr', 'w')
bus = subprocess.Popen([str(ROOT / 'target/debug/interlink-bus'), '--addr', f'127.0.0.1:{port}'], stdout=subprocess.DEVNULL, stderr=buslog)
base = tomllib.loads((ROOT / 'codex/config.toml').read_text())
base['mcp_servers']['interlink'] = {'enabled': True, 'command': str(ROOT / 'target/debug/interlink-mcp'), 'args': ['--host', 'codex', '--key', str(key), '--peers', str(WORK / 'peers.json'), '--url', url], 'env': {'XDG_STATE_HOME': str(WORK / 'state')}}
for event in ['PreToolUse', 'PermissionRequest', 'PostToolUse', 'PreCompact', 'PostCompact', 'SessionStart', 'SessionEnd', 'UserPromptSubmit', 'SubagentStart', 'SubagentStop', 'Stop', 'Interrupt']:
    base['hooks'].setdefault(event, [])
base.update({'features.plugins': False, 'features.apps': False, 'features.hooks': True, 'model_provider': 'fixture', 'model': 'fixture', 'model_providers.fixture': {'name': 'Local validation fixture', 'base_url': f'http://127.0.0.1:{provider.server_port}/v1', 'wire_api': 'responses', 'requires_openai_auth': False}, 'model_reasoning_effort': 'low'})
server = None
try:
    server = Server(base, 'untrusted')
    result = server.call('hooks/list', {'cwds': [str(WORK)]})
    hooks = [h for entry in result['data'] for h in entry['hooks'] if h.get('server') == 'interlink']
    print('sample hooks:', [(h['eventName'], h['trustStatus']) for h in hooks], flush=True)
    assert len(hooks) == 4, result
    server.close()
    server = None
    # Trust only the reviewed fixture definitions for this invocation.
    # No global trust file or bypass switch is changed.
    base['hooks']['state'] = {h['key']: {'trusted_hash': h['currentHash']} for h in hooks}
    server = Server(base, 'trusted')
    result = server.call('hooks/list', {'cwds': [str(WORK)]})
    hooks = [h for entry in result['data'] for h in entry['hooks'] if h.get('server') == 'interlink']
    assert all((h['trustStatus'] == 'trusted' for h in hooks)), hooks
    print('reviewed hooks: all trusted', flush=True)
    ids = []
    for _ in range(2):
        response = server.call('thread/start', {'cwd': str(WORK), 'ephemeral': True, 'approvalPolicy': 'never', 'sandbox': 'read-only', 'config': base})
        tid = response['thread']['id']
        ids.append(tid)
        server.call('turn/start', {'threadId': tid, 'input': [{'type': 'text', 'text': 'Reply OK.'}]})
        end = time.monotonic() + 20
        while time.monotonic() < end:
            try:
                roster = json.load(urllib.request.urlopen(url + '/roster', timeout=2))
                if tid in json.dumps(roster):
                    break
            except Exception:
                pass
            time.sleep(0.1)
        else:
            raise AssertionError('hook did not bind thread ' + tid)
        print('hook bound thread:', tid, flush=True)
    roster = json.load(urllib.request.urlopen(url + '/roster', timeout=2))
    assert all((tid in json.dumps(roster) for tid in ids))
    print('PASS: trusted lifecycle hooks bound two isolated Codex threads', flush=True)
finally:
    if server:
        server.close()
    bus.terminate()
    bus.wait(timeout=5)
    buslog.close()
    provider.shutdown()
