import json, os, queue, socket, subprocess, threading, time, urllib.request, uuid
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
ROOT = Path(__file__).resolve().parents[2]
WORK = ROOT / 'target/host-validation/claude-run'
WORK.mkdir(parents=True, exist_ok=True)
REQUESTS = []

class Provider(BaseHTTPRequestHandler):

    def log_message(self, *args):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        body = json.loads(raw) if raw else {}
        if 'count_tokens' in self.path:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"input_tokens":1}')
            return
        REQUESTS.append(body)
        msg = {'id': 'msg_fixture_' + str(len(REQUESTS)), 'type': 'message', 'role': 'assistant', 'model': body.get('model', 'fixture'), 'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': 1, 'output_tokens': 1}}
        events = [('message_start', {'type': 'message_start', 'message': msg}), ('content_block_start', {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}}), ('content_block_delta', {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'OK'}}), ('content_block_stop', {'type': 'content_block_stop', 'index': 0}), ('message_delta', {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn', 'stop_sequence': None}, 'usage': {'output_tokens': 1}}), ('message_stop', {'type': 'message_stop'})]
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.end_headers()
        for kind, data in events:
            self.wfile.write(('event: ' + kind + '\ndata: ' + json.dumps(data) + '\n\n').encode())
        self.wfile.flush()
provider = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
threading.Thread(target=provider.serve_forever, daemon=True).start()
sid = str(uuid.uuid4())
settings = {'hooks': {'Stop': [{'hooks': [{'type': 'command', 'command': str(ROOT / 'target/debug/interlink-mcp') + ' wait --renew-after-secs 3', 'async': True, 'asyncRewake': True, 'timeout': 30}]}]}}
plugin = WORK / 'plugin'
(plugin / '.claude-plugin').mkdir(parents=True, exist_ok=True)
(plugin / 'hooks').mkdir(exist_ok=True)
(plugin / '.claude-plugin/plugin.json').write_text(json.dumps(json.loads((ROOT / 'plugin/.claude-plugin/plugin.json').read_text())))
hooks = json.loads((ROOT / 'plugin/hooks/hooks.json').read_text())
(plugin / 'hooks/hooks.json').write_text(json.dumps({'hooks': {'PostToolUse': hooks['hooks']['PostToolUse']}}))
key = WORK / 'id.key'
if not key.exists():
    subprocess.run([str(ROOT / 'target/debug/interlink-keygen'), '--out', str(key)], check=True, stdout=subprocess.DEVNULL)
(WORK / 'peers.json').write_text('{}')
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
url = f'http://127.0.0.1:{port}'
buslog = open(WORK / 'bus.stderr', 'w')
bus = subprocess.Popen([str(ROOT / 'target/debug/interlink-bus'), '--addr', f'127.0.0.1:{port}'], stdout=subprocess.DEVNULL, stderr=buslog)
(plugin / '.mcp.json').write_text(json.dumps({'mcpServers': {'interlink': {
    'command': str(ROOT / 'target/debug/interlink-mcp'),
    'args': ['--key', str(key), '--peers', str(WORK/'peers.json'), '--url', url],
    'env': {'XDG_STATE_HOME': str(WORK/'state'), 'INTERLINK_CHANNELS':'0'}
}}}))
(WORK / 'settings.json').write_text(json.dumps(settings))
env = os.environ.copy()
env.update({'CLAUDE_CONFIG_DIR': str(WORK / 'config'), 'ANTHROPIC_BASE_URL': f'http://127.0.0.1:{provider.server_port}', 'ANTHROPIC_API_KEY': 'interlink-local-fixture', 'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC': '1', 'XDG_STATE_HOME': str(WORK / 'state'), 'INTERLINK_CHANNELS': '0'})
env.pop('CLAUDECODE', None)
args = ['claude', '--print', '--input-format', 'stream-json', '--output-format', 'stream-json', '--verbose', '--no-session-persistence', '--session-id', sid, '--setting-sources', '', '--settings', str(WORK / 'settings.json'), '--plugin-dir', str(plugin), '--strict-mcp-config', '--mcp-config', json.dumps({'mcpServers': {'plugin:interlink:interlink': json.loads((plugin/'.mcp.json').read_text())['mcpServers']['interlink']}}), '--tools', '', '--model', 'claude-sonnet-4-6']
log = open(WORK / 'stderr.log', 'w')
p = subprocess.Popen(args, cwd=WORK, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True)
q = queue.Queue()
observed = []

def reader():
    for line in p.stdout:
        try:
            q.put(json.loads(line))
        except ValueError:
            pass
threading.Thread(target=reader, daemon=True).start()
try:
    p.stdin.write(json.dumps({'type': 'user', 'message': {'role': 'user', 'content': 'Reply OK.'}}) + '\n')
    p.stdin.flush()
    end = time.monotonic() + 40
    injected = False
    while time.monotonic() < end:
        try:
            event = q.get(timeout=0.25)
            observed.append(event)
            print('event:', event.get('type'), event.get('subtype', ''), flush=True)
        except queue.Empty:
            pass
        renewal_seen = any(('interlink listener renewal' in json.dumps(r) for r in REQUESTS))
        if renewal_seen and (not injected):
            path = WORK / 'state/interlink/inbox' / f'{sid}.jsonl'
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('a') as f:
                f.write(json.dumps({'sender': 'fixture-peer', 'msg_id': 'after-renewal', 'content': 'Message after listener renewal'}) + '\n')
            injected = True
            print('renewal reached host; injected follow-up message', flush=True)
        if injected and any(('Message after listener renewal' in json.dumps(r) for r in REQUESTS)):
            print('PASS: real Claude host renewed listener and received a later peer message', flush=True)
            roster = json.load(urllib.request.urlopen(url + '/roster', timeout=2))['roster']
            session = next(a['session'] for a in roster if a['session']['session_id'] == sid)
            assert 'title' not in session, session
            print('PASS: real Claude host registers without title hooks', flush=True)
            break
        if p.poll() is not None:
            raise RuntimeError('Claude exited ' + str(p.returncode))
    else:
        raise AssertionError('renewal not observed; local provider calls=' + str(len(REQUESTS)))

finally:
    (WORK / 'events.json').write_text(json.dumps(observed))
    (WORK / 'requests.json').write_text(json.dumps(REQUESTS))
    p.terminate()
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    log.close()
    provider.shutdown()
    bus.terminate()
    bus.wait(timeout=5)
    buslog.close()
