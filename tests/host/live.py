"""Opt-in delivery checks using the checkout, installed CLIs, and real model accounts."""

import json
import os
import queue
import shutil
import signal
import socket
import subprocess
import threading
import time
import tomllib
import urllib.request
import uuid
from pathlib import Path

try:
    from websockets.exceptions import ConnectionClosed
    from websockets.sync.client import unix_connect
except ImportError as error:
    raise SystemExit("Install tests/host/requirements-live.txt before running live tests") from error

ROOT = Path(__file__).resolve().parents[2]
WORK = ROOT / 'target/host-validation/live' / (time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6])
WORK.mkdir(parents=True)
BIN = ROOT / 'target/debug'
if not (BIN / 'interlink-mcp').is_file():
    raise SystemExit('Build Interlink first with cargo build --all-features --bins')
CODEX_TEST_HOME = WORK / 'codex-home'
CODEX_TEST_HOME.mkdir()
PROCESSES = []
LOGS = []


def launch(args, label, env=None):
    log = open(WORK / (label + '.stderr'), 'w')
    LOGS.append(log)
    p = subprocess.Popen([str(x) for x in args], cwd=WORK, env=env, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=log, text=True, start_new_session=True)
    PROCESSES.append(p)
    return p


class Rpc:
    def __init__(self, p, label, mcp=False):
        self.p = p
        self.q = queue.Queue()
        self.i = 0
        self.events = []
        self.log = open(WORK / (label + '.jsonl'), 'w')
        LOGS.append(self.log)
        threading.Thread(target=self.reader, daemon=True).start()
        params = {'clientInfo': {'name': 'interlink-live-validation', 'version': '1'}}
        if mcp:
            params.update(protocolVersion='2025-06-18', capabilities={})
        else:
            params['capabilities'] = {'experimentalApi': True}
        result = self.call('initialize', params)
        self.send({'jsonrpc': '2.0', 'method': 'notifications/initialized' if mcp else 'initialized'})
        if mcp:
            print('Checkout MCP:', result['serverInfo'], flush=True)

    def reader(self):
        for line in self.p.stdout:
            self.log.write(line)
            self.log.flush()
            try:
                self.q.put(json.loads(line))
            except ValueError:
                pass

    def send(self, value):
        self.p.stdin.write(json.dumps(value) + '\n')
        self.p.stdin.flush()

    def call(self, method, params, timeout=45):
        self.i += 1
        ident = self.i
        self.send({'jsonrpc': '2.0', 'id': ident, 'method': method, 'params': params})
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = self.q.get(timeout=max(.1, deadline - time.monotonic()))
            except queue.Empty as error:
                raise TimeoutError(method) from error
            if value.get('id') == ident:
                if 'error' in value:
                    raise RuntimeError((method, value['error']))
                return value['result']
            self.events.append(value)
        raise TimeoutError(method)

    def tool(self, name, arguments):
        result = self.call('tools/call', {'name': name, 'arguments': arguments})
        assert not result.get('isError'), result
        return '\n'.join(c['text'] for c in result['content'] if c['type'] == 'text')


def wait_for(check, label, timeout=150):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            print('PASS:', label, flush=True)
            return value
        time.sleep(.5)
    raise AssertionError(label)


def toml(value):
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(k) + '=' + toml(v) for k, v in value.items()) + '}'
    if isinstance(value, list):
        return '[' + ','.join(map(toml, value)) + ']'
    return json.dumps(value)


def main():
    home = CODEX_TEST_HOME
    source_home = Path(os.environ.get('CODEX_HOME', Path.home() / '.codex'))
    auth = source_home / 'auth.json'
    if not auth.is_file():
        raise RuntimeError('The live test needs Codex file-based login in ' + str(auth))
    (home / 'auth.json').symlink_to(auth)
    env = dict(os.environ, CODEX_HOME=str(home), XDG_STATE_HOME=str(WORK / 'state'),
               INTERLINK_CHANNELS='0', INTERLINK_CLAUDE_DELIVERY='auto')
    for name in ['CLAUDECODE', 'CLAUDE_CODE_MESSAGING_SOCKET', 'CLAUDE_CODE_MESSAGING_TOKEN',
                 'CLAUDE_CODE_SESSION_ID', 'INTERLINK_SESSION']:
        env.pop(name, None)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    url = f'http://127.0.0.1:{port}'
    subprocess.run([str(BIN/'interlink-keygen'), '--out', str(WORK/'id.key')], check=True, stdout=subprocess.DEVNULL)
    (WORK/'peers.json').write_text('{}')
    launch([BIN/'interlink-bus', '--addr', f'127.0.0.1:{port}', '--db', WORK/'bus.redb'], 'bus', env)
    def broker_ready():
        try:
            return json.load(urllib.request.urlopen(url + '/roster', timeout=2))
        except OSError:
            return False
    wait_for(broker_ready, 'isolated broker is ready', 10)
    args = ['--key', str(WORK/'id.key'), '--peers', str(WORK/'peers.json'), '--url', url]
    sender_id = str(uuid.uuid4())
    sender = Rpc(launch([BIN/'interlink-mcp', '--host', 'claude', *args, '--session', sender_id], 'sender', env), 'sender', True)
    local_config = tomllib.loads((source_home/'config.toml').read_text())
    config_text = '\n'.join([
        'model = ' + toml(local_config['model']),
        'model_reasoning_effort = ' + toml(local_config.get('model_reasoning_effort', 'high')),
        'approval_policy = "never"', 'sandbox_mode = "read-only"',
        '[features]', 'hooks = true', 'plugins = false', 'apps = false',
        (ROOT/'codex/config.toml').read_text().replace('command = "interlink-mcp"', 'command = ' + toml(str(BIN/'interlink-mcp'))).replace('args = ["--host", "codex"]', 'args = ' + toml(['--host', 'codex', *args])),
        '[mcp_servers.interlink.env]', 'CODEX_HOME = ' + toml(str(home)),
        'XDG_STATE_HOME = ' + toml(str(WORK/'state')),
        'INTERLINK_CODEX_BIN = ' + toml(shutil.which('codex')),
    ]) + '\n'
    # Approval applies only to these reviewed test tools, in the temporary home.
    for tool in ['get_my_session_id', 'send_message', 'receive_messages', 'acknowledge_messages', 'discover']:
        config_text += '\n[mcp_servers.interlink.tools.' + tool + ']\napproval_mode = "approve"\n'
    (home/'config.toml').write_text(config_text)
    probe = Rpc(launch(['codex', 'app-server'], 'hook-review', env), 'hook-review')
    inventory = probe.call('hooks/list', {'cwds': [str(WORK)]})
    hooks = [h for e in inventory['data'] for h in e['hooks'] if h.get('server') == 'interlink']
    assert len(hooks) == 4, inventory
    # Trust the exact local binding definitions, without changing global hook trust.
    for hook in hooks:
        config_text += '\n[hooks.state.' + toml(hook['key']) + ']\ntrusted_hash = ' + toml(hook['currentHash']) + '\n'
    (home/'config.toml').write_text(config_text)
    probe.p.stdin.close()
    probe.p.wait(timeout=10)
    subprocess.run(['codex', 'app-server', 'daemon', 'start'], env=env, cwd=WORK, check=True, timeout=30)
    class SocketProcess:
        def __init__(self):
            self.ws = unix_connect(str((home/'app-server-control/app-server-control.sock').resolve()))
            self.stdin = self.stdout = self
        def __iter__(self):
            try:
                for message in self.ws:
                    yield message + '\n'
            except ConnectionClosed:
                return
        def write(self, line):
            self.ws.send(line.strip())
        def flush(self):
            pass
    server = Rpc(SocketProcess(), 'codex')
    tid = server.call('thread/start', {'cwd': str(WORK), 'approvalPolicy': 'never', 'sandbox': 'read-only'})['thread']['id']
    server.call('mcpServerStatus/list', {'threadId': tid, 'serverName': 'interlink'})
    instruction = ('You are an Interlink delivery test participant. Use only Interlink MCP tools. '
        'Do not use shell, browse, change files, pair peers, or delegate. For each inbox notice fetch with its notification_id and acknowledge exact receipts before handling messages. '
        'A message containing ECHO:<token> requires one send_message reply to its sender session with text ACK:<token>. '
        'A message containing FORWARD:<session>:<token> requires sending ECHO:<token> to that explicit session using to=self. '
        'Do not reply to ACK messages. After handling a batch, end the turn. '
        'First call get_my_session_id and send READY to self session ' + sender_id + ', then end your turn.')
    server.call('turn/start', {'threadId': tid, 'input': [{'type': 'text', 'text': instruction}]})
    seen = []
    def drain():
        text = sender.tool('receive_messages', {})
        receipts = [json.loads(line.split('] Receipt: ', 1)[1]) for line in text.splitlines() if '] Receipt: ' in line]
        if receipts:
            sender.tool('acknowledge_messages', {'messages': receipts})
            seen.append(text)
            print('Controller received:', text[:500], flush=True)
        return '\n'.join(seen)
    sender_inbox = WORK/'state/interlink/inbox'/f'{sender_id}.jsonl'
    wait_for(lambda: sender_inbox.exists() and sender_inbox.stat().st_size > 0,
             'controller notice is queued before manual consumption')
    initial_notice = sender_inbox.read_text().splitlines()
    assert len(initial_notice) == 1, initial_notice
    wait_for(lambda: 'READY' in drain(), 'live Codex model registers and sends via checkout MCP')
    def codex_idle():
        while True:
            try:
                server.events.append(server.q.get_nowait())
            except queue.Empty:
                break
        turns = [event for event in server.events
                 if event.get('method') in ['turn/started', 'turn/completed']
                 and event.get('params', {}).get('threadId') == tid]
        return turns and turns[-1]['method'] == 'turn/completed'
    wait_for(codex_idle, 'Codex is idle before the wake-up message')
    sender.tool('send_message', {'to': 'self', 'session': tid, 'text': 'ECHO:codex-idle'})
    wait_for(lambda: 'ACK:codex-idle' in drain(), 'real codex queue wakes idle model, which fetches, acknowledges, and replies')

    plugin = WORK/'plugin'
    shutil.copytree(ROOT/'plugin', plugin)
    spec = {'command': str(BIN/'interlink-mcp'), 'args': ['--host', 'claude', *args], 'env': {'XDG_STATE_HOME': str(WORK/'state'), 'INTERLINK_CHANNELS': '0', 'INTERLINK_CLAUDE_DELIVERY': 'auto'}}
    (plugin/'.mcp.json').write_text(json.dumps({'mcpServers': {'interlink': spec}}))
    hooks = json.loads((plugin/'hooks/hooks.json').read_text())
    # A one-second legacy renewal makes accidental polling visible during the quiet check.
    hooks['hooks']['Stop'][0]['hooks'][0]['command'] = str(BIN/'interlink-mcp') + ' wait --renew-after-secs 1'
    (plugin/'hooks/hooks.json').write_text(json.dumps(hooks))
    claude_id = str(uuid.uuid4())
    claude_env = dict(env)
    claude = launch(['claude', '--print', '--input-format', 'stream-json', '--output-format', 'stream-json', '--verbose',
        '--no-session-persistence', '--session-id', claude_id, '--setting-sources', '', '--plugin-dir', plugin,
        '--debug-file', WORK/'claude.debug',
        '--strict-mcp-config', '--mcp-config', json.dumps({'mcpServers': {'plugin:interlink:interlink': spec}}),
        '--tools', '', '--allowedTools', ','.join('mcp__plugin_interlink_interlink__' + name for name in
        ['get_my_session_id', 'send_message', 'receive_messages', 'acknowledge_messages', 'discover']),
        '--permission-mode', 'dontAsk'], 'claude', claude_env)
    claude_log = open(WORK/'claude.jsonl', 'w')
    LOGS.append(claude_log)
    claude_events = []
    def claude_reader():
        for line in claude.stdout:
            claude_log.write(line)
            claude_log.flush()
            claude_events.append(json.loads(line))
    def claude_turn_events():
        # Hook lifecycle events can arrive after result without starting another model turn.
        return [event for event in claude_events if event.get('type') in ['assistant', 'user', 'result']]
    def claude_idle():
        events = claude_turn_events()
        return events and events[-1]['type'] == 'result'
    threading.Thread(target=claude_reader, daemon=True).start()
    seen.clear()
    claude.stdin.write(json.dumps({'type': 'user', 'message': {'role': 'user', 'content': instruction}})+'\n')
    claude.stdin.flush()
    wait_for(lambda: 'READY' in drain(), 'live Claude model registers and sends via checkout MCP')
    wait_for(claude_idle, 'Claude is idle before the wake-up message')
    sender.tool('send_message', {'to': 'self', 'session': claude_id, 'text': 'ECHO:claude-idle'})
    wait_for(lambda: 'ACK:claude-idle' in drain(), 'Claude delivery wakes idle model, which fetches, acknowledges, and replies')
    for i in range(6):
        for session in [tid, claude_id]:
            sender.tool('send_message', {'to': 'self', 'session': session, 'text': f'ECHO:burst-{session[:8]}-{i}'})
    def burst_complete():
        received = drain()
        return all(f'ACK:burst-{session[:8]}-{i}' in received
                   for session in [tid, claude_id] for i in range(6))
    wait_for(burst_complete, 'both live models consume and reply to all burst messages', 240)
    sender.tool('send_message', {'to': 'self', 'session': tid, 'text': f'FORWARD:{claude_id}:codex-to-claude'})
    sender.tool('send_message', {'to': 'self', 'session': claude_id, 'text': f'FORWARD:{tid}:claude-to-codex'})
    def consumed_cross_messages():
        mailboxes = {}
        for path in (WORK/'state/interlink').glob('mailbox/**/*.json'):
            mailboxes[path.stem] = json.loads(path.read_text()).get('records', [])
        return all(
            any(r['message']['text'] == 'ACK:' + token and r['state'] == 'receiver_acknowledged'
                for r in mailboxes.get(recipient, []))
            for recipient, token in [(tid, 'codex-to-claude'), (claude_id, 'claude-to-codex')]
        )
    wait_for(consumed_cross_messages, 'bidirectional live Claude/Codex conversation acknowledged', 240)
    wait_for(claude_idle, 'Claude finishes the exchange')
    before = len(claude_turn_events())
    time.sleep(10)
    assert len(claude_turn_events()) == before, 'idle Claude started additional model turns'
    assert claude.poll() is None, 'Claude exited instead of remaining available'
    assert not (WORK/'state/interlink/inbox'/f'{claude_id}.jsonl').exists(), 'Claude used legacy inbox delivery'
    print('PASS: socket delivery stayed quiet for ten seconds with one-second legacy renewal configured', flush=True)
    assert sender_inbox.read_text().splitlines() == initial_notice, 'manual consumption accumulated notices'
    print('PASS: manual fetch/ack kept one outstanding notice throughout the live exchange', flush=True)
    summary = {'codex_session': tid, 'claude_session': claude_id,
               'codex_version': subprocess.check_output(['codex', '--version'], text=True).strip(),
               'claude_version': subprocess.check_output(['claude', '--version'], text=True).strip(),
               'interlink_version': subprocess.check_output([str(BIN/'interlink-mcp'), '--version'], text=True).strip(),
               'interlink_binary': str(BIN/'interlink-mcp'),
               'checks': ['idle_codex', 'idle_claude', 'six_message_bursts', 'bidirectional_exchange', 'socket_idle_quiet', 'manual_consumption_notice_coalescing']}
    (WORK/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    server.p.ws.close()
    print('ALL LIVE CHECKS PASSED', flush=True)


try:
    print('Diagnostics:', WORK, flush=True)
    main()
finally:
    for p in reversed(PROCESSES):
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        if p.poll() is None:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()
    home = CODEX_TEST_HOME
    if home.exists():
        try:
            subprocess.run(['codex', 'app-server', 'daemon', 'stop'], env=dict(os.environ, CODEX_HOME=str(home)),
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        finally:
            (home/'auth.json').unlink(missing_ok=True)
            shutil.rmtree(home)
    for log in LOGS:
        log.close()
