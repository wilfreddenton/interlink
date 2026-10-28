"""Exercise real Claude socket delivery with local model-response fixtures."""

import json
import os
import queue
import re
import signal
import shlex
import sys
import socket
import subprocess
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BIN = ROOT / 'target/debug'
REQUESTS = []


class Provider(BaseHTTPRequestHandler):
    behavior = "normal"

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
        if 'count_tokens' in self.path:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"input_tokens":1}')
            return
        REQUESTS.append(body)
        blocks = body['messages'][-1].get('content', [])
        serial = json.dumps(blocks)
        content = [{'type': 'text', 'text': 'OK'}]
        reason = 'end_turn'
        tools = {t['name']: t for t in body.get('tools', [])}
        receive = next((n for n in tools if n.endswith('receive_messages')), None)
        ack = next((n for n in tools if n.endswith('acknowledge_messages')), None)
        match = re.search(r'notification_id["\\\s:=]+([a-zA-Z0-9_-]+)', serial)
        notice = 'Interlink inbox notice' in serial
        recovery = 'Interlink has' in serial and 'unread messages' in serial
        should_fetch = (notice and self.behavior not in ['ignore', 'ignore_all']) or (recovery and self.behavior != 'ignore_all')
        if match and receive and should_fetch:
            content = [{'type': 'tool_use', 'id': 'tool_' + uuid.uuid4().hex,
                        'name': receive, 'input': {} if self.behavior == 'manual' else {'notification_id': match[1]}}]
            reason = 'tool_use'
        elif isinstance(blocks, list) and any(b.get('type') == 'tool_result' for b in blocks):
            texts = []
            for b in blocks:
                value = b.get('content', '')
                if isinstance(value, list):
                    texts.extend(c.get('text', '') for c in value)
                elif isinstance(value, str):
                    texts.append(value)
            text = '\n'.join(texts)
            receipts = []
            for raw in re.findall(r'Receipt: (\{[^\n]+\})', text):
                try:
                    receipts.append(json.loads(raw))
                except ValueError:
                    pass
            if receipts and ack:
                content = [{'type': 'tool_use', 'id': 'tool_' + uuid.uuid4().hex,
                            'name': ack, 'input': {'messages': receipts}}]
                reason = 'tool_use'
        msg = {'id': 'msg_' + uuid.uuid4().hex, 'type': 'message', 'role': 'assistant',
               'model': body.get('model', 'fixture'), 'content': [], 'stop_reason': None,
               'stop_sequence': None, 'usage': {'input_tokens': 1, 'output_tokens': 1}}
        events = [('message_start', {'type': 'message_start', 'message': msg})]
        for i, block in enumerate(content):
            start = dict(block)
            if block['type'] == 'text':
                start['text'] = ''
                delta = {'type': 'text_delta', 'text': block['text']}
            else:
                start['input'] = {}
                delta = {'type': 'input_json_delta', 'partial_json': json.dumps(block['input'])}
            events += [('content_block_start', {'type': 'content_block_start', 'index': i, 'content_block': start}),
                       ('content_block_delta', {'type': 'content_block_delta', 'index': i, 'delta': delta}),
                       ('content_block_stop', {'type': 'content_block_stop', 'index': i})]
        events += [('message_delta', {'type': 'message_delta', 'delta': {'stop_reason': reason, 'stop_sequence': None}, 'usage': {'output_tokens': 1}}),
                   ('message_stop', {'type': 'message_stop'})]
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.end_headers()
        for kind, data in events:
            self.wfile.write(('event: ' + kind + '\ndata: ' + json.dumps(data) + '\n\n').encode())
        self.wfile.flush()


def wait_for(check, label, timeout=35):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            print('PASS:', label, flush=True)
            return value
        time.sleep(.1)
    raise AssertionError(label)


class Rpc:
    def __init__(self, process):
        self.process = process
        self.responses = queue.Queue()
        self.next_id = 0
        threading.Thread(target=self.read, daemon=True).start()
        self.call('initialize', {'protocolVersion': '2025-06-18', 'capabilities': {},
                                 'clientInfo': {'name': 'socket-fixture', 'version': '1'}})
        self.write({'jsonrpc': '2.0', 'method': 'notifications/initialized'})

    def read(self):
        for line in self.process.stdout:
            self.responses.put(json.loads(line))

    def write(self, message):
        self.process.stdin.write(json.dumps(message) + '\n')
        self.process.stdin.flush()

    def call(self, method, params):
        self.next_id += 1
        self.write({'jsonrpc': '2.0', 'id': self.next_id, 'method': method, 'params': params})
        deadline = time.monotonic() + 15
        while True:
            reply = self.responses.get(timeout=max(.01, deadline - time.monotonic()))
            if reply.get('id') == self.next_id:
                assert 'error' not in reply, reply
                return reply['result']

    def tool(self, name, arguments):
        reply = self.call('tools/call', {'name': name, 'arguments': arguments})
        assert not reply.get('isError'), reply
        return reply


def run(policy, behavior="normal"):
    REQUESTS.clear()
    Provider.behavior = behavior
    work = ROOT / 'target/host-validation/claude-socket' / (policy + '-' + behavior + '-' + uuid.uuid4().hex[:8])
    work.mkdir(parents=True, mode=0o700)
    print('Diagnostics:', work, flush=True)
    processes = []
    logs = []
    events = []
    provider = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
    threading.Thread(target=provider.serve_forever, daemon=True).start()
    env = dict(os.environ, CLAUDE_CONFIG_DIR=str(work / 'config'),
               ANTHROPIC_BASE_URL=f'http://127.0.0.1:{provider.server_port}',
               ANTHROPIC_API_KEY='interlink-local-fixture',
               CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC='1',
               XDG_STATE_HOME=str(work / 'state'), INTERLINK_CHANNELS='0',
               INTERLINK_CLAUDE_DELIVERY='auto')
    for name in ['CLAUDECODE', 'CLAUDE_CODE_MESSAGING_SOCKET', 'CLAUDE_CODE_MESSAGING_TOKEN',
                 'CLAUDE_CODE_SESSION_ID', 'INTERLINK_SESSION',
                 'CLAUDE_CODE_OAUTH_TOKEN', 'ANTHROPIC_AUTH_TOKEN']:
        env.pop(name, None)

    def launch(args, label):
        log = open(work / (label + '.stderr'), 'w')
        logs.append(log)
        process = subprocess.Popen(list(map(str, args)), cwd=work, env=env,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=log, text=True, start_new_session=True)
        processes.append(process)
        return process

    try:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        url = f'http://127.0.0.1:{port}'
        subprocess.run([str(BIN / 'interlink-keygen'), '--out', str(work / 'id.key')],
                       check=True, stdout=subprocess.DEVNULL)
        (work / 'peers.json').write_text('{}')
        launch([BIN / 'interlink-bus', '--addr', f'127.0.0.1:{port}'], 'bus')

        def roster():
            try:
                return json.load(urllib.request.urlopen(url + '/roster', timeout=1))
            except OSError:
                return None

        wait_for(roster, 'isolated broker ready')
        common = ['--key', str(work / 'id.key'), '--peers', str(work / 'peers.json'), '--url', url]
        sender = Rpc(launch([BIN / 'interlink-mcp', *common, '--session', str(uuid.uuid4())], 'sender'))
        mcp = {'mcpServers': {'interlink': {'command': str(BIN / 'interlink-mcp'), 'args': common,
               'env': {'XDG_STATE_HOME': str(work / 'state'), 'INTERLINK_CHANNELS': '0',
                       'INTERLINK_CLAUDE_DELIVERY': 'auto'}}}}
        # Accelerate the legacy renewal: any failure to disable it starts extra turns.
        settings = {'hooks': {'Stop': [{'hooks': [{'type': 'command',
                    'command': str(BIN / 'interlink-mcp') + ' wait --renew-after-secs 1',
                    'async': True, 'asyncRewake': True, 'timeout': 30}]}]}}
        capture = work / 'capture-socket.py'
        socket_env = work / 'socket-env.json'
        capture.write_text('import json, os\nfrom pathlib import Path\n'
                           + 'Path(' + repr(str(socket_env)) + ').write_text(json.dumps({k: os.environ.get(k) for k in '
                           + repr(['CLAUDE_CODE_MESSAGING_SOCKET', 'CLAUDE_CODE_MESSAGING_TOKEN']) + '}))\n')
        capture_hook = {'type': 'command', 'command': shlex.quote(sys.executable) + ' ' + shlex.quote(str(capture))}
        bind_hook = {'type': 'mcp_tool', 'server': 'interlink', 'tool': 'bind_claude_session',
                     'input': {'session_id': '${session_id}', 'cwd': '${cwd}'}}
        def binding(event):
            hook = {**bind_hook, 'input': {**bind_hook['input'], 'hook_event_name': event}}
            if event == 'UserPromptSubmit':
                hook['input']['prompt'] = '${prompt}'
            return hook

        for event in ['SessionStart', 'UserPromptSubmit']:
            settings['hooks'][event] = [{'hooks': [binding(event)]}]
        settings['hooks']['Stop'].insert(0, {'hooks': [{'type': 'mcp_tool', 'server': 'interlink',
            'tool': 'check_claude_inbox', 'input': {'session_id': '${session_id}', 'stop_hook_active': '${stop_hook_active}'}}]})
        settings['hooks']['SessionStart'][0]['hooks'].insert(0, capture_hook)
        settings['hooks']['SessionEnd'] = [{'hooks': [{**bind_hook, 'tool': 'end_claude_session'}]}]
        settings['hooks']['PreToolUse'] = [{'matcher': '^mcp__interlink__', 'hooks': [binding('PreToolUse')]}]
        if policy == 'refuse':
            settings['crossSessionInbound'] = 'refuse'
        (work / 'settings.json').write_text(json.dumps(settings))
        sid = str(uuid.uuid4())
        claude = launch(['claude', '--print', '--input-format', 'stream-json', '--output-format', 'stream-json',
                         '--verbose', '--session-id', sid,
                         '--setting-sources', '', '--settings', work / 'settings.json',
                         '--strict-mcp-config', '--mcp-config', json.dumps(mcp), '--tools', '',
                         '--allowedTools', 'mcp__interlink__receive_messages', 'mcp__interlink__acknowledge_messages',
                         '--model', 'claude-sonnet-4-6', '--debug-file', work / 'claude.debug'], 'claude')

        def read_events():
            for line in claude.stdout:
                events.append(json.loads(line))

        threading.Thread(target=read_events, daemon=True).start()
        claude.stdin.write(json.dumps({'type': 'user', 'message': {'role': 'user', 'content': 'Reply OK.'}}) + '\n')
        claude.stdin.flush()
        wait_for(lambda: any(e.get('type') == 'result' for e in events), 'initial Claude turn completed')
        guidance = 'Interlink session summary:'
        assert json.dumps(REQUESTS[-1]['messages']).count(guidance) == 1, 'missing summary guidance'
        wait_for(lambda: any(a['session']['session_id'] == sid for a in (roster() or {}).get('roster', [])),
                 'Claude connected to real MCP without a compatibility shim')
        before = len(REQUESTS)
        marker = 'BROKER-SOCKET-' + uuid.uuid4().hex
        sender.tool('send_message', {'to': 'self', 'session': sid, 'text': marker})

        def mailbox():
            matches = list((work / 'state/interlink/mailbox').glob('**/' + sid + '.json'))
            return json.loads(matches[0].read_text()) if matches else {}

        if policy == 'refuse':
            wait_for(lambda: (mailbox().get('notice') or {}).get('state') == 'socket_sent', 'refused notice was written to socket')
            wait_for(lambda: 'refused inbound peer message' in (work / 'claude.debug').read_text(),
                     'Claude applies inbound refusal')
            time.sleep(3)
            assert len(REQUESTS) == before, 'refused notice or renewal started a turn'
            records = mailbox()['records']
            assert len(records) == 1 and records[0]['state'] != 'receiver_acknowledged'
            print('PASS: refusal leaves the body unread and does not start a model turn', flush=True)
        elif behavior == 'ignore_all':
            wait_for(lambda: len([e for e in events if e.get('type') == 'result']) >= 2,
                     'ignored notice ends after bounded Stop recovery')
            assert len(REQUESTS) == before + 2, 'expected exactly one recovery continuation'
            assert 'Interlink has 1 unread messages' in json.dumps(REQUESTS[-1]['messages'])
            assert mailbox()['notice']['host_started'] and not mailbox()['notice']['observed']
            assert mailbox()['records'][0]['state'] != 'receiver_acknowledged'
            calls = len(REQUESTS)
            time.sleep(4)
            assert len(REQUESTS) == calls, 'ignored recovery caused idle model requests'
            print('PASS: an ignored recovery remains unread without looping or duplicate socket notices', flush=True)
            claude.stdin.write(json.dumps({'type': 'user', 'message': {'role': 'user', 'content': '/compact'}}) + '\n')
            claude.stdin.flush()
            wait_for(lambda: any(e.get('subtype') == 'compact_boundary' for e in events),
                     'Claude compacts the conversation while the notice remains unread')
            # The fixture summary omits the notice ID. Recovery must use durable hook state.
            Provider.behavior = 'manual'
            claude.stdin.write(json.dumps({'type': 'user', 'message': {'role': 'user', 'content': 'Reply OK.'}}) + '\n')
            claude.stdin.flush()
            wait_for(lambda: any(r['message']['text'] == marker and r['state'] == 'receiver_acknowledged'
                                 for r in mailbox().get('records', [])), 'post-compaction Stop recovery fetches and acknowledges without the model retaining an ID')
            wait_for(lambda: not mailbox().get('notice'), 'post-compaction manual fetch retires the started notice')
            Provider.behavior = 'normal'
            later = 'AFTER-COMPACTION-' + uuid.uuid4().hex
            sender.tool('send_message', {'to': 'self', 'session': sid, 'text': later})
            wait_for(lambda: any(r['message']['text'] == later and r['state'] == 'receiver_acknowledged'
                                 for r in mailbox().get('records', [])), 'new arrivals wake Claude after compaction recovery')

        else:
            wait_for(lambda: any(marker in json.dumps(r['messages']) for r in REQUESTS),
                     'socket wakes idle Claude and real MCP fetch returns broker message')
            wait_for(lambda: any(r['message']['text'] == marker and r['state'] == 'receiver_acknowledged'
                                 for r in mailbox().get('records', [])), 'real MCP acknowledgement persists')
            wait_for(lambda: len([e for e in events if e.get('type') == 'result']) >= 2, 'delivery turn ends')
            calls = len(REQUESTS)
            time.sleep(4)
            assert len(REQUESTS) == calls, 'idle session started extra model requests'
            assert json.dumps(REQUESTS[-1]['messages']).count(guidance) == 1, 'summary reminder repeated'
            assert not (work / 'state/interlink/inbox' / (sid + '.jsonl')).exists()
            print('PASS: accelerated Stop hook stays disabled, no idle renewals or legacy inbox', flush=True)
        if policy == 'default' and behavior in ['manual', 'ignore']:
            if behavior == 'ignore':
                assert any('Interlink has 1 unread messages' in json.dumps(r['messages']) for r in REQUESTS)
            wait_for(lambda: not mailbox().get('notice'), 'started notice retires after fetch and acknowledgement')
            Provider.behavior = 'normal'
            later = 'AFTER-RECOVERY-' + uuid.uuid4().hex
            sender.tool('send_message', {'to': 'self', 'session': sid, 'text': later})
            wait_for(lambda: any(r['message']['text'] == later and r['state'] == 'receiver_acknowledged'
                                 for r in mailbox().get('records', [])), 'later message wakes Claude after recovery')
        if policy == 'default' and behavior == 'normal':
            claude.stdin.write(json.dumps({'type': 'user', 'message': {'role': 'user', 'content': '/clear'}}) + '\n')
            claude.stdin.flush()
            new_id = wait_for(lambda: next((e['session_id'] for e in events
                                           if e.get('session_id') and e['session_id'] != sid), None),
                              '/clear creates a different Claude conversation')
            wait_for(lambda: any(a['session']['session_id'] == new_id for a in (roster() or {}).get('roster', [])),
                     'SessionStart binds the new mailbox without a model prompt')
            assert not any(a['session']['session_id'] == sid for a in roster()['roster'])
            connection_info = json.loads(socket_env.read_text())
            calls = len(REQUESTS)
            with socket.socket(socket.AF_UNIX) as stale:
                stale.connect(connection_info['CLAUDE_CODE_MESSAGING_SOCKET'])
                stale.sendall((json.dumps({'type': 'auth', 'token': connection_info['CLAUDE_CODE_MESSAGING_TOKEN']}) + '\n'
                               + json.dumps({'type': 'user', 'session_id': sid,
                                             'message': {'role': 'user', 'content': 'STALE-CHAT-FRAME'}}) + '\n').encode())
                stale.shutdown(socket.SHUT_WR)
            wait_for(lambda: 'session_id mismatch' in (work / 'claude.debug').read_text(),
                     'Claude rejects an in-flight frame addressed to the old chat')
            assert len(REQUESTS) == calls, 'stale socket frame started a turn in the new chat'
            old_marker = 'OLD-CHAT-' + uuid.uuid4().hex
            sender.tool('send_message', {'to': 'self', 'session': sid, 'text': old_marker})
            new_marker = 'NEW-CHAT-' + uuid.uuid4().hex
            sender.tool('send_message', {'to': 'self', 'session': new_id, 'text': new_marker})
            wait_for(lambda: any(new_marker in json.dumps(r['messages']) for r in REQUESTS),
                     'new chat receives its own message after /clear')
            assert json.dumps(REQUESTS[-1]['messages']).count(guidance) == 1, 'fresh chat needs its own summary guidance'
            assert not any(old_marker in json.dumps(r['messages']) for r in REQUESTS)
            print('PASS: old chat messages do not enter the new conversation', flush=True)
            wait_for(lambda: any(r['message']['text'] == new_marker and r['state'] == 'receiver_acknowledged'
                                 for path in (work / 'state/interlink/mailbox').glob('**/' + new_id + '.json')
                                 for r in json.loads(path.read_text()).get('records', [])),
                     'new chat finishes acknowledging its own message')
            claude.stdin.close()
            claude.wait(timeout=10)
            claude = launch(['claude', '--print', '--input-format', 'stream-json', '--output-format', 'stream-json',
                             '--verbose', '--resume', sid,
                             '--setting-sources', '', '--settings', work / 'settings.json',
                             '--strict-mcp-config', '--mcp-config', json.dumps(mcp), '--tools', '',
                             '--allowedTools', 'mcp__interlink__receive_messages', 'mcp__interlink__acknowledge_messages',
                             '--model', 'claude-sonnet-4-6', '--debug-file', work / 'resume.debug'], 'claude-resume')
            threading.Thread(target=read_events, daemon=True).start()
            claude.stdin.write(json.dumps({'type': 'user', 'message': {'role': 'user', 'content': 'Reply OK.'}}) + '\n')
            claude.stdin.flush()
            wait_for(lambda: any(a['session']['session_id'] == sid for a in (roster() or {}).get('roster', [])),
                     '--resume rebinds the original conversation')
            wait_for(lambda: any(old_marker in json.dumps(r['messages']) for r in REQUESTS),
                     'resumed conversation receives its pending message')
            # Resumed history retains the original reminder; the new runtime adds one.
            assert json.dumps(REQUESTS[-1]['messages']).count(guidance) == 2, 'missing resume guidance'
            print('PASS: summary guidance appears once on binding, /clear, and resume', flush=True)
        debug = (work / 'claude.debug').read_text()
        assert 'Hook Stop (Stop) error' not in debug, 'Stop hook failed'
        assert 'Hook UserPromptSubmit (UserPromptSubmit) error' not in debug, 'prompt hook failed'
        assert claude.poll() is None
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        (work / 'socket-env.json').unlink(missing_ok=True)
        provider.shutdown()
        provider.server_close()
        for log in logs:
            log.close()
        (work / 'events.json').write_text(json.dumps(events))
        (work / 'requests.json').write_text(json.dumps(REQUESTS))


if __name__ == '__main__':
    for inbound in ['default', 'refuse']:
        run(inbound)
    for behavior in ['manual', 'ignore', 'ignore_all']:
        run('default', behavior)
