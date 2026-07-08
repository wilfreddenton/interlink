#!/usr/bin/env bash
# Reproducible round-trip demo (no Claude instances required): shows the bus,
# the armed signal, the Stop-hook decision, and an MCP send arriving at a
# background listener. Handy for recording an asciinema/GIF.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release"
CA="$ROOT/certs/ca.pem"
URL="https://localhost:9443"

[ -x "$BIN/duet-bus" ] || { echo "build first: cargo build --release"; exit 1; }

echo "▶ starting duet-bus"
"$BIN/duet-bus" >/tmp/duet-demo.log 2>&1 &
BUS=$!
trap 'kill $BUS 2>/dev/null' EXIT
sleep 1

echo "▶ armed(alice) before any listener:"
curl -s --cacert "$CA" "$URL/armed?me=alice"; echo

echo "▶ Stop hook with no listener → blocks:"
echo '{}' | DUET_SELF=alice DUET_URL="$URL" DUET_CA="$CA" "$BIN/duet-liveness" | head -c 120; echo '...'

echo "▶ arming a background listener for alice"
curl -s --cacert "$CA" "$URL/recv?me=alice&timeout_ms=8000" >/tmp/duet-recv.out &
sleep 1
echo "▶ armed(alice) now:"; curl -s --cacert "$CA" "$URL/armed?me=alice"; echo
echo "▶ Stop hook with listener armed → allows (empty):"
echo '{}' | DUET_SELF=alice DUET_URL="$URL" DUET_CA="$CA" "$BIN/duet-liveness"; echo "(no output = allowed)"

echo "▶ bob sends to alice via the MCP tool (real JSON-RPC through duet-chat)"
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"demo","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"send_message","arguments":{"to":"alice","text":"hello from bob"}}}' \
  | DUET_SELF=bob DUET_URL="$URL" DUET_CA="$CA" timeout 4 "$BIN/duet-chat" >/dev/null 2>&1 || true

sleep 1
echo "▶ alice's listener received:"
cat /tmp/duet-recv.out; echo
echo "✓ done"
