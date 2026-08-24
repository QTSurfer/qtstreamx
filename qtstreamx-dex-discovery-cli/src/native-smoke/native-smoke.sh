#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: native-smoke.sh <native-cli>" >&2
  exit 64
fi

cli="$1"

protocols_output="$("$cli" protocols --output json)"
if ! grep -Eq '"schemaVersion"[[:space:]]*:[[:space:]]*1' <<<"$protocols_output" \
    || ! grep -Eq '"alias"[[:space:]]*:[[:space:]]*"uniswap"' <<<"$protocols_output"; then
  echo "native protocols JSON contract failed" >&2
  exit 1
fi

set +e
capture_error="$("$cli" uniswap capture --network ethereum --version v3 --start-block 24000000 --out events.csv 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640 2>&1 >/dev/null)"
capture_status=$?
set -e

if [ "$capture_status" -ne 2 ]; then
  echo "native capture validation returned $capture_status, expected 2" >&2
  exit 1
fi
case "$capture_error" in
  *QTSTREAMX_EVM_HTTP_URL*);;
  *)
    echo "native capture validation did not report the missing active HTTP endpoint" >&2
    exit 1
    ;;
esac
case "$capture_error" in
  *http://*|*https://*|*ws://*|*wss://*)
    echo "native capture validation exposed an endpoint" >&2
    exit 1
    ;;
esac
