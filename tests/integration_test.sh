#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 BUILD_DIRECTORY" >&2
    exit 2
fi

build_dir=$(realpath "$1")
proxy_bin="$build_dir/async_proxy"
if [[ ! -x "$proxy_bin" ]]; then
    echo "async_proxy executable not found: $proxy_bin" >&2
    exit 2
fi

for command in curl openssl python3; do
    command -v "$command" >/dev/null || {
        echo "required command not found: $command" >&2
        exit 2
    }
done

temp_dir=$(mktemp -d)
pids=()

cleanup() {
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    rm -rf -- "$temp_dir"
}
trap cleanup EXIT

free_port_pair() {
    python3 - <<'PY'
import socket

with socket.socket() as first, socket.socket() as second:
    first.bind(("127.0.0.1", 0))
    second.bind(("127.0.0.1", 0))
    print(first.getsockname()[1], second.getsockname()[1])
PY
}

read -r plain_upstream_port plain_proxy_port < <(free_port_pair)

python3 -m http.server "$plain_upstream_port" \
    --bind 127.0.0.1 \
    --directory "$temp_dir" \
    >"$temp_dir/plain-upstream.log" 2>&1 &
pids+=("$!")

"$proxy_bin" \
    --listen 127.0.0.1 \
    --http-port "$plain_proxy_port" \
    --upstream-host 127.0.0.1 \
    --upstream-port "$plain_upstream_port" \
    >"$temp_dir/plain-proxy.log" 2>&1 &
pids+=("$!")

sleep 1
curl --fail --silent --show-error \
    "http://127.0.0.1:$plain_proxy_port/" \
    --output "$temp_dir/plain-response"
test -s "$temp_dir/plain-response"

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$temp_dir/server.key" \
    -out "$temp_dir/server.crt" \
    -days 1 \
    -subj /CN=localhost \
    >"$temp_dir/certificate.log" 2>&1

read -r tls_upstream_port tls_proxy_port < <(free_port_pair)

openssl s_server \
    -accept "127.0.0.1:$tls_upstream_port" \
    -cert "$temp_dir/server.crt" \
    -key "$temp_dir/server.key" \
    -WWW \
    -quiet \
    >"$temp_dir/tls-upstream.log" 2>&1 &
pids+=("$!")

"$proxy_bin" \
    --listen 127.0.0.1 \
    --http-port 0 \
    --https-port "$tls_proxy_port" \
    --cert "$temp_dir/server.crt" \
    --key "$temp_dir/server.key" \
    --upstream-host 127.0.0.1 \
    --upstream-port "$tls_upstream_port" \
    --upstream-tls \
    --upstream-insecure \
    >"$temp_dir/tls-proxy.log" 2>&1 &
pids+=("$!")

sleep 1
curl --fail --insecure --silent --show-error \
    "https://127.0.0.1:$tls_proxy_port/" \
    --output "$temp_dir/tls-response"
test -s "$temp_dir/tls-response"

echo "plain HTTP and TLS relay integration tests passed"
