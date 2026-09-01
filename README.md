# Async Proxy

`async_proxy` is a resource-bounded HTTP/HTTPS reverse proxy for Embedded Linux.
It was extracted from a production gateway relay pipeline and generalized so the
networking core can be studied without domain-specific authentication, eBPF,
device-control, or firmware code.

## Highlights

- C++20 coroutines with standalone Asio
- HTTP and HTTPS listeners
- HTTP or HTTPS upstream
- Bounded request channel and configurable worker concurrency
- Global in-flight byte budget
- Per-listener session limit
- Strict HTTP/1.0 and HTTP/1.1 request framing
- Content-Length and chunked response framing
- TLS handshake, request, upstream, and response timeouts
- Reused OpenSSL contexts with bounded downstream session cache
- Retry after transient `accept()` failures
- Optional request preflight and request/response transform hooks
- Coordinated process shutdown on SIGINT or SIGTERM

The proxy intentionally uses one HTTP/1.1 request per downstream connection and
adds `Connection: close` to upstream requests. HTTP/2, WebSocket upgrades,
CONNECT tunneling, and HTTP pipelining are outside this example's scope.

## Architecture

```text
HTTP/HTTPS client
       |
       v
 listener coroutine
       |
       v
 strict HTTP parser + byte lease
       |
       v
 bounded request channel
       |
       v
 upstream worker coroutine
       |
       v
 HTTP/HTTPS upstream
       |
       v
 per-session response channel
       |
       v
 original client session
```

Each accepted session reserves bytes from a process-wide budget as request and
response data arrives. The lease is released when the client session and its
queued job are both complete. Queue and session limits reject overload instead
of allowing unbounded heap growth.

## Dependencies

- CMake 3.20 or later
- C++20 compiler
- standalone Asio
- OpenSSL
- pthreads/Threads

The CMake project searches these locations for standalone Asio:

1. `third_party/asio/include`
2. `../otac-gw/third_party/asio_1.38.0/include`
3. paths supplied through `ASIO_INCLUDE_DIR`

For a standalone public repository, vendor Asio under `third_party/asio` or pass
its installed include directory explicitly.

## Build

```sh
cmake -S . -B build \
  -DASIO_INCLUDE_DIR=/path/to/asio/include \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
bash ./tests/integration_test.sh ./build
```

The integration test additionally requires `curl`, `python3`, and the OpenSSL
command-line tools. It starts temporary plain HTTP and TLS upstream servers on
loopback addresses and removes all generated keys and logs on exit.

## Run

Start a plain HTTP proxy on port 8080 and forward to a local server on port 9000:

```sh
./build/async_proxy \
  --listen 0.0.0.0 \
  --http-port 8080 \
  --upstream-host 127.0.0.1 \
  --upstream-port 9000
```

Enable HTTPS on both sides:

```sh
./build/async_proxy \
  --http-port 0 \
  --https-port 8443 \
  --cert ./server.crt \
  --key ./server.key \
  --upstream-host api.example.test \
  --upstream-port 443 \
  --upstream-tls \
  --upstream-ca ./ca.crt
```

Use `--upstream-insecure` only in an isolated development environment.

## Extension Hooks

`ProxyHandlers` keeps application policy outside the networking pipeline:

```cpp
ProxyHandlers handlers;

handlers.request_preflight = [](ProxyRequest& request)
    -> asio::awaitable<std::optional<ProxyResponse>> {
    if (request.http.target == "/health") {
        co_return ProxyResponse{
            "HTTP/1.1 204 No Content\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n\r\n"
        };
    }
    co_return std::nullopt;
};

handlers.request_transform = [](ProxyRequest& request) {
    request.http.headers.push_back({"X-Proxy", "async-proxy"});
};
```

Authentication, rate limiting, routing, metrics, or domain-specific policy can
be added through these hooks without modifying listener and queue ownership.

## Security Boundaries

- Duplicate Content-Length is rejected.
- Content-Length plus Transfer-Encoding is rejected.
- Negative and overflowing Content-Length is rejected.
- HTTP headers must use CRLF line endings.
- HTTP/1.1 requires exactly one Host header.
- Hop-by-hop headers are removed before forwarding.
- Chunked requests are decoded and forwarded with a generated Content-Length.
- Incomplete messages at EOF are rejected.
- Upstream certificates and host names are verified by default.

## Portfolio Notes

Useful measurements for a public write-up include:

- concurrent clients before overload rejection
- average and P95 relay latency
- RSS before and after repeated TLS sessions
- stable file-descriptor count after a load test
- behavior when the request queue and byte budget are exhausted
- recovery after transient accept and upstream failures

Do not claim measured results until the test environment and commands are
included with the numbers. Add a license before publishing this as a standalone
repository.
