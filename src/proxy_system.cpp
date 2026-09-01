#include "async_proxy/proxy_system.h"

#include <asio/as_tuple.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/ssl.hpp>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <exception>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

using namespace std;

namespace async_proxy {
namespace {
    using tcp = asio::ip::tcp;
    using TlsStream = asio::ssl::stream<tcp::socket>;

    struct ByteLease {
        ByteLease(atomic<size_t>& used, size_t limit)
            : used_(used), limit_(limit) {}

        ~ByteLease() {
            if (reserved_ != 0)
                used_.fetch_sub(reserved_);
        }

        ByteLease(const ByteLease&) = delete;
        ByteLease& operator=(const ByteLease&) = delete;

        bool grow(size_t bytes) {
            if (bytes == 0)
                return true;

            size_t observed = used_.load();
            while (observed <= limit_ && bytes <= limit_ - observed) {
                if (used_.compare_exchange_weak(observed, observed + bytes)) {
                    reserved_ += bytes;
                    return true;
                }
            }
            return false;
        }

    private:
        atomic<size_t>& used_;
        size_t limit_;
        size_t reserved_{0};
    };

    struct ReadRequestResult {
        asio::error_code error;
        HttpRequest request;
        string error_message;
        bool too_large{false};
        bool resource_exhausted{false};
    };

    struct UpstreamResult {
        asio::error_code error;
        string response;
        string error_message;
        bool timed_out{false};
    };

    template <typename Stream>
    asio::awaitable<ReadRequestResult> read_request(
        Stream& stream,
        const ProxyConfig& config,
        const shared_ptr<ByteLease>& lease
    ) {
        array<char, 16 * 1024> buffer{};
        string input;
        input.reserve(min<size_t>(config.max_request_size, 64 * 1024));

        while (input.size() <= config.max_request_size) {
            auto [ec, bytes_read] = co_await stream.async_read_some(
                asio::buffer(buffer),
                asio::as_tuple(asio::use_awaitable)
            );

            if (bytes_read != 0) {
                if (bytes_read > config.max_request_size - min(config.max_request_size, input.size())) {
                    co_return ReadRequestResult{
                        asio::error::message_size, {}, "HTTP request is too large", true, false
                    };
                }
                if (!lease->grow(bytes_read)) {
                    co_return ReadRequestResult{
                        asio::error::no_buffer_space, {}, "proxy in-flight byte budget is exhausted", false, true
                    };
                }

                input.append(buffer.data(), bytes_read);
                auto parsed = parse_http_request(
                    input,
                    config.max_request_size,
                    config.max_header_size
                );
                switch (parsed.status) {
                    case HttpParseStatus::Complete:
                        co_return ReadRequestResult{{}, move(parsed.request), {}, false, false};
                    case HttpParseStatus::Invalid:
                        co_return ReadRequestResult{
                            asio::error::invalid_argument, {}, move(parsed.error), false, false
                        };
                    case HttpParseStatus::TooLarge:
                        co_return ReadRequestResult{
                            asio::error::message_size, {}, move(parsed.error), true, false
                        };
                    case HttpParseStatus::Incomplete:
                        break;
                }
            }

            if (ec) {
                co_return ReadRequestResult{
                    ec, {}, input.empty() ? ec.message() : "incomplete HTTP request", false, false
                };
            }
        }

        co_return ReadRequestResult{
            asio::error::message_size, {}, "HTTP request is too large", true, false
        };
    }

    template <typename Stream>
    asio::awaitable<UpstreamResult> read_response(
        Stream& stream,
        const ProxyConfig& config,
        const shared_ptr<ByteLease>& lease
    ) {
        array<char, 16 * 1024> buffer{};
        string input;
        input.reserve(min<size_t>(config.max_response_size, 64 * 1024));

        while (input.size() <= config.max_response_size) {
            auto [ec, bytes_read] = co_await stream.async_read_some(
                asio::buffer(buffer),
                asio::as_tuple(asio::use_awaitable)
            );

            if (bytes_read != 0) {
                if (bytes_read > config.max_response_size - min(config.max_response_size, input.size())) {
                    co_return UpstreamResult{
                        asio::error::message_size, {}, "upstream response is too large", false
                    };
                }
                if (!lease->grow(bytes_read)) {
                    co_return UpstreamResult{
                        asio::error::no_buffer_space, {}, "proxy in-flight byte budget is exhausted", false
                    };
                }
                input.append(buffer.data(), bytes_read);

                const auto framing = inspect_http_response(
                    input,
                    config.max_response_size,
                    config.max_header_size
                );
                if (framing.status == HttpResponseStatus::Complete)
                    co_return UpstreamResult{{}, move(input), {}, false};
                if (framing.status == HttpResponseStatus::Invalid) {
                    co_return UpstreamResult{
                        asio::error::invalid_argument, {}, framing.error, false
                    };
                }
                if (framing.status == HttpResponseStatus::TooLarge) {
                    co_return UpstreamResult{
                        asio::error::message_size, {}, framing.error, false
                    };
                }
            }

            if (ec) {
                const bool eof = ec == asio::error::eof ||
                    ec == asio::ssl::error::stream_truncated;
                if (eof && !input.empty()) {
                    const auto framing = inspect_http_response(
                        input,
                        config.max_response_size,
                        config.max_header_size
                    );
                    if (framing.status == HttpResponseStatus::Complete ||
                        framing.status == HttpResponseStatus::CloseDelimited) {
                        co_return UpstreamResult{{}, move(input), {}, false};
                    }
                    co_return UpstreamResult{
                        asio::error::invalid_argument,
                        {},
                        framing.error.empty() ? "upstream closed with an incomplete response" : framing.error,
                        false
                    };
                }
                co_return UpstreamResult{ec, {}, ec.message(), false};
            }
        }

        co_return UpstreamResult{
            asio::error::message_size, {}, "upstream response is too large", false
        };
    }

    string upstream_authority(const ProxyConfig& config) {
        const bool default_port =
            (!config.upstream_tls && config.upstream_port == "80") ||
            (config.upstream_tls && config.upstream_port == "443");
        return default_port
            ? config.upstream_host
            : config.upstream_host + ":" + config.upstream_port;
    }

    string log_prefix(LogLevel level) {
        switch (level) {
            case LogLevel::Info: return "INFO";
            case LogLevel::Warning: return "WARN";
            case LogLevel::Error: return "ERROR";
        }
        return "INFO";
    }
}

class ProxySystem::Impl {
public:
    Impl(ProxyConfig config, ProxyHandlers handlers)
        : config_(move(config)),
          handlers_(move(handlers)),
          upstream_tls_context_(asio::ssl::context::tls_client),
          downstream_tls_context_(asio::ssl::context::tls_server) {
        validate_config();
        configure_upstream_tls();
        configure_downstream_tls();
    }

    ~Impl() {
        stop();
    }

    void start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true))
            return;

        try {
            io_.restart();
            work_guard_.emplace(asio::make_work_guard(io_));
            request_queue_ = make_unique<RequestQueue>(
                io_.get_executor(),
                max<size_t>(1, config_.queue_capacity)
            );

            if (config_.http_port != 0) {
                http_acceptor_ = make_acceptor(config_.http_port);
                spawn(accept_loop(http_acceptor_, false), "HTTP accept loop");
            }
            if (config_.https_port != 0) {
                https_acceptor_ = make_acceptor(config_.https_port);
                spawn(accept_loop(https_acceptor_, true), "HTTPS accept loop");
            }

            for (size_t i = 0; i < config_.worker_count; ++i)
                spawn(worker_loop(), "proxy worker");

            threads_.reserve(config_.io_thread_count);
            for (size_t i = 0; i < config_.io_thread_count; ++i) {
                threads_.emplace_back([this] {
                    try {
                        io_.run();
                    } catch (const exception& error) {
                        log(LogLevel::Error, string("I/O thread failed: ") + error.what());
                    } catch (...) {
                        log(LogLevel::Error, "I/O thread failed with an unknown exception");
                    }
                });
            }

            log(LogLevel::Info, "async proxy started");
        } catch (...) {
            running_.store(false);
            if (request_queue_)
                request_queue_->close();
            work_guard_.reset();
            io_.stop();
            for (auto& thread : threads_) {
                if (thread.joinable())
                    thread.join();
            }
            threads_.clear();
            request_queue_.reset();
            throw;
        }
    }

    void stop() {
        if (!running_.exchange(false))
            return;

        if (request_queue_)
            request_queue_->close();
        work_guard_.reset();
        io_.stop();

        for (auto& thread : threads_) {
            if (thread.joinable())
                thread.join();
        }
        threads_.clear();
        http_acceptor_.reset();
        https_acceptor_.reset();
        request_queue_.reset();
        log(LogLevel::Info, "async proxy stopped");
    }

    bool is_running() const noexcept {
        return running_.load();
    }

private:
    using ResponseChannel =
        asio::experimental::concurrent_channel<void(asio::error_code, ProxyResponse)>;

    struct Job {
        ProxyRequest request;
        shared_ptr<ResponseChannel> response_channel;
        shared_ptr<ByteLease> byte_lease;
    };

    using JobPtr = shared_ptr<Job>;
    using RequestQueue =
        asio::experimental::concurrent_channel<void(asio::error_code, JobPtr)>;
    using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;

    struct SessionLease {
        explicit SessionLease(atomic<size_t>& count) : count_(count) {}
        ~SessionLease() { count_.fetch_sub(1); }
        atomic<size_t>& count_;
    };

    ProxyConfig config_;
    ProxyHandlers handlers_;
    asio::io_context io_;
    optional<WorkGuard> work_guard_;
    unique_ptr<RequestQueue> request_queue_;
    shared_ptr<tcp::acceptor> http_acceptor_;
    shared_ptr<tcp::acceptor> https_acceptor_;
    asio::ssl::context upstream_tls_context_;
    asio::ssl::context downstream_tls_context_;
    vector<thread> threads_;
    atomic<bool> running_{false};
    atomic<size_t> active_sessions_{0};
    atomic<size_t> inflight_bytes_{0};
    atomic<uint64_t> next_request_id_{1};
    mutex log_mutex_;

    void validate_config() const {
        if (config_.http_port == 0 && config_.https_port == 0)
            throw invalid_argument("at least one listener port must be enabled");
        if (config_.upstream_host.empty() || config_.upstream_port.empty())
            throw invalid_argument("upstream host and port are required");
        if (config_.io_thread_count == 0 || config_.worker_count == 0 ||
            config_.queue_capacity == 0 || config_.max_sessions == 0 ||
            config_.max_inflight_bytes == 0 || config_.max_request_size == 0 ||
            config_.max_response_size == 0 || config_.max_header_size == 0) {
            throw invalid_argument("proxy limits and worker counts must be greater than zero");
        }
        if (config_.https_port != 0 &&
            (config_.certificate_file.empty() || config_.private_key_file.empty())) {
            throw invalid_argument("HTTPS listener requires certificate and private key files");
        }
    }

    void configure_upstream_tls() {
        upstream_tls_context_.set_options(
            asio::ssl::context::default_workarounds |
            asio::ssl::context::no_sslv2 |
            asio::ssl::context::no_sslv3 |
            asio::ssl::context::no_tlsv1 |
            asio::ssl::context::no_compression
        );

        if (config_.upstream_insecure) {
            upstream_tls_context_.set_verify_mode(asio::ssl::verify_none);
        } else {
            upstream_tls_context_.set_verify_mode(asio::ssl::verify_peer);
            if (!config_.upstream_ca_file.empty())
                upstream_tls_context_.load_verify_file(config_.upstream_ca_file);
            else
                upstream_tls_context_.set_default_verify_paths();
        }
        SSL_CTX_set_mode(upstream_tls_context_.native_handle(), SSL_MODE_RELEASE_BUFFERS);
    }

    void configure_downstream_tls() {
        if (config_.https_port == 0)
            return;
        if (!filesystem::exists(config_.certificate_file) ||
            !filesystem::exists(config_.private_key_file)) {
            throw invalid_argument("HTTPS certificate or private key file does not exist");
        }

        downstream_tls_context_.set_options(
            asio::ssl::context::default_workarounds |
            asio::ssl::context::no_sslv2 |
            asio::ssl::context::no_sslv3 |
            asio::ssl::context::no_tlsv1 |
            asio::ssl::context::single_dh_use
        );
        downstream_tls_context_.use_certificate_chain_file(config_.certificate_file);
        downstream_tls_context_.use_private_key_file(
            config_.private_key_file,
            asio::ssl::context::pem
        );
        SSL_CTX_set_session_cache_mode(
            downstream_tls_context_.native_handle(),
            SSL_SESS_CACHE_SERVER
        );
        SSL_CTX_sess_set_cache_size(downstream_tls_context_.native_handle(), 64);
        SSL_CTX_set_timeout(downstream_tls_context_.native_handle(), 60);
        SSL_CTX_set_mode(downstream_tls_context_.native_handle(), SSL_MODE_RELEASE_BUFFERS);
    }

    shared_ptr<tcp::acceptor> make_acceptor(uint16_t port) {
        asio::error_code ec;
        const auto address = asio::ip::make_address(config_.listen_address, ec);
        if (ec)
            throw invalid_argument("invalid listen address: " + ec.message());

        auto acceptor = make_shared<tcp::acceptor>(io_);
        const tcp::endpoint endpoint(address, port);
        acceptor->open(endpoint.protocol(), ec);
        if (!ec)
            acceptor->set_option(tcp::acceptor::reuse_address(true), ec);
        if (!ec)
            acceptor->bind(endpoint, ec);
        if (!ec)
            acceptor->listen(asio::socket_base::max_listen_connections, ec);
        if (ec)
            throw runtime_error("failed to open listener on port " + to_string(port) + ": " + ec.message());
        return acceptor;
    }

    template <typename Awaitable>
    void spawn(Awaitable awaitable, string operation) {
        asio::co_spawn(
            io_,
            move(awaitable),
            [this, operation = move(operation)](exception_ptr error) {
                if (!error)
                    return;
                try {
                    rethrow_exception(error);
                } catch (const exception& ex) {
                    log(LogLevel::Error, operation + " failed: " + ex.what());
                } catch (...) {
                    log(LogLevel::Error, operation + " failed with an unknown exception");
                }
            }
        );
    }

    void log(LogLevel level, string_view message) {
        if (handlers_.log) {
            handlers_.log(level, message);
            return;
        }
        lock_guard lock(log_mutex_);
        cerr << '[' << log_prefix(level) << "] " << message << '\n';
    }

    bool try_acquire_session() {
        size_t observed = active_sessions_.load();
        while (observed < config_.max_sessions) {
            if (active_sessions_.compare_exchange_weak(observed, observed + 1))
                return true;
        }
        return false;
    }

    asio::awaitable<void> accept_loop(shared_ptr<tcp::acceptor> acceptor, bool tls) {
        while (running_.load()) {
            auto [ec, socket] = co_await acceptor->async_accept(
                asio::as_tuple(asio::use_awaitable)
            );
            if (ec) {
                if (!running_.load() || ec == asio::error::operation_aborted ||
                    ec == asio::error::bad_descriptor) {
                    co_return;
                }
                log(LogLevel::Warning, "accept failed; retrying: " + ec.message());
                asio::steady_timer timer(io_, config_.accept_retry_delay);
                auto [timer_ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
                if (timer_ec)
                    co_return;
                continue;
            }

            if (!try_acquire_session()) {
                log(LogLevel::Warning, "session limit reached; rejecting connection");
                asio::error_code ignored;
                socket.close(ignored);
                continue;
            }

            if (tls) {
                auto stream = make_shared<TlsStream>(move(socket), downstream_tls_context_);
                spawn(tls_session(move(stream)), "HTTPS session");
            } else {
                auto stream = make_shared<tcp::socket>(move(socket));
                spawn(plain_session(move(stream)), "HTTP session");
            }
        }
    }

    ProxyRequest make_proxy_request(
        HttpRequest request,
        const tcp::endpoint& endpoint,
        bool tls
    ) {
        ProxyRequest proxy_request;
        proxy_request.id = next_request_id_.fetch_add(1);
        proxy_request.http = move(request);
        proxy_request.client_address = endpoint.address().to_string();
        proxy_request.client_port = endpoint.port();
        proxy_request.tls = tls;
        return proxy_request;
    }

    asio::awaitable<ProxyResponse> enqueue_and_wait(
        ProxyRequest request,
        shared_ptr<ByteLease> byte_lease,
        asio::any_io_executor executor
    ) {
        auto response_channel = make_shared<ResponseChannel>(executor, 1);
        auto job = make_shared<Job>();
        job->request = move(request);
        job->response_channel = response_channel;
        job->byte_lease = move(byte_lease);

        if (!request_queue_ ||
            !request_queue_->try_send(asio::error_code{}, move(job))) {
            co_return ProxyResponse{
                make_error_response(503, "Service Unavailable", "proxy request queue is full")
            };
        }

        auto timeout = make_shared<asio::steady_timer>(executor, config_.response_timeout);
        asio::co_spawn(
            executor,
            [timeout, response_channel]() -> asio::awaitable<void> {
                auto [ec] = co_await timeout->async_wait(asio::as_tuple(asio::use_awaitable));
                if (!ec)
                    response_channel->try_send(asio::error::timed_out, ProxyResponse{});
            },
            asio::detached
        );

        auto [ec, response] = co_await response_channel->async_receive(
            asio::as_tuple(asio::use_awaitable)
        );
        timeout->cancel();
        response_channel->close();
        if (ec) {
            co_return ProxyResponse{
                make_error_response(504, "Gateway Timeout", "proxy response timed out")
            };
        }
        co_return response;
    }

    asio::awaitable<void> plain_session(shared_ptr<tcp::socket> socket) {
        SessionLease session_lease(active_sessions_);
        auto byte_lease = make_shared<ByteLease>(inflight_bytes_, config_.max_inflight_bytes);

        asio::error_code endpoint_ec;
        const tcp::endpoint endpoint = socket->remote_endpoint(endpoint_ec);
        auto timed_out = make_shared<atomic<bool>>(false);
        auto timeout = make_shared<asio::steady_timer>(io_, config_.request_timeout);
        timeout->async_wait([socket, timed_out](const asio::error_code& ec) {
            if (ec)
                return;
            timed_out->store(true);
            asio::error_code ignored;
            socket->cancel(ignored);
            socket->close(ignored);
        });

        auto read = co_await read_request(*socket, config_, byte_lease);
        timeout->cancel();
        if (timed_out->load())
            co_return;

        ProxyResponse response;
        if (read.error) {
            const int status = read.too_large ? 413 : (read.resource_exhausted ? 503 : 400);
            response.raw_http = make_error_response(
                status,
                status == 413 ? "Payload Too Large" :
                    (status == 503 ? "Service Unavailable" : "Bad Request"),
                read.error_message
            );
        } else {
            response = co_await enqueue_and_wait(
                make_proxy_request(move(read.request), endpoint, false),
                byte_lease,
                socket->get_executor()
            );
        }

        if (!response.raw_http.empty()) {
            auto [write_ec, bytes] = co_await asio::async_write(
                *socket,
                asio::buffer(response.raw_http),
                asio::as_tuple(asio::use_awaitable)
            );
            (void)bytes;
            if (write_ec)
                log(LogLevel::Warning, "client response write failed: " + write_ec.message());
        }

        asio::error_code ignored;
        socket->shutdown(tcp::socket::shutdown_both, ignored);
        socket->close(ignored);
    }

    asio::awaitable<void> tls_session(shared_ptr<TlsStream> stream) {
        SessionLease session_lease(active_sessions_);
        auto byte_lease = make_shared<ByteLease>(inflight_bytes_, config_.max_inflight_bytes);

        asio::error_code endpoint_ec;
        const tcp::endpoint endpoint = stream->next_layer().remote_endpoint(endpoint_ec);
        auto handshake_timed_out = make_shared<atomic<bool>>(false);
        auto handshake_timeout = make_shared<asio::steady_timer>(io_, config_.handshake_timeout);
        handshake_timeout->async_wait([stream, handshake_timed_out](const asio::error_code& ec) {
            if (ec)
                return;
            handshake_timed_out->store(true);
            asio::error_code ignored;
            stream->next_layer().cancel(ignored);
            stream->next_layer().close(ignored);
        });

        auto [handshake_ec] = co_await stream->async_handshake(
            asio::ssl::stream_base::server,
            asio::as_tuple(asio::use_awaitable)
        );
        handshake_timeout->cancel();
        if (handshake_ec || handshake_timed_out->load()) {
            if (!handshake_timed_out->load())
                log(LogLevel::Warning, "TLS handshake failed: " + handshake_ec.message());
            co_return;
        }

        auto read_timed_out = make_shared<atomic<bool>>(false);
        auto read_timeout = make_shared<asio::steady_timer>(io_, config_.request_timeout);
        read_timeout->async_wait([stream, read_timed_out](const asio::error_code& ec) {
            if (ec)
                return;
            read_timed_out->store(true);
            asio::error_code ignored;
            stream->next_layer().cancel(ignored);
            stream->next_layer().close(ignored);
        });

        auto read = co_await read_request(*stream, config_, byte_lease);
        read_timeout->cancel();
        if (read_timed_out->load())
            co_return;

        ProxyResponse response;
        if (read.error) {
            const int status = read.too_large ? 413 : (read.resource_exhausted ? 503 : 400);
            response.raw_http = make_error_response(
                status,
                status == 413 ? "Payload Too Large" :
                    (status == 503 ? "Service Unavailable" : "Bad Request"),
                read.error_message
            );
        } else {
            response = co_await enqueue_and_wait(
                make_proxy_request(move(read.request), endpoint, true),
                byte_lease,
                stream->get_executor()
            );
        }

        if (!response.raw_http.empty()) {
            auto [write_ec, bytes] = co_await asio::async_write(
                *stream,
                asio::buffer(response.raw_http),
                asio::as_tuple(asio::use_awaitable)
            );
            (void)bytes;
            if (write_ec)
                log(LogLevel::Warning, "TLS client response write failed: " + write_ec.message());
        }

        auto shutdown_timeout = make_shared<asio::steady_timer>(io_, config_.tls_shutdown_timeout);
        shutdown_timeout->async_wait([stream](const asio::error_code& ec) {
            if (ec)
                return;
            asio::error_code ignored;
            stream->next_layer().cancel(ignored);
            stream->next_layer().close(ignored);
        });
        auto [shutdown_ec] = co_await stream->async_shutdown(asio::as_tuple(asio::use_awaitable));
        (void)shutdown_ec;
        shutdown_timeout->cancel();
        asio::error_code ignored;
        stream->next_layer().close(ignored);
    }

    asio::awaitable<void> worker_loop() {
        while (running_.load()) {
            if (!request_queue_)
                co_return;
            auto [ec, job] = co_await request_queue_->async_receive(
                asio::as_tuple(asio::use_awaitable)
            );
            if (ec || !job)
                co_return;

            ProxyResponse response;
            try {
                if (handlers_.request_preflight) {
                    auto immediate = co_await handlers_.request_preflight(job->request);
                    if (immediate)
                        response = move(*immediate);
                }

                if (response.raw_http.empty()) {
                    if (handlers_.request_transform)
                        handlers_.request_transform(job->request);

                    auto upstream = co_await send_upstream(job->request, job->byte_lease);
                    if (upstream.error) {
                        const int status = upstream.timed_out ? 504 : 502;
                        response.raw_http = make_error_response(
                            status,
                            upstream.timed_out ? "Gateway Timeout" : "Bad Gateway",
                            upstream.error_message.empty() ? "upstream request failed" : upstream.error_message
                        );
                        log(LogLevel::Warning, "upstream request failed: " + upstream.error_message);
                    } else {
                        response.raw_http = move(upstream.response);
                    }
                }

                if (handlers_.response_transform)
                    handlers_.response_transform(job->request, response);
            } catch (const exception& error) {
                response.raw_http = make_error_response(500, "Internal Server Error", "proxy handler failed");
                log(LogLevel::Error, string("proxy handler failed: ") + error.what());
            } catch (...) {
                response.raw_http = make_error_response(500, "Internal Server Error", "proxy handler failed");
                log(LogLevel::Error, "proxy handler failed with an unknown exception");
            }

            job->response_channel->try_send(asio::error_code{}, move(response));
        }
    }

    asio::awaitable<UpstreamResult> send_upstream(
        const ProxyRequest& request,
        const shared_ptr<ByteLease>& byte_lease
    ) {
        string raw_request;
        try {
            raw_request = serialize_http_request(request.http, upstream_authority(config_));
        } catch (const exception& error) {
            co_return UpstreamResult{
                asio::error::invalid_argument, {}, error.what(), false
            };
        }

        if (!byte_lease->grow(raw_request.size())) {
            co_return UpstreamResult{
                asio::error::no_buffer_space, {}, "proxy in-flight byte budget is exhausted", false
            };
        }

        if (config_.upstream_tls)
            co_return co_await send_upstream_tls(move(raw_request), byte_lease);
        co_return co_await send_upstream_plain(move(raw_request), byte_lease);
    }

    asio::awaitable<UpstreamResult> send_upstream_plain(
        string request,
        shared_ptr<ByteLease> byte_lease
    ) {
        auto resolver = make_shared<tcp::resolver>(io_);
        auto socket = make_shared<tcp::socket>(io_);
        auto timed_out = make_shared<atomic<bool>>(false);
        auto timeout = make_shared<asio::steady_timer>(io_, config_.upstream_timeout);
        timeout->async_wait([resolver, socket, timed_out](const asio::error_code& ec) {
            if (ec)
                return;
            timed_out->store(true);
            resolver->cancel();
            asio::error_code ignored;
            socket->cancel(ignored);
            socket->close(ignored);
        });

        auto [resolve_ec, endpoints] = co_await resolver->async_resolve(
            config_.upstream_host,
            config_.upstream_port,
            asio::as_tuple(asio::use_awaitable)
        );
        if (resolve_ec) {
            timeout->cancel();
            co_return UpstreamResult{resolve_ec, {}, resolve_ec.message(), timed_out->load()};
        }

        auto [connect_ec, endpoint] = co_await asio::async_connect(
            *socket,
            endpoints,
            asio::as_tuple(asio::use_awaitable)
        );
        (void)endpoint;
        if (connect_ec) {
            timeout->cancel();
            co_return UpstreamResult{connect_ec, {}, connect_ec.message(), timed_out->load()};
        }

        auto [write_ec, bytes] = co_await asio::async_write(
            *socket,
            asio::buffer(request),
            asio::as_tuple(asio::use_awaitable)
        );
        (void)bytes;
        if (write_ec) {
            timeout->cancel();
            co_return UpstreamResult{write_ec, {}, write_ec.message(), timed_out->load()};
        }

        auto result = co_await read_response(*socket, config_, byte_lease);
        result.timed_out = timed_out->load();
        if (result.timed_out && !result.error)
            result.error = asio::error::timed_out;
        timeout->cancel();
        asio::error_code ignored;
        socket->close(ignored);
        co_return result;
    }

    asio::awaitable<UpstreamResult> send_upstream_tls(
        string request,
        shared_ptr<ByteLease> byte_lease
    ) {
        auto resolver = make_shared<tcp::resolver>(io_);
        auto stream = make_shared<TlsStream>(io_, upstream_tls_context_);
        auto timed_out = make_shared<atomic<bool>>(false);
        auto timeout = make_shared<asio::steady_timer>(io_, config_.upstream_timeout);
        timeout->async_wait([resolver, stream, timed_out](const asio::error_code& ec) {
            if (ec)
                return;
            timed_out->store(true);
            resolver->cancel();
            asio::error_code ignored;
            stream->next_layer().cancel(ignored);
            stream->next_layer().close(ignored);
        });

        auto fail = [&](asio::error_code ec) {
            timeout->cancel();
            return UpstreamResult{ec, {}, ec.message(), timed_out->load()};
        };

        auto [resolve_ec, endpoints] = co_await resolver->async_resolve(
            config_.upstream_host,
            config_.upstream_port,
            asio::as_tuple(asio::use_awaitable)
        );
        if (resolve_ec)
            co_return fail(resolve_ec);

        auto [connect_ec, endpoint] = co_await asio::async_connect(
            stream->next_layer(),
            endpoints,
            asio::as_tuple(asio::use_awaitable)
        );
        (void)endpoint;
        if (connect_ec)
            co_return fail(connect_ec);

        if (!SSL_set_tlsext_host_name(stream->native_handle(), config_.upstream_host.c_str())) {
            co_return fail(asio::error::operation_aborted);
        }
        if (!config_.upstream_insecure) {
            stream->set_verify_callback(
                asio::ssl::host_name_verification(config_.upstream_host)
            );
        }

        auto [handshake_ec] = co_await stream->async_handshake(
            asio::ssl::stream_base::client,
            asio::as_tuple(asio::use_awaitable)
        );
        if (handshake_ec)
            co_return fail(handshake_ec);

        auto [write_ec, bytes] = co_await asio::async_write(
            *stream,
            asio::buffer(request),
            asio::as_tuple(asio::use_awaitable)
        );
        (void)bytes;
        if (write_ec)
            co_return fail(write_ec);

        auto result = co_await read_response(*stream, config_, byte_lease);
        result.timed_out = timed_out->load();
        if (result.timed_out && !result.error)
            result.error = asio::error::timed_out;
        timeout->cancel();
        asio::error_code ignored;
        stream->next_layer().close(ignored);
        co_return result;
    }
};

ProxySystem::ProxySystem(ProxyConfig config, ProxyHandlers handlers)
    : impl_(make_unique<Impl>(move(config), move(handlers))) {}

ProxySystem::~ProxySystem() = default;

void ProxySystem::start() {
    impl_->start();
}

void ProxySystem::stop() {
    impl_->stop();
}

bool ProxySystem::is_running() const noexcept {
    return impl_->is_running();
}
}
