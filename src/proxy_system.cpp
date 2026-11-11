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
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

namespace async_proxy {
namespace {
    using tcp = asio::ip::tcp;
    using TlsStream = asio::ssl::stream<tcp::socket>;
    using Clock = chrono::steady_clock;

    class Deadline {
    public:
        template <typename OnTimeout>
        Deadline(asio::any_io_executor executor, Clock::time_point expiry, OnTimeout on_timeout)
            : timer_(executor, expiry), armed_(make_shared<bool>(true)) {
            timer_.async_wait([armed = armed_, on_timeout = move(on_timeout)](const asio::error_code& ec) {
                if (!ec && *armed)
                    on_timeout();
            });
        }

        ~Deadline() {
            // A ready timer handler can still run after the operation returns.
            // The operation and the timer handler run on the same strand.
            *armed_ = false;
        }

        bool expired() const {
            return Clock::now() >= timer_.expiry();
        }

    private:
        asio::steady_timer timer_;
        shared_ptr<bool> armed_;
    };

    void close_socket(tcp::socket& socket) {
        asio::error_code ignored;
        socket.close(ignored);
    }

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
        const shared_ptr<ByteLease>& lease,
        string_view request_method
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
                    config.max_header_size,
                    request_method
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
                if ((ec == asio::error::eof || ec == asio::ssl::error::stream_truncated) &&
                    !input.empty()) {
                    const auto framing = inspect_http_response(
                        input,
                        config.max_response_size,
                        config.max_header_size,
                        request_method
                    );
                    if (framing.status == HttpResponseStatus::Complete ||
                        (ec == asio::error::eof && framing.status == HttpResponseStatus::CloseDelimited)) {
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
        unique_lock lock(lifecycle_mutex_);
        if (running_.load())
            return;

        try {
            auto http_acceptor = make_acceptor(config_.http_port);
            auto https_acceptor = make_acceptor(config_.https_port);
            io_.restart();
            work_guard_.emplace(asio::make_work_guard(io_));
            request_queue_ = make_unique<RequestQueue>(
                io_.get_executor(),
                max<size_t>(1, config_.queue_capacity)
            );

            running_.store(true);
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

            start_listener(move(http_acceptor), false);
            start_listener(move(https_acceptor), true);
            for (size_t i = 0; i < config_.worker_count; ++i)
                spawn(asio::make_strand(io_), worker_loop(), "proxy worker");
        } catch (...) {
            shutdown();
            throw;
        }
        lock.unlock();
        log(LogLevel::Info, "async proxy started");
    }

    void stop() {
        unique_lock lock(lifecycle_mutex_);
        if (!running_.load())
            return;

        shutdown();
        lock.unlock();
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
        Clock::time_point deadline;

        bool expired() const { return Clock::now() >= deadline; }
    };

    using JobPtr = shared_ptr<Job>;
    using RequestQueue =
        asio::experimental::concurrent_channel<void(asio::error_code, JobPtr)>;
    using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;

    struct SessionLease {
        explicit SessionLease(atomic<size_t>& count) : count_(&count) {}
        SessionLease(SessionLease&& other) noexcept : count_(exchange(other.count_, nullptr)) {}
        ~SessionLease() {
            if (count_)
                count_->fetch_sub(1);
        }
        atomic<size_t>* count_;
    };

    ProxyConfig config_;
    ProxyHandlers handlers_;
    asio::io_context io_;
    optional<WorkGuard> work_guard_;
    unique_ptr<RequestQueue> request_queue_;
    asio::ssl::context upstream_tls_context_;
    asio::ssl::context downstream_tls_context_;
    vector<thread> threads_;
    mutex lifecycle_mutex_;
    mutex tasks_mutex_;
    unordered_map<shared_ptr<asio::cancellation_signal>, asio::any_io_executor> tasks_;
    atomic<bool> running_{false};
    atomic<size_t> active_sessions_{0};
    atomic<size_t> inflight_bytes_{0};
    atomic<uint64_t> next_request_id_{1};
    mutex log_mutex_;

    void shutdown() {
        running_.store(false);
        if (request_queue_)
            request_queue_->close();

        decltype(tasks_) tasks;
        {
            lock_guard lock(tasks_mutex_);
            tasks.swap(tasks_);
        }
        for (const auto& [signal, executor] : tasks) {
            asio::post(executor, [signal] { signal->emit(asio::cancellation_type::terminal); });
        }

        work_guard_.reset();
        for (auto& thread : threads_)
            thread.join();
        threads_.clear();
        request_queue_.reset();
    }

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

    optional<tcp::acceptor> make_acceptor(uint16_t port) {
        if (port == 0)
            return nullopt;

        asio::error_code ec;
        const auto address = asio::ip::make_address(config_.listen_address, ec);
        if (ec)
            throw invalid_argument("invalid listen address: " + ec.message());

        tcp::acceptor acceptor(asio::make_strand(io_));
        const tcp::endpoint endpoint(address, port);
        acceptor.open(endpoint.protocol(), ec);
        if (!ec)
            acceptor.set_option(tcp::acceptor::reuse_address(true), ec);
        if (!ec)
            acceptor.bind(endpoint, ec);
        if (!ec)
            acceptor.listen(asio::socket_base::max_listen_connections, ec);
        if (ec)
            throw runtime_error("failed to open listener on port " + to_string(port) + ": " + ec.message());
        return acceptor;
    }

    void start_listener(optional<tcp::acceptor> acceptor, bool tls) {
        if (!acceptor)
            return;
        const auto executor = acceptor->get_executor();
        spawn(executor, accept_loop(move(*acceptor), tls), tls ? "HTTPS accept loop" : "HTTP accept loop");
    }

    void spawn(asio::any_io_executor executor, asio::awaitable<void> work, string operation) {
        asio::post(executor, [this, executor, work = move(work), operation = move(operation)]() mutable {
            auto signal = make_shared<asio::cancellation_signal>();
            {
                lock_guard lock(tasks_mutex_);
                if (!running_.load())
                    return;
                tasks_.emplace(signal, executor);
            }

            auto complete = [this, signal, operation = move(operation)](exception_ptr error) {
                {
                    lock_guard lock(tasks_mutex_);
                    tasks_.erase(signal);
                }
                if (!error)
                    return;
                try {
                    rethrow_exception(error);
                } catch (const asio::system_error& ex) {
                    if (running_.load() || ex.code() != asio::error::operation_aborted)
                        log(LogLevel::Error, operation + " failed: " + ex.what());
                } catch (const exception& ex) {
                    log(LogLevel::Error, operation + " failed: " + ex.what());
                } catch (...) {
                    log(LogLevel::Error, operation + " failed with an unknown exception");
                }
            };
            try {
                asio::co_spawn(executor, move(work), asio::bind_cancellation_slot(signal->slot(), complete));
            } catch (...) {
                complete(current_exception());
            }
        });
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

    asio::awaitable<void> accept_loop(tcp::acceptor acceptor, bool tls) {
        while (running_.load()) {
            asio::any_io_executor executor = asio::make_strand(io_);
            auto [ec, socket] = co_await acceptor.async_accept(
                executor,
                asio::as_tuple(asio::use_awaitable)
            );
            if (ec) {
                if (!running_.load() || ec == asio::error::operation_aborted ||
                    ec == asio::error::bad_descriptor) {
                    co_return;
                }
                log(LogLevel::Warning, "accept failed; retrying: " + ec.message());
                asio::steady_timer timer(acceptor.get_executor(), config_.accept_retry_delay);
                auto [timer_ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
                if (timer_ec)
                    co_return;
                continue;
            }

            if (!try_acquire_session()) {
                log(LogLevel::Warning, "session limit reached; rejecting connection");
                continue;
            }

            spawn(executor, session(move(socket), SessionLease(active_sessions_), tls),
                  tls ? "HTTPS session" : "HTTP session");
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
        const bool include_body = request.http.method != "HEAD";
        const auto expires_at = Clock::now() + config_.response_timeout;
        auto response_channel = make_shared<ResponseChannel>(executor, 1);
        auto job = make_shared<Job>();
        job->request = move(request);
        job->response_channel = response_channel;
        job->byte_lease = move(byte_lease);
        job->deadline = expires_at;

        if (!request_queue_->try_send(asio::error_code{}, move(job))) {
            co_return ProxyResponse{
                make_error_response(503, "Service Unavailable", "proxy request queue is full", include_body)
            };
        }

        Deadline deadline(executor, expires_at, [response_channel] {
            response_channel->try_send(asio::error::timed_out, ProxyResponse{});
        });

        auto [ec, response] = co_await response_channel->async_receive(
            asio::as_tuple(asio::use_awaitable)
        );
        response_channel->close();
        if (ec || deadline.expired()) {
            co_return ProxyResponse{
                make_error_response(504, "Gateway Timeout", "proxy response timed out", include_body)
            };
        }
        co_return response;
    }

    template <typename Stream>
    asio::awaitable<bool> serve_session(Stream& stream, tcp::socket& socket, bool tls) {
        auto byte_lease = make_shared<ByteLease>(inflight_bytes_, config_.max_inflight_bytes);
        const auto executor = stream.get_executor();
        asio::error_code endpoint_ec;
        const tcp::endpoint endpoint = socket.remote_endpoint(endpoint_ec);
        ReadRequestResult read;
        {
            Deadline deadline(executor, Clock::now() + config_.request_timeout, [&socket] {
                close_socket(socket);
            });
            read = co_await read_request(stream, config_, byte_lease);
            if (deadline.expired() || !running_.load())
                co_return false;
        }

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
                make_proxy_request(move(read.request), endpoint, tls),
                byte_lease,
                executor
            );
        }

        if (!running_.load())
            co_return false;
        if (response.raw_http.empty())
            co_return true;

        Deadline deadline(executor, Clock::now() + config_.response_write_timeout, [&socket] {
            close_socket(socket);
        });
        auto [write_ec, bytes] = co_await asio::async_write(
            stream,
            asio::buffer(response.raw_http),
            asio::as_tuple(asio::use_awaitable)
        );
        (void)bytes;
        if (!running_.load())
            co_return false;
        if (deadline.expired()) {
            log(LogLevel::Warning, "client response write timed out");
            co_return false;
        }
        if (write_ec)
            log(LogLevel::Warning, "client response write failed: " + write_ec.message());
        co_return !write_ec;
    }

    asio::awaitable<void> session(tcp::socket socket, [[maybe_unused]] SessionLease lease, bool tls) {
        if (!tls) {
            if (co_await serve_session(socket, socket, false)) {
                asio::error_code ignored;
                socket.shutdown(tcp::socket::shutdown_both, ignored);
            }
            co_return;
        }

        TlsStream stream(move(socket), downstream_tls_context_);
        const auto executor = stream.get_executor();
        auto& transport = stream.next_layer();
        auto close = [&transport] { close_socket(transport); };
        {
            Deadline deadline(executor, Clock::now() + config_.handshake_timeout, close);
            auto [ec] = co_await stream.async_handshake(
                asio::ssl::stream_base::server,
                asio::as_tuple(asio::use_awaitable)
            );
            if (deadline.expired() || !running_.load())
                co_return;
            if (ec) {
                log(LogLevel::Warning, "TLS handshake failed: " + ec.message());
                co_return;
            }
        }

        if (co_await serve_session(stream, transport, true)) {
            Deadline deadline(executor, Clock::now() + config_.tls_shutdown_timeout, close);
            co_await stream.async_shutdown(asio::as_tuple(asio::use_awaitable));
        }
    }

    asio::awaitable<void> worker_loop() {
        while (running_.load()) {
            auto [ec, job] = co_await request_queue_->async_receive(
                asio::as_tuple(asio::use_awaitable)
            );
            if (ec || !job || !running_.load())
                co_return;
            if (job->expired())
                continue;

            const bool include_body = job->request.http.method != "HEAD";
            ProxyResponse response;
            try {
                if (handlers_.request_preflight) {
                    auto immediate = co_await handlers_.request_preflight(job->request);
                    if (immediate)
                        response = move(*immediate);
                }
                if (job->expired() || !running_.load())
                    continue;

                if (response.raw_http.empty()) {
                    if (handlers_.request_transform)
                        handlers_.request_transform(job->request);

                    if (job->expired() || !running_.load())
                        continue;
                    auto upstream = co_await send_upstream(*job);
                    if (job->expired() || !running_.load())
                        continue;
                    if (upstream.error) {
                        const int status = upstream.timed_out ? 504 : 502;
                        response.raw_http = make_error_response(
                            status,
                            upstream.timed_out ? "Gateway Timeout" : "Bad Gateway",
                            upstream.error_message.empty() ? "upstream request failed" : upstream.error_message,
                            include_body
                        );
                        log(LogLevel::Warning, "upstream request failed: " + upstream.error_message);
                    } else {
                        response.raw_http = move(upstream.response);
                    }
                }

                if (handlers_.response_transform && !job->expired() && running_.load())
                    handlers_.response_transform(job->request, response);
            } catch (const exception& error) {
                if (!running_.load())
                    co_return;
                response.raw_http = make_error_response(500, "Internal Server Error", "proxy handler failed", include_body);
                log(LogLevel::Error, string("proxy handler failed: ") + error.what());
            } catch (...) {
                if (!running_.load())
                    co_return;
                response.raw_http = make_error_response(500, "Internal Server Error", "proxy handler failed", include_body);
                log(LogLevel::Error, "proxy handler failed with an unknown exception");
            }

            job->response_channel->try_send(asio::error_code{}, move(response));
        }
    }

    asio::awaitable<UpstreamResult> send_upstream(const Job& job) {
        const auto expires_at = min(job.deadline, Clock::now() + config_.upstream_timeout);
        string raw_request;
        try {
            raw_request = serialize_http_request(job.request.http, upstream_authority(config_));
        } catch (const exception& error) {
            co_return UpstreamResult{
                asio::error::invalid_argument, {}, error.what(), false
            };
        }

        if (!job.byte_lease->grow(raw_request.size())) {
            co_return UpstreamResult{
                asio::error::no_buffer_space, {}, "proxy in-flight byte budget is exhausted", false
            };
        }

        const auto executor = co_await asio::this_coro::executor;
        if (config_.upstream_tls) {
            TlsStream stream(executor, upstream_tls_context_);
            co_return co_await exchange_upstream(stream, stream.next_layer(), raw_request, job, expires_at);
        }
        tcp::socket socket(executor);
        co_return co_await exchange_upstream(socket, socket, raw_request, job, expires_at);
    }

    template <typename Stream>
    asio::awaitable<UpstreamResult> exchange_upstream(
        Stream& stream,
        tcp::socket& socket,
        const string& request,
        const Job& job,
        Clock::time_point expires_at
    ) {
        const auto executor = stream.get_executor();
        tcp::resolver resolver(executor);
        Deadline deadline(executor, expires_at, [&resolver, &socket] {
            resolver.cancel();
            close_socket(socket);
        });

        auto fail = [&](asio::error_code ec) {
            if (deadline.expired())
                return UpstreamResult{asio::error::timed_out, {}, "upstream request timed out", true};
            return UpstreamResult{ec, {}, ec.message(), false};
        };
        if (deadline.expired())
            co_return fail(asio::error::timed_out);

        auto [resolve_ec, endpoints] = co_await resolver.async_resolve(
            config_.upstream_host,
            config_.upstream_port,
            asio::as_tuple(asio::use_awaitable)
        );
        if (resolve_ec || deadline.expired())
            co_return fail(resolve_ec);

        auto [connect_ec, endpoint] = co_await asio::async_connect(
            socket,
            endpoints,
            asio::as_tuple(asio::use_awaitable)
        );
        (void)endpoint;
        if (connect_ec || deadline.expired())
            co_return fail(connect_ec);

        if constexpr (is_same_v<Stream, TlsStream>) {
            if (!SSL_set_tlsext_host_name(stream.native_handle(), config_.upstream_host.c_str()))
                co_return fail(asio::error::operation_aborted);
            if (!config_.upstream_insecure)
                stream.set_verify_callback(asio::ssl::host_name_verification(config_.upstream_host));

            auto [handshake_ec] = co_await stream.async_handshake(
                asio::ssl::stream_base::client,
                asio::as_tuple(asio::use_awaitable)
            );
            if (handshake_ec || deadline.expired())
                co_return fail(handshake_ec);
        }

        if (!running_.load())
            co_return fail(asio::error::operation_aborted);
        if (deadline.expired())
            co_return fail(asio::error::timed_out);
        auto [write_ec, bytes] = co_await asio::async_write(
            stream,
            asio::buffer(request),
            asio::as_tuple(asio::use_awaitable)
        );
        (void)bytes;
        if (write_ec || deadline.expired())
            co_return fail(write_ec);

        auto result = co_await read_response(stream, config_, job.byte_lease, job.request.http.method);
        if (deadline.expired())
            co_return fail(asio::error::timed_out);
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
