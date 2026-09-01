#ifndef ASYNC_PROXY_PROXY_SYSTEM_H
#define ASYNC_PROXY_PROXY_SYSTEM_H

#include "async_proxy/http.h"

#include <asio.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace async_proxy {
    enum class LogLevel {
        Info,
        Warning,
        Error,
    };

    struct ProxyConfig {
        std::string listen_address{"0.0.0.0"};
        std::uint16_t http_port{8080};
        std::uint16_t https_port{0};
        std::string certificate_file;
        std::string private_key_file;

        std::string upstream_host{"127.0.0.1"};
        std::string upstream_port{"9000"};
        bool upstream_tls{false};
        bool upstream_insecure{false};
        std::string upstream_ca_file;

        std::size_t io_thread_count{2};
        std::size_t worker_count{2};
        std::size_t queue_capacity{1024};
        std::size_t max_sessions{128};
        std::size_t max_inflight_bytes{32 * 1024 * 1024};
        std::size_t max_request_size{4 * 1024 * 1024};
        std::size_t max_response_size{4 * 1024 * 1024};
        std::size_t max_header_size{64 * 1024};

        std::chrono::milliseconds handshake_timeout{5000};
        std::chrono::milliseconds request_timeout{10000};
        std::chrono::milliseconds upstream_timeout{10000};
        std::chrono::milliseconds response_timeout{15000};
        std::chrono::milliseconds tls_shutdown_timeout{1000};
        std::chrono::milliseconds accept_retry_delay{100};
    };

    struct ProxyRequest {
        std::uint64_t id{0};
        HttpRequest http;
        std::string client_address;
        std::uint16_t client_port{0};
        bool tls{false};
    };

    struct ProxyResponse {
        std::string raw_http;
    };

    using RequestPreflight =
        std::function<asio::awaitable<std::optional<ProxyResponse>>(ProxyRequest&)>;
    using RequestTransform = std::function<void(ProxyRequest&)>;
    using ResponseTransform = std::function<void(const ProxyRequest&, ProxyResponse&)>;
    using LogHandler = std::function<void(LogLevel, std::string_view)>;

    struct ProxyHandlers {
        RequestPreflight request_preflight;
        RequestTransform request_transform;
        ResponseTransform response_transform;
        LogHandler log;
    };

    class ProxySystem {
    public:
        explicit ProxySystem(ProxyConfig config, ProxyHandlers handlers = {});
        ~ProxySystem();

        ProxySystem(const ProxySystem&) = delete;
        ProxySystem& operator=(const ProxySystem&) = delete;
        ProxySystem(ProxySystem&&) = delete;
        ProxySystem& operator=(ProxySystem&&) = delete;

        void start();
        void stop();
        bool is_running() const noexcept;

    private:
        class Impl;
        std::unique_ptr<Impl> impl_;
    };
}

#endif
