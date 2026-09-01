#include "async_proxy/proxy_system.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std;
using namespace async_proxy;

namespace {
    volatile sig_atomic_t stop_requested = 0;

    extern "C" void signal_handler(int) {
        stop_requested = 1;
    }

    uint16_t parse_port(const string& value) {
        const unsigned long parsed = stoul(value);
        if (parsed > numeric_limits<uint16_t>::max())
            throw invalid_argument("port is out of range: " + value);
        return static_cast<uint16_t>(parsed);
    }

    size_t parse_size(const string& value, string_view name) {
        const unsigned long long parsed = stoull(value);
        if (parsed == 0 || parsed > numeric_limits<size_t>::max())
            throw invalid_argument(string(name) + " is out of range: " + value);
        return static_cast<size_t>(parsed);
    }

    void print_usage(const char* program) {
        cout
            << "Usage: " << program << " [options]\n\n"
            << "Required upstream options:\n"
            << "  --upstream-host HOST       Upstream host or IP\n"
            << "  --upstream-port PORT       Upstream TCP port\n\n"
            << "Listener options:\n"
            << "  --listen ADDRESS           Listen address (default: 0.0.0.0)\n"
            << "  --http-port PORT           HTTP port; 0 disables HTTP (default: 8080)\n"
            << "  --https-port PORT          HTTPS port; 0 disables HTTPS (default: 0)\n"
            << "  --cert FILE                HTTPS certificate chain\n"
            << "  --key FILE                 HTTPS private key\n\n"
            << "Upstream TLS options:\n"
            << "  --upstream-tls             Enable TLS to upstream\n"
            << "  --upstream-insecure        Disable upstream certificate verification\n"
            << "  --upstream-ca FILE         Additional upstream CA file\n\n"
            << "Resource options:\n"
            << "  --threads COUNT            I/O thread count (default: 2)\n"
            << "  --workers COUNT            Upstream worker count (default: 2)\n"
            << "  --queue-capacity COUNT     Bounded request queue size (default: 1024)\n"
            << "  --max-sessions COUNT       Concurrent client sessions (default: 128)\n"
            << "  --max-inflight-bytes BYTES Global request/response byte budget\n"
            << "  --help                     Show this help\n";
    }

    ProxyConfig parse_arguments(int argc, char* argv[]) {
        ProxyConfig config;

        for (int i = 1; i < argc; ++i) {
            const string argument = argv[i];
            auto next_value = [&](string_view option) -> string {
                if (i + 1 >= argc)
                    throw invalid_argument(string(option) + " requires a value");
                return argv[++i];
            };

            if (argument == "--help") {
                print_usage(argv[0]);
                exit(EXIT_SUCCESS);
            } else if (argument == "--listen") {
                config.listen_address = next_value(argument);
            } else if (argument == "--http-port") {
                config.http_port = parse_port(next_value(argument));
            } else if (argument == "--https-port") {
                config.https_port = parse_port(next_value(argument));
            } else if (argument == "--cert") {
                config.certificate_file = next_value(argument);
            } else if (argument == "--key") {
                config.private_key_file = next_value(argument);
            } else if (argument == "--upstream-host") {
                config.upstream_host = next_value(argument);
            } else if (argument == "--upstream-port") {
                config.upstream_port = next_value(argument);
            } else if (argument == "--upstream-tls") {
                config.upstream_tls = true;
            } else if (argument == "--upstream-insecure") {
                config.upstream_insecure = true;
            } else if (argument == "--upstream-ca") {
                config.upstream_ca_file = next_value(argument);
            } else if (argument == "--threads") {
                config.io_thread_count = parse_size(next_value(argument), argument);
            } else if (argument == "--workers") {
                config.worker_count = parse_size(next_value(argument), argument);
            } else if (argument == "--queue-capacity") {
                config.queue_capacity = parse_size(next_value(argument), argument);
            } else if (argument == "--max-sessions") {
                config.max_sessions = parse_size(next_value(argument), argument);
            } else if (argument == "--max-inflight-bytes") {
                config.max_inflight_bytes = parse_size(next_value(argument), argument);
            } else {
                throw invalid_argument("unknown option: " + argument);
            }
        }
        return config;
    }
}

int main(int argc, char* argv[]) {
    try {
        ProxyConfig config = parse_arguments(argc, argv);
        ProxyHandlers handlers;
        handlers.log = [](LogLevel level, string_view message) {
            const char* prefix = level == LogLevel::Info
                ? "INFO"
                : (level == LogLevel::Warning ? "WARN" : "ERROR");
            cerr << '[' << prefix << "] " << message << '\n';
        };

        signal(SIGINT, signal_handler);
        signal(SIGTERM, signal_handler);

        ProxySystem proxy(move(config), move(handlers));
        proxy.start();
        while (!stop_requested)
            this_thread::sleep_for(chrono::milliseconds(200));
        proxy.stop();
        return EXIT_SUCCESS;
    } catch (const exception& error) {
        cerr << "async_proxy: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
