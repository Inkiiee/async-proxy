#ifndef ASYNC_PROXY_HTTP_H
#define ASYNC_PROXY_HTTP_H

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace async_proxy {
    struct HttpHeader {
        std::string name;
        std::string value;
    };

    struct HttpRequest {
        std::string method;
        std::string target;
        std::vector<HttpHeader> headers;
        std::string body;
        bool has_body_framing{false};
    };

    enum class HttpParseStatus {
        Incomplete,
        Complete,
        Invalid,
        TooLarge,
    };

    struct HttpRequestParseResult {
        HttpParseStatus status{HttpParseStatus::Incomplete};
        HttpRequest request;
        std::string error;
    };

    enum class HttpResponseStatus {
        Incomplete,
        Complete,
        CloseDelimited,
        Invalid,
        TooLarge,
    };

    struct HttpResponseInspection {
        HttpResponseStatus status{HttpResponseStatus::Incomplete};
        std::size_t message_size{0};
        std::string error;
    };

    HttpRequestParseResult parse_http_request(
        std::string_view input,
        std::size_t max_message_size,
        std::size_t max_header_size = 64 * 1024
    );

    HttpResponseInspection inspect_http_response(
        std::string_view input,
        std::size_t max_message_size,
        std::size_t max_header_size = 64 * 1024
    );

    std::string serialize_http_request(
        const HttpRequest& request,
        std::string_view upstream_authority
    );

    std::string make_error_response(
        int status,
        std::string_view reason,
        std::string_view message
    );
}

#endif
