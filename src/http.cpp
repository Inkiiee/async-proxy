#include "async_proxy/http.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

using namespace std;

namespace async_proxy {
namespace {
    constexpr string_view header_delimiter = "\r\n\r\n";

    string lower_ascii(string_view value) {
        string result;
        result.reserve(value.size());
        for (const unsigned char ch : value)
            result.push_back(static_cast<char>(tolower(ch)));
        return result;
    }

    string_view trim_ascii(string_view value) {
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
            value.remove_prefix(1);
        while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
            value.remove_suffix(1);
        return value;
    }

    bool valid_token(string_view value) {
        if (value.empty())
            return false;

        constexpr string_view separators = "()<>@,;:\\\"/[]?={} \t";
        for (const unsigned char ch : value) {
            if (ch <= 32 || ch >= 127 ||
                separators.find(static_cast<char>(ch)) != string_view::npos) {
                return false;
            }
        }
        return true;
    }

    bool valid_field_value(string_view value) {
        return all_of(value.begin(), value.end(), [](unsigned char ch) {
            return ch == '\t' || (ch >= 32 && ch != 127);
        });
    }

    bool valid_target(string_view value) {
        return !value.empty() && all_of(value.begin(), value.end(), [](unsigned char ch) {
            return ch > 32 && ch != 127;
        });
    }

    bool has_invalid_line_endings(string_view value) {
        for (size_t i = 0; i < value.size(); ++i) {
            if (value[i] == '\n' && (i == 0 || value[i - 1] != '\r'))
                return true;
            if (value[i] == '\r' && (i + 1 >= value.size() || value[i + 1] != '\n'))
                return true;
        }
        return false;
    }

    optional<size_t> parse_decimal_size(string_view value) {
        value = trim_ascii(value);
        if (value.empty() || !all_of(value.begin(), value.end(), [](unsigned char ch) {
                return isdigit(ch) != 0;
            })) {
            return nullopt;
        }

        size_t parsed = 0;
        const auto [end, ec] = from_chars(value.data(), value.data() + value.size(), parsed);
        if (ec != errc{} || end != value.data() + value.size())
            return nullopt;
        return parsed;
    }

    optional<size_t> parse_hex_size(string_view value) {
        if (value.empty() || !all_of(value.begin(), value.end(), [](unsigned char ch) {
                return isxdigit(ch) != 0;
            })) {
            return nullopt;
        }

        size_t parsed = 0;
        const auto [end, ec] = from_chars(value.data(), value.data() + value.size(), parsed, 16);
        if (ec != errc{} || end != value.data() + value.size())
            return nullopt;
        return parsed;
    }

    vector<string> comma_tokens(string_view value) {
        vector<string> result;
        size_t begin = 0;
        while (begin <= value.size()) {
            const size_t comma = value.find(',', begin);
            const size_t end = comma == string_view::npos ? value.size() : comma;
            const string_view token = trim_ascii(value.substr(begin, end - begin));
            if (token.empty())
                return {};
            result.push_back(lower_ascii(token));
            if (comma == string_view::npos)
                break;
            begin = comma + 1;
        }
        return result;
    }

    struct ParsedHead {
        vector<HttpHeader> headers;
        unordered_set<string> connection_options;
        optional<size_t> content_length;
        bool chunked{false};
        size_t host_count{0};
        string error;
    };

    bool parse_headers(string_view input, size_t cursor, size_t delimiter, ParsedHead& out) {
        size_t content_length_count = 0;
        size_t transfer_encoding_count = 0;

        while (cursor < delimiter) {
            const size_t line_end = input.find("\r\n", cursor);
            if (line_end == string_view::npos || line_end > delimiter) {
                out.error = "invalid HTTP header line";
                return false;
            }

            const string_view line = input.substr(cursor, line_end - cursor);
            cursor = line_end + 2;
            if (line.empty() || line.front() == ' ' || line.front() == '\t') {
                out.error = "folded or empty HTTP header is not supported";
                return false;
            }

            const size_t colon = line.find(':');
            if (colon == string_view::npos || colon == 0) {
                out.error = "invalid HTTP header";
                return false;
            }

            const string_view name = line.substr(0, colon);
            const string_view value = trim_ascii(line.substr(colon + 1));
            if (!valid_token(name) || !valid_field_value(value)) {
                out.error = "invalid HTTP header name or value";
                return false;
            }

            const string lower_name = lower_ascii(name);
            if (lower_name == "content-length") {
                ++content_length_count;
                const auto length = parse_decimal_size(value);
                if (!length) {
                    out.error = "invalid Content-Length";
                    return false;
                }
                out.content_length = *length;
            } else if (lower_name == "transfer-encoding") {
                ++transfer_encoding_count;
                const auto codings = comma_tokens(value);
                if (codings.size() != 1 || codings.front() != "chunked") {
                    out.error = "only a single chunked transfer coding is supported";
                    return false;
                }
                out.chunked = true;
            } else if (lower_name == "connection") {
                const auto options = comma_tokens(value);
                if (options.empty() && !value.empty()) {
                    out.error = "invalid Connection header";
                    return false;
                }
                out.connection_options.insert(options.begin(), options.end());
            } else if (lower_name == "host") {
                ++out.host_count;
            }

            out.headers.push_back({string(name), string(value)});
        }

        if (content_length_count > 1) {
            out.error = "duplicate Content-Length is not accepted";
            return false;
        }
        if (transfer_encoding_count > 1) {
            out.error = "duplicate Transfer-Encoding is not accepted";
            return false;
        }
        if (out.content_length && out.chunked) {
            out.error = "Content-Length and Transfer-Encoding cannot be combined";
            return false;
        }
        return true;
    }

    enum class ChunkStatus {
        Incomplete,
        Complete,
        Invalid,
        TooLarge,
    };

    struct ChunkResult {
        ChunkStatus status{ChunkStatus::Incomplete};
        size_t consumed{0};
        string decoded;
        string error;
    };

    ChunkResult decode_chunked(string_view input, size_t max_decoded_size, bool keep_body) {
        ChunkResult result;
        size_t cursor = 0;
        size_t decoded_size = 0;

        while (true) {
            const size_t line_end = input.find("\r\n", cursor);
            if (line_end == string_view::npos)
                return result;

            const string_view size_line = input.substr(cursor, line_end - cursor);
            const size_t extension = size_line.find(';');
            const string_view size_text = size_line.substr(0, extension);
            const auto chunk_size = parse_hex_size(size_text);
            if (!chunk_size) {
                result.status = ChunkStatus::Invalid;
                result.error = "invalid chunk size";
                return result;
            }
            if (extension != string_view::npos && !valid_field_value(size_line.substr(extension + 1))) {
                result.status = ChunkStatus::Invalid;
                result.error = "invalid chunk extension";
                return result;
            }

            cursor = line_end + 2;
            if (*chunk_size == 0) {
                while (true) {
                    const size_t trailer_end = input.find("\r\n", cursor);
                    if (trailer_end == string_view::npos)
                        return result;
                    if (trailer_end == cursor) {
                        result.status = ChunkStatus::Complete;
                        result.consumed = trailer_end + 2;
                        return result;
                    }

                    const string_view trailer = input.substr(cursor, trailer_end - cursor);
                    const size_t colon = trailer.find(':');
                    if (colon == string_view::npos || colon == 0 ||
                        !valid_token(trailer.substr(0, colon)) ||
                        !valid_field_value(trim_ascii(trailer.substr(colon + 1)))) {
                        result.status = ChunkStatus::Invalid;
                        result.error = "invalid chunk trailer";
                        return result;
                    }

                    const string lower_name = lower_ascii(trailer.substr(0, colon));
                    if (lower_name == "content-length" || lower_name == "transfer-encoding" ||
                        lower_name == "connection" || lower_name == "host") {
                        result.status = ChunkStatus::Invalid;
                        result.error = "forbidden field in chunk trailer";
                        return result;
                    }
                    cursor = trailer_end + 2;
                }
            }

            if (decoded_size > max_decoded_size ||
                *chunk_size > max_decoded_size - decoded_size) {
                result.status = ChunkStatus::TooLarge;
                result.error = "decoded chunked body is too large";
                return result;
            }
            if (cursor > input.size() || *chunk_size > input.size() - cursor)
                return result;
            if (input.size() - cursor - *chunk_size < 2)
                return result;

            if (input.substr(cursor + *chunk_size, 2) != "\r\n") {
                result.status = ChunkStatus::Invalid;
                result.error = "chunk data is not CRLF terminated";
                return result;
            }

            if (keep_body)
                result.decoded.append(input.substr(cursor, *chunk_size));
            decoded_size += *chunk_size;
            cursor += *chunk_size + 2;
        }
    }

    bool hop_by_hop_header(string_view lower_name, const unordered_set<string>& connection_options) {
        return lower_name == "connection" || lower_name == "proxy-connection" ||
               lower_name == "keep-alive" || lower_name == "te" ||
               lower_name == "trailer" || lower_name == "upgrade" ||
               lower_name == "transfer-encoding" || lower_name == "content-length" ||
               connection_options.contains(string(lower_name));
    }

    HttpRequestParseResult request_result(HttpParseStatus status, string message = {}) {
        HttpRequestParseResult result;
        result.status = status;
        result.error = move(message);
        return result;
    }

    HttpResponseInspection response_result(
        HttpResponseStatus status,
        size_t size = 0,
        string message = {}
    ) {
        return {status, size, move(message)};
    }

    string json_escape(string_view value) {
        string result;
        result.reserve(value.size() + 8);
        for (const char ch : value) {
            switch (ch) {
                case '\\': result += "\\\\"; break;
                case '"': result += "\\\""; break;
                case '\n': result += "\\n"; break;
                case '\r': result += "\\r"; break;
                case '\t': result += "\\t"; break;
                default:
                    if (static_cast<unsigned char>(ch) >= 32)
                        result.push_back(ch);
                    break;
            }
        }
        return result;
    }
}

HttpRequestParseResult parse_http_request(
    string_view input,
    size_t max_message_size,
    size_t max_header_size
) {
    if (input.size() > max_message_size)
        return request_result(HttpParseStatus::TooLarge, "HTTP request is too large");

    const size_t delimiter = input.find(header_delimiter);
    if (delimiter == string_view::npos) {
        if (input.size() >= min(max_message_size, max_header_size))
            return request_result(HttpParseStatus::TooLarge, "HTTP request header is too large");
        return request_result(HttpParseStatus::Incomplete);
    }

    const size_t body_begin = delimiter + header_delimiter.size();
    if (body_begin > max_header_size)
        return request_result(HttpParseStatus::TooLarge, "HTTP request header is too large");
    if (has_invalid_line_endings(input.substr(0, body_begin)))
        return request_result(HttpParseStatus::Invalid, "HTTP headers must use CRLF line endings");

    const size_t request_line_end = input.find("\r\n");
    if (request_line_end == string_view::npos || request_line_end > delimiter)
        return request_result(HttpParseStatus::Invalid, "invalid HTTP request line");

    const string_view request_line = input.substr(0, request_line_end);
    const size_t first_space = request_line.find(' ');
    const size_t second_space = first_space == string_view::npos
        ? string_view::npos
        : request_line.find(' ', first_space + 1);
    if (first_space == string_view::npos || second_space == string_view::npos ||
        request_line.find(' ', second_space + 1) != string_view::npos) {
        return request_result(HttpParseStatus::Invalid, "invalid HTTP request line spacing");
    }

    const string_view method = request_line.substr(0, first_space);
    const string_view target = request_line.substr(first_space + 1, second_space - first_space - 1);
    const string_view version = request_line.substr(second_space + 1);
    if (!valid_token(method) || !valid_target(target) ||
        (version != "HTTP/1.1" && version != "HTTP/1.0")) {
        return request_result(HttpParseStatus::Invalid, "unsupported or invalid HTTP request line");
    }

    ParsedHead head;
    if (!parse_headers(input, request_line_end + 2, delimiter, head))
        return request_result(HttpParseStatus::Invalid, move(head.error));
    if (head.host_count > 1 || (version == "HTTP/1.1" && head.host_count != 1))
        return request_result(HttpParseStatus::Invalid, "HTTP/1.1 requires exactly one Host header");

    HttpRequest request;
    request.method = string(method);
    request.target = string(target);
    request.has_body_framing = head.content_length.has_value() || head.chunked;

    for (auto& header : head.headers) {
        const string lower_name = lower_ascii(header.name);
        if (lower_name == "host" || hop_by_hop_header(lower_name, head.connection_options))
            continue;
        request.headers.push_back(move(header));
    }

    if (head.content_length) {
        if (*head.content_length > max_message_size - body_begin)
            return request_result(HttpParseStatus::TooLarge, "HTTP request body is too large");
        const size_t expected_size = body_begin + *head.content_length;
        if (input.size() < expected_size)
            return request_result(HttpParseStatus::Incomplete);
        if (input.size() > expected_size)
            return request_result(HttpParseStatus::Invalid, "HTTP pipelining is not supported");
        request.body = string(input.substr(body_begin, *head.content_length));
    } else if (head.chunked) {
        auto chunked = decode_chunked(input.substr(body_begin), max_message_size - body_begin, true);
        if (chunked.status == ChunkStatus::Incomplete)
            return request_result(HttpParseStatus::Incomplete);
        if (chunked.status == ChunkStatus::TooLarge)
            return request_result(HttpParseStatus::TooLarge, move(chunked.error));
        if (chunked.status == ChunkStatus::Invalid)
            return request_result(HttpParseStatus::Invalid, move(chunked.error));
        if (body_begin + chunked.consumed != input.size())
            return request_result(HttpParseStatus::Invalid, "HTTP pipelining is not supported");
        request.body = move(chunked.decoded);
    } else if (input.size() != body_begin) {
        return request_result(HttpParseStatus::Invalid, "request body has no framing");
    }

    HttpRequestParseResult result;
    result.status = HttpParseStatus::Complete;
    result.request = move(request);
    return result;
}

HttpResponseInspection inspect_http_response(
    string_view input,
    size_t max_message_size,
    size_t max_header_size
) {
    if (input.size() > max_message_size)
        return response_result(HttpResponseStatus::TooLarge, 0, "HTTP response is too large");

    size_t response_begin = 0;
    while (true) {
        const size_t delimiter = input.find(header_delimiter, response_begin);
        if (delimiter == string_view::npos) {
            if (input.size() - response_begin >= min(max_message_size, max_header_size))
                return response_result(HttpResponseStatus::TooLarge, 0, "HTTP response header is too large");
            return response_result(HttpResponseStatus::Incomplete);
        }

        const size_t body_begin = delimiter + header_delimiter.size();
        if (body_begin - response_begin > max_header_size)
            return response_result(HttpResponseStatus::TooLarge, 0, "HTTP response header is too large");
        if (has_invalid_line_endings(input.substr(response_begin, body_begin - response_begin)))
            return response_result(HttpResponseStatus::Invalid, 0, "HTTP headers must use CRLF line endings");

        const size_t status_line_end = input.find("\r\n", response_begin);
        if (status_line_end == string_view::npos || status_line_end > delimiter)
            return response_result(HttpResponseStatus::Invalid, 0, "invalid HTTP status line");

        const string_view status_line = input.substr(response_begin, status_line_end - response_begin);
        const size_t first_space = status_line.find(' ');
        if (first_space == string_view::npos)
            return response_result(HttpResponseStatus::Invalid, 0, "invalid HTTP status line");
        const string_view version = status_line.substr(0, first_space);
        if (version != "HTTP/1.1" && version != "HTTP/1.0")
            return response_result(HttpResponseStatus::Invalid, 0, "unsupported HTTP response version");

        const size_t code_end = status_line.find(' ', first_space + 1);
        const string_view code_text = status_line.substr(
            first_space + 1,
            (code_end == string_view::npos ? status_line.size() : code_end) - first_space - 1
        );
        int status_code = 0;
        const auto [status_end, status_ec] = from_chars(
            code_text.data(), code_text.data() + code_text.size(), status_code
        );
        if (status_ec != errc{} || status_end != code_text.data() + code_text.size() ||
            code_text.size() != 3 || status_code < 100 || status_code > 999) {
            return response_result(HttpResponseStatus::Invalid, 0, "invalid HTTP status code");
        }

        ParsedHead head;
        if (!parse_headers(input, status_line_end + 2, delimiter, head))
            return response_result(HttpResponseStatus::Invalid, 0, move(head.error));

        if (status_code == 101)
            return response_result(HttpResponseStatus::Invalid, 0, "protocol upgrades are not supported");

        const bool informational = status_code >= 100 && status_code < 200;
        const bool body_forbidden = informational || status_code == 204 || status_code == 304;
        if (body_forbidden) {
            if (head.chunked || (head.content_length && *head.content_length != 0))
                return response_result(HttpResponseStatus::Invalid, 0, "response status cannot contain a body");
            if (informational) {
                if (input.size() == body_begin)
                    return response_result(HttpResponseStatus::Incomplete);
                response_begin = body_begin;
                continue;
            }
            if (input.size() != body_begin)
                return response_result(HttpResponseStatus::Invalid, 0, "unexpected data after bodyless response");
            return response_result(HttpResponseStatus::Complete, body_begin);
        }

        if (head.content_length) {
            if (*head.content_length > max_message_size - body_begin)
                return response_result(HttpResponseStatus::TooLarge, 0, "HTTP response body is too large");
            const size_t response_end = body_begin + *head.content_length;
            if (input.size() < response_end)
                return response_result(HttpResponseStatus::Incomplete);
            if (input.size() > response_end)
                return response_result(HttpResponseStatus::Invalid, 0, "unexpected data after HTTP response");
            return response_result(HttpResponseStatus::Complete, response_end);
        }

        if (head.chunked) {
            auto chunked = decode_chunked(input.substr(body_begin), max_message_size - body_begin, false);
            if (chunked.status == ChunkStatus::Incomplete)
                return response_result(HttpResponseStatus::Incomplete);
            if (chunked.status == ChunkStatus::TooLarge)
                return response_result(HttpResponseStatus::TooLarge, 0, move(chunked.error));
            if (chunked.status == ChunkStatus::Invalid)
                return response_result(HttpResponseStatus::Invalid, 0, move(chunked.error));
            const size_t response_end = body_begin + chunked.consumed;
            if (input.size() != response_end)
                return response_result(HttpResponseStatus::Invalid, 0, "unexpected data after HTTP response");
            return response_result(HttpResponseStatus::Complete, response_end);
        }

        return response_result(HttpResponseStatus::CloseDelimited, input.size());
    }
}

string serialize_http_request(const HttpRequest& request, string_view upstream_authority) {
    if (!valid_token(request.method) || !valid_target(request.target) ||
        upstream_authority.empty() || !valid_field_value(upstream_authority)) {
        throw invalid_argument("invalid proxy request");
    }

    ostringstream output;
    output << request.method << ' ' << request.target << " HTTP/1.1\r\n";
    output << "Host: " << upstream_authority << "\r\n";

    for (const auto& header : request.headers) {
        const string lower_name = lower_ascii(header.name);
        if (!valid_token(header.name) || !valid_field_value(header.value))
            throw invalid_argument("invalid transformed HTTP header");
        if (lower_name == "host" || lower_name == "connection" ||
            lower_name == "content-length" || lower_name == "transfer-encoding" ||
            lower_name == "proxy-connection" || lower_name == "keep-alive" ||
            lower_name == "te" || lower_name == "trailer" || lower_name == "upgrade") {
            continue;
        }
        output << header.name << ": " << header.value << "\r\n";
    }

    if (request.has_body_framing || !request.body.empty())
        output << "Content-Length: " << request.body.size() << "\r\n";
    output << "Connection: close\r\n\r\n";
    output << request.body;
    return output.str();
}

string make_error_response(int status, string_view reason, string_view message) {
    const string body = "{\"error\":\"" + json_escape(message) + "\"}";
    ostringstream output;
    output << "HTTP/1.1 " << status << ' ' << reason << "\r\n"
           << "Content-Type: application/json; charset=utf-8\r\n"
           << "Content-Length: " << body.size() << "\r\n"
           << "Connection: close\r\n\r\n"
           << body;
    return output.str();
}
}
