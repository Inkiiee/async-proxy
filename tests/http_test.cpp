#include "async_proxy/http.h"

#include <cstdlib>
#include <iostream>
#include <string>

using namespace std;
using namespace async_proxy;

namespace {
    int failures = 0;

    void expect(bool condition, const char* name) {
        if (condition)
            return;
        ++failures;
        cerr << "FAILED: " << name << '\n';
    }
}

int main() {
    constexpr size_t limit = 4 * 1024 * 1024;

    {
        const string request =
            "POST /verify HTTP/1.1\r\n"
            "Host: agent.local\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: 2\r\n\r\n{}";
        const auto parsed = parse_http_request(request, limit);
        expect(parsed.status == HttpParseStatus::Complete, "valid Content-Length request");
        expect(parsed.request.method == "POST", "method parsed");
        expect(parsed.request.target == "/verify", "target parsed");
        expect(parsed.request.body == "{}", "body parsed");

        const string serialized = serialize_http_request(parsed.request, "127.0.0.1:9000");
        expect(serialized.find("Host: 127.0.0.1:9000\r\n") != string::npos, "Host rewritten");
        expect(serialized.find("Connection: close\r\n") != string::npos, "Connection close added");
    }

    {
        const string request =
            "POST / HTTP/1.1\r\nHost: test\r\n"
            "Content-Length: 1\r\nContent-Length: 1\r\n\r\nx";
        expect(
            parse_http_request(request, limit).status == HttpParseStatus::Invalid,
            "duplicate Content-Length rejected"
        );
    }

    {
        const string request =
            "POST / HTTP/1.1\r\nHost: test\r\n"
            "Content-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n"
            "0\r\n\r\n";
        expect(
            parse_http_request(request, limit).status == HttpParseStatus::Invalid,
            "Content-Length plus Transfer-Encoding rejected"
        );
    }

    {
        const string request =
            "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: -1\r\n\r\n";
        expect(
            parse_http_request(request, limit).status == HttpParseStatus::Invalid,
            "negative Content-Length rejected"
        );
    }

    {
        const string request =
            "GET / HTTP/1.1\nHost: test\n\n";
        expect(
            parse_http_request(request, limit).status != HttpParseStatus::Complete,
            "bare LF request rejected"
        );
    }

    {
        const string request =
            "POST /chunk HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n"
            "4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Trace: yes\r\n\r\n";
        const auto parsed = parse_http_request(request, limit);
        expect(parsed.status == HttpParseStatus::Complete, "chunked request accepted");
        expect(parsed.request.body == "Wikipedia", "chunked request decoded");
    }

    {
        const string preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        expect(
            parse_http_request(preface, limit).status == HttpParseStatus::Invalid,
            "HTTP/2 preface rejected"
        );
    }

    {
        const string response =
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
        const auto inspected = inspect_http_response(response, limit);
        expect(inspected.status == HttpResponseStatus::Complete, "fixed response complete");
        expect(inspected.message_size == response.size(), "response size reported");
    }

    {
        const string response =
            "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nbody";
        expect(
            inspect_http_response(response, limit).status == HttpResponseStatus::CloseDelimited,
            "close-delimited response recognized"
        );
    }

    {
        const string response =
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            "2\r\nOK\r\n0\r\n\r\n";
        expect(
            inspect_http_response(response, limit).status == HttpResponseStatus::Complete,
            "chunked response complete"
        );
    }

    {
        const string response =
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabc";
        expect(
            inspect_http_response(response, limit).status == HttpResponseStatus::Incomplete,
            "truncated fixed response remains incomplete"
        );
    }

    {
        const string response =
            "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx";
        expect(
            inspect_http_response(response, limit).status == HttpResponseStatus::Invalid,
            "duplicate response Content-Length rejected"
        );
    }

    {
        const string response = "HTTP/2 200\r\n\r\n";
        expect(
            inspect_http_response(response, limit).status == HttpResponseStatus::Invalid,
            "HTTP/2 text response rejected"
        );
    }

    if (failures != 0) {
        cerr << failures << " test(s) failed\n";
        return EXIT_FAILURE;
    }
    cout << "all HTTP parser tests passed\n";
    return EXIT_SUCCESS;
}
