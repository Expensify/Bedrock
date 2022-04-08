#include "TestHTTPS.h"

TestHTTPS::~TestHTTPS() {}

bool TestHTTPS::_onRecv(Transaction* transaction) {
    // Parse the method line, this is more complicated than in most of our code because here we're handling HTTP
    // responses instead of bedrock responses, the difference being that HTTP responses start with `HTTP/X.Y `, which
    // we don't care about. This code looks for the first space in the methodLine, and then for the first non-space
    // after that, and *then* parses the response code. If we fail to find such a code, or can't parse it as an
    // integer, we default to 400.
    string methodLine = transaction->fullResponse.methodLine;
    transaction->response = 0;
    size_t offset = methodLine.find_first_of(' ', 0);
    offset = methodLine.find_first_not_of(' ', offset);
    if (offset != string::npos) {
        int64_t status = SToInt64(methodLine.substr(offset));
        if (status) {
            transaction->response = status;
        }
    }

    return false;
}

SHTTPSManager::Transaction* TestHTTPS::sendRequest(const string& url, SData& request) {
    return _httpsSend(url, request);
}
