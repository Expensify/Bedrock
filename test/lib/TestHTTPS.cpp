/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    TestHTTPS.cpp
 * Path:    test/lib/TestHTTPS.cpp
 * Pair:    TestHTTPS.h
 *
 * INTENT
 *   Implements TestHTTPS as declared in the header - see there for intent.
 *
 * OBJECTS
 *   All members implement TestHTTPS as declared in the header; nothing file-local is added.
 *   `_onRecv` parses the numeric status out of an HTTP method line by skipping past the first
 *   space (the "HTTP/X.Y" token) to the next non-space run, defaulting to 400 if that parse fails.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with the header.
 * ─────────────────────────────────────────────────────────────────────*/
#include "TestHTTPS.h"

TestHTTPS::~TestHTTPS()
{
}

bool TestHTTPS::_onRecv(Transaction& transaction)
{
    // Parse the method line, this is more complicated than in most of our code because here we're handling HTTP
    // responses instead of bedrock responses, the difference being that HTTP responses start with `HTTP/X.Y `, which
    // we don't care about. This code looks for the first space in the methodLine, and then for the first non-space
    // after that, and *then* parses the response code. If we fail to find such a code, or can't parse it as an
    // integer, we default to 400.
    string methodLine = transaction.fullResponse.methodLine;
    transaction.response = 0;
    size_t offset = methodLine.find_first_of(' ', 0);
    offset = methodLine.find_first_not_of(' ', offset);
    if (offset != string::npos) {
        int status = SToInt(methodLine.substr(offset));
        if (status) {
            transaction.response = status;
        }
    }

    return false;
}

unique_ptr<SHTTPSManager::Transaction> TestHTTPS::sendRequest(const string& url, SData& request)
{
    return _httpsSend(url, request);
}
