#include "SHTTPResponseParser.h"

#include <charconv>
#include <libstuff/SData.h>

namespace {
string_view trimWhitespace(string_view value, string_view whitespace = " \t")
{
    const size_t first = value.find_first_not_of(whitespace);
    if (first == string_view::npos) {
        return {};
    }
    return value.substr(first, value.find_last_not_of(whitespace) - first + 1);
}

bool parseSize(string_view value, size_t& size, int base)
{
    if (value.empty()) {
        return false;
    }
    const char* end = value.data() + value.size();
    const auto parsed = from_chars(value.data(), end, size, base);
    return parsed.ec == errc() && parsed.ptr == end;
}
}

int SHTTPResponseParser::getStatusCode(string_view line)
{
    // Internal Bedrock replies can omit the HTTP version and begin with the status code.
    if (line.starts_with("HTTP/")) {
        const size_t statusStart = line.find(' ');
        if (statusStart == string_view::npos) {
            return 0;
        }
        line = trimWhitespace(line.substr(statusStart));
    }
    size_t code = 0;
    const auto token = line.substr(0, line.find_first_of(" \t"));
    return token.size() == 3 && parseSize(token, code, 10) ? static_cast<int>(code) : 0;
}

bool SHTTPResponseParser::_readLine(string_view received, string_view& line)
{
    // Keep the start of the unfinished line and resume the search where the last read stopped.
    _lineSearchOffset = max(_offset, _lineSearchOffset);
    const size_t end = received.find_first_of("\r\n", _lineSearchOffset);
    if (end == string_view::npos) {
        _lineSearchOffset = received.size();
        return false;
    }
    if (received[end] == '\r' && end + 1 == received.size()) {
        _lineSearchOffset = end;
        return false;
    }

    line = received.substr(_offset, end - _offset);
    _offset = end + 1;
    if (received[end] == '\r' && _offset < received.size() && received[_offset] == '\n') {
        ++_offset;
    }
    return true;
}

bool SHTTPResponseParser::_readHeader(string_view line, SData& response)
{
    if (line.front() == ' ' || line.front() == '\t') {
        if (!_lastHeader.empty()) {
            response.nameValueMap[_lastHeader].append(line);
        }
        return true;
    }

    const size_t colon = line.find(':');
    const string_view name = trimWhitespace(line.substr(0, colon), " ");
    if (colon == string_view::npos || name.empty()) {
        return false;
    }

    const bool isFramingHeader = SIEquals(name, "Content-Length") || SIEquals(name, "Transfer-Encoding");
    if (_phase == Phase::TRAILERS && isFramingHeader) {
        _lastHeader.clear();
        return true;
    }

    _lastHeader = name;
    const string_view value = trimWhitespace(line.substr(colon + 1), " ");
    if (isFramingHeader) {
        // Validate all framing values together. Overwriting duplicates could hide a conflict.
        auto [header, inserted] = response.nameValueMap.try_emplace(_lastHeader, value);
        if (!inserted) {
            header->second += ',';
            header->second.append(value);
        }
    } else {
        SSetHTTPHeader(response.nameValueMap, _lastHeader, value);
    }
    return true;
}

bool SHTTPResponseParser::_startBody(SData& response, bool isHeadRequest)
{
    // Internal Bedrock replies use Content-Length independently of their status codes.
    const int code = response.methodLine.starts_with("HTTP/") ? getStatusCode(response.methodLine) : 0;
    if (code >= 100 && code < 200 && code != 101) {
        // An informational response is followed by another status line on the same connection.
        response.clear();
        _phase = Phase::STATUS_LINE;
    } else if (isHeadRequest || code == 101 || code == 204 || code == 304) {
        _phase = Phase::COMPLETE;
    } else if (const auto encoding = response.nameValueMap.find("Transfer-Encoding"); encoding != response.nameValueMap.end()) {
        // Transfer-Encoding takes precedence over Content-Length. A final chunked coding defines the boundary.
        response.erase("Content-Length");
        const string_view value(encoding->second);
        const size_t lastComma = value.rfind(',');
        const string_view lastCoding = trimWhitespace(lastComma == string_view::npos ? value : value.substr(lastComma + 1));
        if (lastCoding.empty()) {
            return false;
        }
        if (SIEquals(lastCoding, "chunked")) {
            if (lastComma != string_view::npos) {
                _remainingTransferEncoding = trimWhitespace(value.substr(0, lastComma));
            }
            _phase = Phase::CHUNK_SIZE;
        } else {
            _phase = Phase::CLOSE_DELIMITED_BODY;
        }
    } else if (const auto length = response.nameValueMap.find("Content-Length"); length != response.nameValueMap.end()) {
        string_view value(length->second);
        size_t comma = value.find(',');
        if (!parseSize(trimWhitespace(value.substr(0, comma)), _remaining, 10)) {
            return false;
        }
        if (comma != string_view::npos) {
            do
            {
                value.remove_prefix(comma + 1);
                comma = value.find(',');
                size_t duplicate = 0;
                if (!parseSize(trimWhitespace(value.substr(0, comma)), duplicate, 10) || duplicate != _remaining) {
                    return false;
                }
            } while (comma != string_view::npos);
            length->second = to_string(_remaining);
        }
        _phase = Phase::FIXED_BODY;
    } else {
        _phase = Phase::CLOSE_DELIMITED_BODY;
    }
    _lastHeader.clear();
    return true;
}

SHTTPResponseParser::Result SHTTPResponseParser::_needMore(bool atEOF)
{
    if (atEOF) {
        _phase = Phase::TRUNCATED;
        return Result::TRUNCATED;
    }
    return Result::NEED_MORE;
}

SHTTPResponseParser::Result SHTTPResponseParser::parse(string_view received, SData& response, bool atEOF, bool isHeadRequest)
{
    if (received.size() < max(_offset, _lineSearchOffset)) {
        _phase = Phase::INVALID;
    }

    while (true) {
        switch (_phase) {
            case Phase::STATUS_LINE:
            case Phase::HEADERS:
            case Phase::CHUNK_SIZE:
            case Phase::TRAILERS: {
                string_view line;
                if (!_readLine(received, line)) {
                    return _needMore(atEOF);
                }
                if (_phase == Phase::STATUS_LINE) {
                    const string_view status = trimWhitespace(line, " ");
                    if (!status.empty()) {
                        response.methodLine = status;
                        _phase = Phase::HEADERS;
                    }
                } else if (_phase == Phase::CHUNK_SIZE) {
                    const string_view size = trimWhitespace(line.substr(0, line.find(';')));
                    if (received.substr(_offset - 2, 2) != "\r\n" || !parseSize(size, _remaining, 16)) {
                        _phase = Phase::INVALID;
                    } else {
                        _phase = _remaining ? Phase::CHUNK_DATA : Phase::TRAILERS;
                    }
                } else if (!line.empty()) {
                    if (!_readHeader(line, response)) {
                        _phase = Phase::INVALID;
                    }
                } else if (_phase == Phase::TRAILERS) {
                    if (received.substr(_offset - 2, 2) != "\r\n") {
                        _phase = Phase::INVALID;
                        break;
                    }
                    if (_remainingTransferEncoding.empty()) {
                        response.erase("Transfer-Encoding");
                        response.set("Content-Length", response.content.size());
                    } else {
                        response.nameValueMap["Transfer-Encoding"] = move(_remainingTransferEncoding);
                    }
                    _phase = Phase::COMPLETE;
                } else if (!_startBody(response, isHeadRequest)) {
                    _phase = Phase::INVALID;
                }
                break;
            }

            case Phase::FIXED_BODY:
            case Phase::CLOSE_DELIMITED_BODY:
            case Phase::CHUNK_DATA: {
                const size_t available = received.size() - _offset;
                const size_t count = _phase == Phase::CLOSE_DELIMITED_BODY ? available : min(_remaining, available);
                response.content.append(received.substr(_offset, count));
                _offset += count;
                if (_phase == Phase::CLOSE_DELIMITED_BODY) {
                    if (!atEOF) {
                        return Result::NEED_MORE;
                    }
                    _phase = Phase::COMPLETE;
                } else {
                    _remaining -= count;
                    if (_remaining) {
                        return _needMore(atEOF);
                    }
                    _phase = _phase == Phase::CHUNK_DATA ? Phase::CHUNK_END : Phase::COMPLETE;
                }
                break;
            }

            case Phase::CHUNK_END:
                if (received.size() - _offset < 2) {
                    return _needMore(atEOF);
                }
                if (received.substr(_offset, 2) == "\r\n") {
                    _offset += 2;
                    _phase = Phase::CHUNK_SIZE;
                } else {
                    _phase = Phase::INVALID;
                }
                break;

            case Phase::COMPLETE:
                response.reserveJSONHeaderPadding();
                return Result::COMPLETE;

            case Phase::INVALID:
                return Result::INVALID;

            case Phase::TRUNCATED:
                return Result::TRUNCATED;
        }
    }
}

size_t SHTTPResponseParser::consumed() const
{
    return _offset;
}
