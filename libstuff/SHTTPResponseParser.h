#pragma once

#include <string>
#include <string_view>

using namespace std;

struct SData;

class SHTTPResponseParser {
public:
    enum class Result { NEED_MORE, COMPLETE, INVALID, TRUNCATED };

    // Pass the entire growing buffer and the same initially empty output on each call.
    // The output body grows as bytes arrive.
    Result parse(string_view received, SData& response, bool atEOF, bool isHeadRequest);

    // The number of bytes to remove from the receive buffer after COMPLETE.
    size_t consumed() const;

    // Returns zero when the status line does not contain a three-digit code.
    static int getStatusCode(string_view line);

private:
    enum class Phase { STATUS_LINE, HEADERS, FIXED_BODY, CLOSE_DELIMITED_BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_END, TRAILERS, COMPLETE, INVALID, TRUNCATED };

    bool _readLine(string_view received, string_view& line);
    bool _readHeader(string_view line, SData& response);
    bool _startBody(SData& response, bool isHeadRequest);
    Result _needMore(bool atEOF);

    Phase _phase = Phase::STATUS_LINE;
    size_t _offset = 0;
    size_t _lineSearchOffset = 0;
    size_t _remaining = 0;
    string _lastHeader;
    string _remainingTransferEncoding;
};
