/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SFastBuffer.h
 * Path:    libstuff/SFastBuffer.h
 * Pair:    SFastBuffer.cpp
 *
 * INTENT
 *   An append-friendly byte buffer that avoids repeated reallocation/erase
 *   on the front by tracking a `front` offset into a single string, for use
 *   as socket send/recv buffers polled incrementally over many calls.
 *
 * OBJECTS
 *   SFastBuffer (class) - front/data storage; empty/size/c_str/clear;
 *     consumeFront (drop bytes already sent/read); append; operator+=/=;
 *     startsWithHTTPRequest() - incremental scan for an HTTP header
 *     terminator (`\r\n\r\n` or `\n\n`) across repeated calls.
 *   operator<<(ostream&, const SFastBuffer&) - free stream helper.
 *
 * OUT OF PLACE
 *   [CANDIDATE] startsWithHTTPRequest() and its associated state
 *     (nextToCheck, headerLength) - bakes HTTP-specific framing knowledge
 *     into what is otherwise a protocol-agnostic buffer type.
 *
 * NAME/LOCATION FIT
 *   Fits; a protocol-agnostic buffer name, slightly undercut by the
 *   HTTP-specific method noted above.
 *
 * NAMING QUALITY
 *   Consistent. `contentLength` is a private member that is reset to 0 in
 *   four places but never read or assigned any other value anywhere in the
 *   .cpp - a dead/vestigial field that reads as meaningful but does nothing.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include <string>
#include <ostream>

using namespace std;

class SFastBuffer {
public:
    SFastBuffer();
    SFastBuffer(const string& str);
    bool empty() const;
    bool startsWithHTTPRequest();
    size_t size() const;
    const char* c_str() const;
    void clear();
    void consumeFront(size_t bytes);
    void append(const char* buffer, size_t bytes);
    SFastBuffer& operator+=(const string& rhs);
    SFastBuffer& operator=(const string& rhs);

private:
    size_t front;
    string data;

    // State for managing checking if this contains an HTTP request.
    size_t nextToCheck = 0;
    size_t headerLength = 0;
    size_t contentLength = 0;
};
ostream& operator<<(ostream& os, const SFastBuffer& buf);
