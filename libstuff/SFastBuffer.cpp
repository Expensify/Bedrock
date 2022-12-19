#include "SFastBuffer.h"

#include <cstring>

SFastBuffer::SFastBuffer() : front(0) {
}

SFastBuffer::SFastBuffer(const string& str) : front(0), data(str) {
}

bool SFastBuffer::startsWithHTTPRequest() {
    // No HTTP request is less than 4 bytes. Strictly, an HTTP request is longer than this, but this is all we need to care about.
    if (size() < 4) {
        return false;
    }

    // Do we have headers yet? If not, keep looking for them.
    // Headers are optional, but this will actually contain the methodline as well, so we won't end up with an ambiguous case where '0' means both "we haven't found them yet" and "there
    // aren't any".
    if (!headerLength) {
        size_t bodySeparator = data.find("\r\n\r\n", nextToCheck);
        if (bodySeparator == string::npos) {
            // This is dumb.
            bodySeparator = data.find("\n\n", nextToCheck);
        }
        if (bodySeparator != string::npos) {
            headerLength = bodySeparator - front;
        } else {
            // We subtract 4 so that we can't accidentally end up in the middle of the 4-char sequence that we're searching for and end up missing it on two sequential calls because each
            // contained only a single newline.
            nextToCheck = data.size() - 4;
        }
    }

    // This is good enough for what we need right now, but it suffers the same exact problem that this was meant to fix, except for the body. This may be deferred as a future improvement to
    // deal with long bodies in addition to long headers.
    return headerLength;
}

bool SFastBuffer::empty() const {
    return size() == 0;
}

size_t SFastBuffer::size() const {
    return data.size() - front;
}

const char* SFastBuffer::c_str() const {
    return data.data() + front;
}

void SFastBuffer::clear() {
    front = 0;
    nextToCheck = 0;
    headerLength = 0;
    contentLength = 0;
    data.clear();
}

void SFastBuffer::consumeFront(size_t bytes) {
    front += bytes;

    nextToCheck = front;
    headerLength = 0;
    contentLength = 0;

    // If we're all caught up, reset.
    if (front == data.size()) {
        clear();
    }
}

void SFastBuffer::append(const char* buffer, size_t bytes) {
    // When will we condense everything to the front of the buffer?
    // When:
    // 1. We're not already at the front of the buffer (this implies there's data in the buffer).
    // 2. We'd have to do a realloc anyway because our buffer's not big enough for the new string (including the
    //    existing consumed buffer).
    if (front && (data.capacity() - data.size() < bytes)) {
        memmove(&data[0], data.data() + front, size());
        data.resize(size());
        front = 0;

        // If the capacity is more than 4x the size we need, let's give some memory back.
        if (data.capacity() > (data.size() + bytes) * 4) {
            data.shrink_to_fit();
        }
    }

    // After the resize, we may or may not need to actually reallocate. We can append now and let the string
    // implementation decide if it needs more room.
    data.append(buffer, bytes);
}

SFastBuffer& SFastBuffer::operator+=(const string& rhs) {
    append(rhs.c_str(), rhs.size());
    return *this;
}

SFastBuffer& SFastBuffer::operator=(const string& rhs) {
    front = 0;
    nextToCheck = 0;
    headerLength = 0;
    contentLength = 0;
    data = rhs;
    return *this;
}

ostream& operator<<(ostream& os, const SFastBuffer& buf)
{
    os << buf.c_str();
    return os;
}
