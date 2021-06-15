#include "SFastBuffer.h"

#include <cstring>

SFastBuffer::SFastBuffer() : front(0) {
}

SFastBuffer::SFastBuffer(const string& str) : front(0), data(str) {
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
    data.clear();
}

void SFastBuffer::consumeFront(size_t bytes) {
    front += bytes;

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
    data = rhs;
    return *this;
}

ostream& operator<<(ostream& os, const SFastBuffer& buf)
{
    os << buf.c_str();
    return os;
}
