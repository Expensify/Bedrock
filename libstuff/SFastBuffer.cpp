#include <libstuff/libstuff.h>

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
    // If we're going to need to realloc anyway, because we're running out of space in our string, condense everything
    // to the front.
    if (data.capacity() - data.size() > bytes) {
        data = string(data, front);
        front = 0;
    }

    // And do the append.
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
