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
    data = "";
}

void SFastBuffer::consumeFront(size_t bytes) {
    // TODO
}

void SFastBuffer::append(const char* buffer, size_t bytes) {
    // TODO
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
