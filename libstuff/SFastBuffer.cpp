#include <libstuff/libstuff.h>

atomic<size_t> SFastBuffer::bufferIDs(0);

SFastBuffer::SFastBuffer() : front(0), bufferID(bufferIDs++), _enableLogging(false) {
}

SFastBuffer::SFastBuffer(const string& str) : front(0), data(str), bufferID(bufferIDs++), _enableLogging(false) {
}

void SFastBuffer::enableLogging(bool enable, const string& pfx) {
    if (enable != _enableLogging) {
        prefix = pfx;
        _enableLogging = enable;
        string fake;
        if (enable && size()) {
            // If we turn this on with bytes already in the buffer, let's make sure they don't throw off the rest of
            // the count.
            _insertionTimes.emplace_back(chrono::steady_clock::now(), size(), 0);
            fake = ", adding " + to_string(size()) + " fake bytes";
        }
        if (!enable) {
            // Clear any outstanding logging data.
            _insertionTimes.clear();
        }
        SINFO("Logging " << (enable ? "enabled" : "disabled") << " for buffer " << bufferID << " (" << prefix << ")" << fake << ".");
    }
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

    if(_enableLogging) {
        _insertionTimes.clear();
    }
}

void SFastBuffer::consumeFront(size_t bytes) {
    if (_enableLogging) {
        // Timing info.
        auto timestamp = chrono::steady_clock::now();
        size_t remainingBytes = bytes;
        while (_insertionTimes.size() && remainingBytes) {
            auto& first = _insertionTimes.front();
            auto consumed = min(first.length, remainingBytes);
            SINFO("Consumed " << consumed << " bytes from buffer " << bufferID << " (" << prefix << ") inserted "
                  << chrono::duration_cast<chrono::milliseconds>(timestamp - first.timestamp).count()
                  << "ms ago (behind " << first.sizeAtInsertion << " bytes), " << (size() - consumed) << " bytes remaining.");
            if (first.length > remainingBytes) {
                first.length -= remainingBytes;
                break;
            } else {
                remainingBytes -= first.length;
                // Remove the whole thing.
                _insertionTimes.pop_front();
            }
        }
    }

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

    // Record how many bytes we just inserted, and when.
    if (_enableLogging) {
        _insertionTimes.emplace_back(chrono::steady_clock::now(), bytes, size());
        SINFO("Inserted " << bytes << " bytes into buffer " << bufferID << " (" << prefix << ") behind " << size() << " bytes.");
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
    clear();
    append(rhs.c_str(), rhs.size());
    return *this;
}

ostream& operator<<(ostream& os, const SFastBuffer& buf)
{
    os << buf.c_str();
    return os;
}
