#pragma once
#include <chrono>

class SFastBuffer {
  public:
    SFastBuffer();
    SFastBuffer(const string& str);
    bool empty() const;
    size_t size() const;
    const char* c_str() const;
    void clear();
    void consumeFront(size_t bytes);
    void append(const char* buffer, size_t bytes);
    SFastBuffer& operator+=(const string& rhs);
    SFastBuffer& operator=(const string& rhs);

    void enableLogging(bool enable, const string& pfx = "");

  private:
    size_t front;
    string data;

    // Whenever we insert a block of data, we record the timestamp and length.
    struct Timing {
        Timing(chrono::time_point<chrono::steady_clock> ts, size_t l, size_t sai) : 
          timestamp(ts), length(l), sizeAtInsertion(sai) { }
        chrono::time_point<chrono::steady_clock> timestamp;
        size_t length;
        size_t sizeAtInsertion; // not-yet-consumed size, not including the new data being inserted.
    };

    size_t bufferID;
    list<Timing> _insertionTimes;

    static atomic<size_t> bufferIDs;
    bool _enableLogging;
    string prefix;
};
ostream& operator<<(ostream& os, const SFastBuffer& buf);
