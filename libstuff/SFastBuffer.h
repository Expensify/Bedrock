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
