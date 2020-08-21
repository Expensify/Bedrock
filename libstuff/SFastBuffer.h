#pragma once
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

  private:
    size_t front;
    string data;
};
ostream& operator<<(ostream& os, const SFastBuffer& buf);
