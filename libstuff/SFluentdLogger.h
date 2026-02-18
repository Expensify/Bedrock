#pragma once

#include <atomic>
#include <string>
#include <thread>

#include <libstuff/SRingBuffer.h>

using namespace std;

class SFluentdLogger {
public:
    SFluentdLogger(const string& host, int port);
    ~SFluentdLogger();

    bool log(string&& json);

private:
    int openSocket();
    bool sendAll(int fd, const string& data);
    void senderLoop();

    string host;
    int port;
    atomic<bool> running{false};
    SRingBuffer<string, SRINGBUFFER_DEFAULT_CAPACITY> buffer;
    thread senderThread;
};
