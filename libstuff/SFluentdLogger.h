#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <libstuff/SRingBuffer.h>

using namespace std;

struct FluentdLogRecord {
    int priority;
    string json;
};

class SFluentdLogger {
public:
    SFluentdLogger(const string& host, int port);
    ~SFluentdLogger();

    bool log(int priority, string&& json);

private:
    int openSocket();
    bool sendAll(int fd, const string& data);
    void senderLoop();

    string host;
    int port;
    atomic<bool> running{false};
    unique_ptr<SRingBuffer<FluentdLogRecord, SRINGBUFFER_DEFAULT_CAPACITY>> buffer;
    thread senderThread;
};
