#pragma once

#include <atomic>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <thread>

#include <libstuff/SRingBuffer.h>

using namespace std;

const char* SPriorityName(int priority);

struct FluentdLogRecord
{
    int priority;
    string json;
};

class SFluentdLogger {
public:
    SFluentdLogger(const string& host, in_port_t port);
    ~SFluentdLogger();

    bool log(int priority, string&& json);

private:
    int openSocket();
    bool sendAll(int fd, const string& data);
    void senderLoop();

    string host;
    in_port_t port;
    atomic<bool> running{false};
    unique_ptr<SRingBuffer<FluentdLogRecord, SRINGBUFFER_DEFAULT_CAPACITY>> buffer;
    thread senderThread;
};
