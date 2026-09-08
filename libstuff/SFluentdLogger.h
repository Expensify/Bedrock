/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SFluentdLogger.h
 * Path:    libstuff/SFluentdLogger.h
 * Pair:    SFluentdLogger.cpp
 *
 * INTENT
 *   Async, non-blocking log shipper to a Fluentd TCP endpoint: callers push JSON log
 *   records onto a lock-free ring buffer (never blocking the calling thread), a single
 *   background sender thread drains it and forwards to Fluentd, falling back to
 *   syslog() on send failure.
 *
 * OBJECTS
 *   FluentdLogRecord - one queued record: syslog priority + JSON payload string.
 *   SFluentdLogger    - log() public API; openSocket/sendAll/senderLoop private
 *     implementation; `instance`/`tag` are process-wide inline statics (global
 *     singleton handle + a caller-set tag), not per-instance state.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent, but `instance` and `tag` are bare public statics acting as a global
 *   singleton, a different pattern from this class's own instance members
 *   (host/port/buffer/senderThread), which are conventionally private.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include <memory>
#include <netinet/in.h>
#include <string>
#include <thread>

#include <libstuff/SRingBuffer.h>

using namespace std;

struct FluentdLogRecord
{
    int priority = 0;
    string json;
};

class SFluentdLogger {
public:
    SFluentdLogger(const string& host, in_port_t port);
    ~SFluentdLogger();

    bool log(int priority, string&& json);

    inline static unique_ptr<SFluentdLogger> instance;
    inline static string tag;

private:
    int openSocket();
    bool sendAll(int fd, const string& data);
    void senderLoop();

    string host;
    in_port_t port;
    unique_ptr<SRingBuffer<FluentdLogRecord, SRINGBUFFER_DEFAULT_CAPACITY>> buffer;
    thread senderThread;
};
