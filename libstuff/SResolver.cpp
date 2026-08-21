#include "SResolver.h"

#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include <libstuff/libstuff.h>

// How many lookups can be in flight at once. Anything past this queues.
#define SRESOLVER_THREAD_COUNT 4

SResolver::Resolution::Resolution(const string& host)
    : host(host), _state(PENDING), _addr{}, _pipeFD{-1, -1}
{
    if (pipe(_pipeFD)) {
        STHROW("Failed to create pipe: " + to_string(errno) + " "s + strerror(errno));
    }

    // The reader polls this and drains it opportunistically, so it must never block.
    int flags = fcntl(_pipeFD[0], F_GETFL, 0);
    fcntl(_pipeFD[0], F_SETFL, flags | O_NONBLOCK);
}

SResolver::Resolution::~Resolution()
{
    if (_pipeFD[0] != -1) {
        close(_pipeFD[0]);
    }
    if (_pipeFD[1] != -1) {
        close(_pipeFD[1]);
    }
}

void SResolver::Resolution::drain()
{
    while (true) {
        char buffer[1];
        int result = read(_pipeFD[0], buffer, sizeof(buffer));
        if (result <= 0) {
            break;
        }
    }
}

void SResolver::Resolution::_complete(bool success, const sockaddr_in& addr)
{
    if (success) {
        _addr = addr;
    }

    // The state has to be visible before the notification is, or a reader woken by the pipe could
    // still see PENDING.
    _state.store(success ? RESOLVED : FAILED);

    // Wake anyone polling. There's exactly one of these per Resolution, so the pipe can't fill.
    const char byte = 1;
    if (write(_pipeFD[1], &byte, 1) != 1) {
        SWARN("Failed to notify completed resolution for '" << host << "': " << strerror(errno));
    }
}

SResolver& SResolver::getInstance()
{
    // Deliberately leaked. See the declaration.
    static SResolver* instance = new SResolver(SRESOLVER_THREAD_COUNT);
    return *instance;
}

SResolver::SResolver(size_t threadCount)
{
    for (size_t i = 0; i < threadCount; i++) {
        _threads.emplace_back(&SResolver::_workerFunc, this);
    }
}

SResolver::~SResolver()
{
    {
        lock_guard<mutex> lock(_mutex);
        _exit = true;
    }
    _cv.notify_all();
    for (auto& t : _threads) {
        t.join();
    }
}

shared_ptr<SResolver::Resolution> SResolver::resolve(const string& host)
{
    auto resolution = make_shared<Resolution>(host);

    // If we already know the answer, skip the pool entirely. This is the common case.
    sockaddr_in addr;
    if (SResolveHostCached(host, addr)) {
        resolution->_complete(true, addr);
        return resolution;
    }

    {
        lock_guard<mutex> lock(_mutex);
        _queue.push_back(resolution);
    }
    _cv.notify_one();

    return resolution;
}

void SResolver::_workerFunc()
{
    SInitialize("resolver");
    while (true) {
        shared_ptr<Resolution> resolution;
        {
            unique_lock<mutex> lock(_mutex);
            while (!_exit && _queue.empty()) {
                _cv.wait(lock);
            }
            if (_exit) {
                return;
            }
            resolution = move(_queue.front());
            _queue.pop_front();
        }

        // Outside the lock: this is the part that can take seconds. The Resolution is kept alive by
        // our own reference, so it doesn't matter if whoever asked for it has since given up.
        sockaddr_in addr;
        const bool success = SResolveHost(resolution->host, addr);
        resolution->_complete(success, addr);
    }
}
