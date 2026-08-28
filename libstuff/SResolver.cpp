#include "SResolver.h"

#include <cstring>
#include <fcntl.h>
#include <system_error>
#include <thread>
#include <unistd.h>

#include <libstuff/libstuff.h>

SResolution::SResolution(const string& host)
    : host(host), _state(PENDING), _addr{}, _pipeFD{-1, -1}
{
    // O_CLOEXEC because a child that inherits the write end holds the pipe open after we close
    // ours, and the hangup half of the notification never arrives.
    if (pipe2(_pipeFD, O_CLOEXEC)) {
        STHROW("Failed to create pipe: " + to_string(errno) + " "s + strerror(errno));
    }
}

SResolution::~SResolution()
{
    if (_pipeFD[0] != -1) {
        close(_pipeFD[0]);
    }
    if (_pipeFD[1] != -1) {
        close(_pipeFD[1]);
    }
}

SResolution::State SResolution::getState() const
{
    return _state.load();
}

const sockaddr_in& SResolution::getAddr() const
{
    return _addr;
}

int SResolution::getFD() const
{
    return _pipeFD[0];
}

void SResolution::complete(bool success, const sockaddr_in& addr)
{
    if (success) {
        _addr = addr;
    }

    // The state has to be visible before the notification is, or a reader woken by the pipe could still see PENDING.
    _state.store(success ? RESOLVED : FAILED);

    // Notify twice over: the byte is what a reader polls on, and closing the write end both hands
    // back an fd and leaves the pipe hung up, so a reader that isn't looking yet can't miss it. The
    // byte matters because a forked child can still be holding its own copy of the write end, in
    // which case there's no hangup to see. Nobody consumes the byte, so it stays readable.
    const char byte = 1;
    if (write(_pipeFD[1], &byte, 1) != 1) {
        SWARN("Failed to notify completed resolution for '" << host << "': " << strerror(errno));
    }

    // The read end has to stay open until this object is destroyed. A poll thread can be
    // registering it at this very moment, and closing it here would free the number for some other
    // thread to reuse, leaving that poll waiting on an unrelated fd.
    close(_pipeFD[1]);
    _pipeFD[1] = -1;
}

shared_ptr<SResolution> SResolve(const string& host)
{
    // The thread holds its own reference, so it doesn't matter if whoever asked for this has given
    // up by the time the lookup finishes.
    auto resolution = make_shared<SResolution>(host);

    // A literal address needs no lookup.
    sockaddr_in addr;
    if (SIPToAddr(host, addr)) {
        resolution->complete(true, addr);
        return resolution;
    }

    try {
        thread([resolution]() {
            // Deliberately not SInitialize(): that registers a single global buffer as this thread's alternate signal stack.
            SLogSetThreadName("resolver");

            sockaddr_in threadAddr;
            const bool success = SResolveHost(resolution->host, threadAddr);
            resolution->complete(success, threadAddr);
        }).detach();
    } catch (const system_error& e) {
        STHROW("Couldn't start a thread to resolve '" + host + "': " + e.what());
    }

    return resolution;
}
