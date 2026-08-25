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
    if (pipe(_pipeFD)) {
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

    // Closing the write end is the notification: it makes the read end poll POLLHUP.
    //
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
