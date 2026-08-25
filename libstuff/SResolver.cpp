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

    // The state has to be visible before the notification is, or a reader woken by the pipe could
    // still see PENDING.
    _state.store(success ? RESOLVED : FAILED);

    // Closing the write end is the notification: it makes the read end poll POLLHUP, which is part
    // of SREADEVTS, and it hands back one of the two file descriptors instead of holding it for as
    // long as the socket lives. It also stays hung up, so a reader that isn't looking yet can't
    // miss it.
    //
    // The read end has to stay open until this object is destroyed. A poll thread can be
    // registering it at this very moment, and closing it here would free the number for some other
    // thread to reuse, leaving that poll waiting on an unrelated fd.
    close(_pipeFD[1]);
    _pipeFD[1] = -1;
}

shared_ptr<SResolution> SResolve(const string& host)
{
    auto resolution = make_shared<SResolution>(host);

    // A literal address needs no lookup, so it would be a whole thread spent on nothing -- and
    // worse, the caller would race it and could end up deferring a socket that had nothing to wait
    // for. Answer inline instead.
    sockaddr_in addr;
    if (SIPToAddr(host, addr)) {
        resolution->complete(true, addr);
        return resolution;
    }

    // The thread holds its own reference, so it doesn't matter if whoever asked for this has given
    // up by the time the lookup finishes.
    //
    // A failed spawn arrives as a system_error, which callers of a socket constructor have no
    // reason to expect. Convert it, so running out of threads fails the request instead of
    // unwinding past everyone's catch.
    try {
        thread([resolution]() {
            // Deliberately not SInitialize(): that registers a single global buffer as this thread's
            // alternate signal stack, which is fine for a handful of long-lived threads but not for one
            // of these per request, all sharing the same 64KB. We only need the two things it would
            // give us that matter here, and one comes for free -- a new thread inherits the signal mask
            // of the thread that spawned it, which has already blocked everything the signal handling
            // thread wants to receive.
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
