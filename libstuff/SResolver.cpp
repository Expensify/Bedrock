#include "SResolver.h"

#include <cstring>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

#include <libstuff/libstuff.h>

const int S_RESOLVE_MAX_IN_FLIGHT = 500;

// Lookups running right now. A lookup only lasts as long as getaddrinfo() does, so under normal
// conditions this sits at zero or one.
static atomic<int> _inFlight(0);

int SResolveInFlight()
{
    return _inFlight.load();
}

SResolution::SResolution(const string& host)
    : host(host), _state(PENDING), _addr{}, _pipeFD{-1, -1}
{
    if (pipe(_pipeFD)) {
        STHROW("Failed to create pipe: " + to_string(errno) + " "s + strerror(errno));
    }

    // The reader polls this and drains it opportunistically, so it must never block.
    int flags = fcntl(_pipeFD[0], F_GETFL, 0);
    fcntl(_pipeFD[0], F_SETFL, flags | O_NONBLOCK);
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

void SResolution::drain()
{
    while (true) {
        char buffer[1];
        int result = read(_pipeFD[0], buffer, sizeof(buffer));
        if (result <= 0) {
            break;
        }
    }
}

void SResolution::complete(bool success, const sockaddr_in& addr)
{
    if (success) {
        _addr = addr;
    }

    // The state has to be visible before the notification is, or a reader woken by the pipe could
    // still see PENDING.
    _state.store(success ? RESOLVED : FAILED);

    // Wake anyone polling. There's exactly one of these per resolution, so the pipe can't fill.
    const char byte = 1;
    if (write(_pipeFD[1], &byte, 1) != 1) {
        SWARN("Failed to notify completed resolution for '" << host << "': " << strerror(errno));
    }
}

shared_ptr<SResolution> SResolve(const string& host)
{
    if (_inFlight.load() >= S_RESOLVE_MAX_IN_FLIGHT) {
        STHROW("Too many DNS lookups in flight (" + to_string(S_RESOLVE_MAX_IN_FLIGHT) + "), refusing to start another");
    }

    auto resolution = make_shared<SResolution>(host);
    _inFlight++;

    // The thread holds its own reference, so it doesn't matter if whoever asked for this has given
    // up by the time the lookup finishes.
    thread([resolution]() {
        // Among other things this blocks signals, which a transient thread must do so it can't
        // swallow one meant for the signal handling thread.
        SInitialize("resolver");

        sockaddr_in addr;
        const bool success = SResolveHost(resolution->host, addr);
        resolution->complete(success, addr);
        _inFlight--;
    }).detach();

    return resolution;
}
