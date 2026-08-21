#pragma once
#include <atomic>
#include <memory>
#include <netinet/in.h>
#include <string>

using namespace std;

// The result of a single DNS lookup running on its own thread.
//
// This is handed out as a shared_ptr and co-owned by the caller and the thread performing the
// lookup, which is what makes abandonment safe: a caller that gives up (because its request timed
// out, say) just drops its reference, and the thread finishes writing into storage that only it
// still references. There's no cancellation because getaddrinfo() has none to offer.
class SResolution {
public:
    enum State { PENDING, RESOLVED, FAILED };

    SResolution(const string& host);
    ~SResolution();

    // Not copyable or movable; the resolving thread holds a pointer to this.
    SResolution(const SResolution&) = delete;
    SResolution& operator=(const SResolution&) = delete;

    State getState() const
    {
        return _state.load();
    }

    // Only meaningful once the state is RESOLVED.
    const sockaddr_in& getAddr() const
    {
        return _addr;
    }

    // The read end of the notification pipe. A byte becomes readable here exactly once, when the
    // lookup finishes. Poll this to be woken on completion rather than waiting out a timeout.
    int getFD() const
    {
        return _pipeFD[0];
    }

    // Consume the notification byte. Safe to call when there's nothing to read.
    void drain();

    // Records the result and wakes anyone polling on the pipe. Called on the resolving thread.
    void complete(bool success, const sockaddr_in& addr);

    const string host;

private:
    atomic<State> _state;
    sockaddr_in _addr;
    int _pipeFD[2];
};

// Starts resolving `host` on a detached thread and returns immediately.
//
// Throws if too many lookups are already in flight. That only happens when the resolver has stopped
// answering, since a lookup can occupy its thread for as long as glibc is willing to retry, and the
// cap keeps a resolver outage from turning into unbounded thread growth. Callers already handle a
// throwing socket constructor by failing the request.
shared_ptr<SResolution> SResolve(const string& host);

// How many lookups may be in flight at once before SResolve starts throwing.
extern const int S_RESOLVE_MAX_IN_FLIGHT;

// Number of lookups currently in flight. Exists for tests.
int SResolveInFlight();
