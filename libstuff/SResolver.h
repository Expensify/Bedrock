#pragma once
#include <atomic>
#include <memory>
#include <netinet/in.h>
#include <string>

using namespace std;

// Calling SResolve returns an SResolution that runs SResolveHost in its own thread.
// Both the thread and the caller get a shared_ptr to the SResolution object, so either
// can complete and be destroyed safely while the other continues. A literal IP needs no
// thread and comes back already resolved.
// This can be poll()'ed upon for completion with `getFD()`.
class SResolution {
public:
    enum State { PENDING, RESOLVED, FAILED };

    SResolution(const string& host);
    ~SResolution();

    // Not copyable or movable, the resolving thread holds a pointer to this.
    SResolution(const SResolution&) = delete;
    SResolution& operator=(const SResolution&) = delete;

    State getState() const;

    // Only meaningful once the state is RESOLVED.
    const sockaddr_in& getAddr() const;

    // The read end of the notification pipe. It hangs up when the lookup finishes, and stays that
    // way. Poll this to be woken on completion rather than waiting out a timeout.
    int getFD() const;

    // Records the result and hangs up the pipe to wake anyone polling on it. Called on the
    // resolving thread, exactly once.
    void complete(bool success, const sockaddr_in& addr);

    const string host;

private:
    atomic<State> _state;
    sockaddr_in _addr;
    int _pipeFD[2];
};

// Starts resolving `host` on a detached thread and returns immediately.
// A literal address is answered inline without a thread.
shared_ptr<SResolution> SResolve(const string& host);
