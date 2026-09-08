/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SResolver.h
 * Path:    libstuff/SResolver.h
 * Pair:    SResolver.cpp
 *
 * INTENT
 *   Asynchronous, poll()-able DNS resolution: SResolve() starts a lookup on a detached
 *   thread (or resolves a literal IP inline) and hands back a shared SResolution that
 *   both sides can safely outlive each other on.
 *
 * OBJECTS
 *   SResolution - PENDING/RESOLVED/FAILED state machine; a self-pipe (getFD()) that
 *     becomes readable on completion; getAddr() valid once RESOLVED; complete() is
 *     called exactly once, from the resolving thread.
 *   SResolve()  - free function; starts the lookup and returns the SResolution.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent: `_`-prefixed private members, bare public members/methods.
 * ─────────────────────────────────────────────────────────────────────*/

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

    // The read end of the notification pipe. It becomes readable when the lookup finishes, and
    // stays that way. Poll this to be woken on completion rather than waiting out a timeout.
    int getFD() const;

    // Records the result and wakes anyone polling on the pipe. Called on the resolving thread,
    // exactly once.
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
