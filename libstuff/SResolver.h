#pragma once
#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <vector>

using namespace std;

// A small fixed pool of threads that perform DNS lookups, so that threads which can't afford to
// block don't have to call getaddrinfo() themselves.
//
// The pool is deliberately bounded. Spawning a thread per lookup would turn a DNS outage into
// unbounded thread growth, which is a worse version of the stall this exists to avoid. With a fixed
// pool, a hung resolver occupies one slot and everything else queues behind it.
class SResolver {
  public:
    // The result of a single lookup. This is handed out as a shared_ptr and co-owned by the caller
    // and the worker performing the lookup, which is what makes abandonment safe: a caller that
    // gives up (because its request timed out, say) just drops its reference, and the worker
    // finishes writing into storage that only it still references.
    class Resolution {
      public:
        enum State { PENDING, RESOLVED, FAILED };

        Resolution(const string& host);
        ~Resolution();

        // Not copyable or movable; the worker holds a pointer to this.
        Resolution(const Resolution&) = delete;
        Resolution& operator=(const Resolution&) = delete;

        State getState() const { return _state.load(); }

        // Only meaningful once the state is RESOLVED.
        const sockaddr_in& getAddr() const { return _addr; }

        // The read end of the notification pipe. A byte becomes readable here exactly once, when the
        // lookup finishes. Register this with poll() to be woken on completion rather than waiting
        // out a timeout.
        int getFD() const { return _pipeFD[0]; }

        // Consume the notification byte. Safe to call when there's nothing to read.
        void drain();

        const string host;

      private:
        friend class SResolver;

        // Records the result and wakes anyone polling on the pipe. Called on a worker thread.
        void _complete(bool success, const sockaddr_in& addr);

        atomic<State> _state;
        sockaddr_in _addr;
        int _pipeFD[2];
    };

    // The process-wide pool. Intentionally never destroyed: a worker stuck inside getaddrinfo()
    // can't notice a shutdown flag, so joining it at exit could hang the process for as long as the
    // resolver takes to give up.
    static SResolver& getInstance();

    // Queue a lookup. Returns immediately. If the host can be answered from cache or is a raw IP,
    // the returned Resolution is already RESOLVED and no worker is involved.
    shared_ptr<Resolution> resolve(const string& host);

    SResolver(size_t threadCount);
    ~SResolver();

  private:
    void _workerFunc();

    list<shared_ptr<Resolution>> _queue;
    mutex _mutex;
    condition_variable _cv;
    bool _exit = false;
    vector<thread> _threads;
};
