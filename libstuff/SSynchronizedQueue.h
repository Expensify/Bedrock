#pragma once

#include <cstring>
#include <libstuff/libstuff.h>
#include <unistd.h>
#include <fcntl.h>

template <typename T>
class SSynchronizedQueue {
  public:
    // Constructor/Destructor
    SSynchronizedQueue();
    ~SSynchronizedQueue();

    // Explicitly delete copy constructor so it can't accidentally get called.
    SSynchronizedQueue(const SSynchronizedQueue& other) = delete;

    // This queue can be watched by a `poll` loop. These functions are called with an fd_map to prepare for/handle
    // activity from polling.
    void prePoll(fd_map& fdm);
    void postPoll(fd_map& fdm);

    // Erases all elements from the container.
    void clear() noexcept;

    // Returns true if the queue is empty.
    bool empty() const;

    // Return a const reference to the front item in the list, for inspection.
    // throws out_of_range if nothing is available.
    virtual const T& front() const;

    // Get an item off the queue.
    virtual T pop();

    // Push an item onto the queue, by move.
    virtual void push(T&& rhs);

    // Returns the queue's size.
    size_t size() const;

    // Apply a lambda to each item in the queue.
    void each(const function<void (T&)> f);

  protected:
    list<T> _queue;
    mutable recursive_mutex _queueMutex;

    // You may be wondering why we use a pipe instead of a condition variable
    // for alerting threads that work is available. That's because we are treating
    // queue activity like network activity in BedrockServer. This means that we
    // treat the queue as if it was a network socket and we use the pipe to know
    // if there are "unread bytes on the socket" that is to say -- work available --
    // in the queue.
    // This is kind of weird but prevents threads from waiting for a the poll()
    // timeout on network activity. Since queue activity also causes poll() to return
    // if there is work in the queue.
    int _pipeFD[2] = {-1, -1};
};

template<typename T>
SSynchronizedQueue<T>::SSynchronizedQueue() {
    // Open up a pipe for communication and set the non-blocking reads.
    int result = pipe(_pipeFD);
    if (result == -1) {
        STHROW("Failed to create pipe: " + to_string(errno) + " "s + strerror(errno));
    }
    int flags = fcntl(_pipeFD[0], F_GETFL, 0);
    fcntl(_pipeFD[0], F_SETFL, flags | O_NONBLOCK);
}

template<typename T>
SSynchronizedQueue<T>::~SSynchronizedQueue() {
    if (_pipeFD[0] != -1) {
        close(_pipeFD[0]);
    }
    if (_pipeFD[1] != -1) {
        close(_pipeFD[1]);
    }
}

template<typename T>
void SSynchronizedQueue<T>::prePoll(fd_map& fdm) {
    // Put the read-side of the pipe into the fd set.
    // **NOTE: This is *not* synchronized.  All threads use the same pipes. All threads use *different* fd_maps, though
    //         so we don't have to worry about contention inside FDSet.
    SFDset(fdm, _pipeFD[0], SREADEVTS);
}

template<typename T>
void SSynchronizedQueue<T>::postPoll(fd_map& fdm) {
    // Check if there is anything to read off of the pipe, if there is, empty it.
    if (SFDAnySet(fdm, _pipeFD[0], SREADEVTS)) {
        // Read until there is nothing left to read.
        while (true) {
            char readbuffer[1];
            int ret = read(_pipeFD[0], readbuffer, sizeof(readbuffer));

            // Since the pipe is set to non-blocking reads, read() will return -1
            // when there is no data to read  and will set errno to EAGAIN/EWOULDBLOCK
            // otherwise it will 0 when we have read it all
            if (ret <= 0 && errno == EWOULDBLOCK) {
                break;
            } else if (ret == -1) {
                STHROW("Failed to read from pipe");
            }
        }
    }
}

template<typename T>
void SSynchronizedQueue<T>::clear() noexcept {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    _queue.clear();
}

template<typename T>
bool SSynchronizedQueue<T>::empty() const {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    return _queue.empty();
}

template<typename T>
const T& SSynchronizedQueue<T>::front() const {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    if (!_queue.empty()) {
        return _queue.front();
    }
    throw out_of_range("No commands");
}

template<typename T>
T SSynchronizedQueue<T>::pop() {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    if (!_queue.empty()) {
        T item = move(_queue.front());
        _queue.pop_front();
        return item;
    }
    throw out_of_range("No commands");
}

template<typename T>
void SSynchronizedQueue<T>::push(T&& rhs) {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    // Just add to the queue
    _queue.push_back(move(rhs));

    // Write arbitrary buffer to the pipe so any subscribers will be awoken.
    // **NOTE: 1 byte so write is atomic.
    SASSERT(write(_pipeFD[1], "A", 1));
}

template<typename T>
size_t SSynchronizedQueue<T>::size() const {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    return _queue.size();
}

template<typename T>
void SSynchronizedQueue<T>::each(const function<void (T&)> f) {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    for_each(_queue.begin(), _queue.end(), f);
}
