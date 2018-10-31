#pragma once

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
    void postPoll(fd_map& fdm, int bytesToRead = 1);

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
    int _pipeFD[2] = {-1, -1};
};

template<typename T>
SSynchronizedQueue<T>::SSynchronizedQueue() {
    // Open up a pipe for communication and set the non-blocking reads.
    SASSERT(0 == pipe(_pipeFD));
    int flags = fcntl(_pipeFD[0], F_GETFL, 0);
    fcntl(_pipeFD[0], F_SETFL, flags | O_NONBLOCK);
}

template<typename T>
SSynchronizedQueue<T>::~SSynchronizedQueue() {
    if (_pipeFD[0] != -1) {
        close(_pipeFD[0]);
    }
    if (_pipeFD[1] != -1) {
        close(_pipeFD[0]);
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
void SSynchronizedQueue<T>::postPoll(fd_map& fdm, int bytesToRead) {
    // Caller determines the bytes to read.  If a consumer can only process one item then it will only read 1 byte. If
    // the pipe has more data to read it will continue to "fire" so other threads also subscribing will pick up work.
    if (SFDAnySet(fdm, _pipeFD[0], SREADEVTS)) {
        char readbuffer[bytesToRead];
        read(_pipeFD[0], readbuffer, sizeof(readbuffer));
    }
}

template<typename T>
bool SSynchronizedQueue<T>::empty() const {
    SAUTOLOCK(_queueMutex);
    return _queue.empty();
}

template<typename T>
const T& SSynchronizedQueue<T>::front() const {
    SAUTOLOCK(_queueMutex);
    if (!_queue.empty()) {
        return _queue.front();
    }
    throw out_of_range("No commands");
}

template<typename T>
T SSynchronizedQueue<T>::pop() {
    SAUTOLOCK(_queueMutex);
    if (!_queue.empty()) {
        T item = move(_queue.front());
        _queue.pop_front();
        return item;
    }
    throw out_of_range("No commands");
}

template<typename T>
void SSynchronizedQueue<T>::push(T&& rhs) {
    SAUTOLOCK(_queueMutex);
    // Just add to the queue
    _queue.push_back(move(rhs));

    // Write arbitrary buffer to the pipe so any subscribers will be awoken.
    // **NOTE: 1 byte so write is atomic.
    SASSERT(write(_pipeFD[1], "A", 1));
}

template<typename T>
size_t SSynchronizedQueue<T>::size() const {
    SAUTOLOCK(_queueMutex);
    return _queue.size();
}

template<typename T>
void SSynchronizedQueue<T>::each(const function<void (T&)> f) {
    SAUTOLOCK(_queueMutex);
    for_each(_queue.begin(), _queue.end(), f);
}
