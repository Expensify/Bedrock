#pragma once

template <typename T>
class SSynchronizedQueue {
  public:
    // Constructor / Destructor
    SSynchronizedQueue();
    ~SSynchronizedQueue();

    // Explicitly delete copy constructor so it can't accidentally get called.
    SSynchronizedQueue(const SSynchronizedQueue& other) = delete;

    // Wait for something to be put onto the queue
    int preSelect(fd_map& fdm);
    void postSelect(fd_map& fdm, int bytesToRead = 1);

    // Synchronized interface to add/remove work
    //void push(T rhs);
    void push(T&& rhs);
    T pop();

    bool empty();

  protected:
    // Private state
    list<T> _queue;
    recursive_mutex _queueMutex;
    int _pipeFD[2] = {-1, -1};
};

template<typename T>
SSynchronizedQueue<T>::SSynchronizedQueue() {
    // Initialize
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
int SSynchronizedQueue<T>::preSelect(fd_map& fdm) {
    // Put the read-side of the pipe into the fd set.
    // **NOTE: This is *not* synchronized.  All threads use the same pipes.
    //         All threads use *different* fd_maps, though so we don't have
    //         to worry about contention inside FDSet.
    SFDset(fdm, _pipeFD[0], SREADEVTS);
    return _pipeFD[0];
}

/*
template<typename T>
void SSynchronizedQueue<T>::push(T rhs) {
    SAUTOLOCK(_queueMutex);
    // Just add to the queue
    _queue.push_back(rhs);

    // Write arbitrary buffer to the pipe so any subscribers will
    // be awoken.
    // **NOTE: 1 byte so write is atomic.
    SASSERT(write(_pipeFD[1], "A", 1));
}
*/

template<typename T>
void SSynchronizedQueue<T>::push(T&& rhs) {
    SAUTOLOCK(_queueMutex);
    // Just add to the queue
    _queue.push_back(move(rhs));

    // Write arbitrary buffer to the pipe so any subscribers will
    // be awoken.
    // **NOTE: 1 byte so write is atomic.
    SASSERT(write(_pipeFD[1], "A", 1));
}

template<typename T>
T SSynchronizedQueue<T>::pop() {
    SAUTOLOCK(_queueMutex);
    if (!_queue.empty()) {
        // Take the first
        T item = move(_queue.front());
        _queue.pop_front();
        return item;
    }
    // TODO: Better exception.
    throw "No items!";
}

template<typename T>
bool SSynchronizedQueue<T>::empty() {
    SAUTOLOCK(_queueMutex);
    // Just return the state of the queue
    return _queue.empty();
}

template<typename T>
void SSynchronizedQueue<T>::postSelect(fd_map& fdm, int bytesToRead) {
    // Caller determines the bytes to read.  If a consumer can
    // only process one item then it will only read 1 byte.  If
    // the pipe has more data to read it will continue to "fire"
    // so other threads also subscribing will pick up work.
    if (SFDAnySet(fdm, _pipeFD[0], SREADEVTS)) {
        char readbuffer[bytesToRead];
        read(_pipeFD[0], readbuffer, sizeof(readbuffer));
    }
}

