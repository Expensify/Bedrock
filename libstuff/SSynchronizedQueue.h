#pragma once

template <typename T>
class SSynchronizedQueue {
  public:
    // Constructor/Destructor
    SSynchronizedQueue();
    ~SSynchronizedQueue();

    // Explicitly delete copy constructor so it can't accidentally get called.
    SSynchronizedQueue(const SSynchronizedQueue& other) = delete;

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
};

template<typename T>
SSynchronizedQueue<T>::SSynchronizedQueue() {
}

template<typename T>
SSynchronizedQueue<T>::~SSynchronizedQueue() {
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
