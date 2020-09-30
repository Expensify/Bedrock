// This is a class that's just a std::map that's thread-safe. It does not implement every std::map function, please add
// them as needed.
template <typename T, typename U>
class SynchronizedMap {
  public:

    // A LockGuard is like a std::lock_guard, but for the entire object. It allows the whole thing to be locked so
    // that the caller can perform multiple operations atomically. A LockGuard can easily be created for a
    // SynchronizedMap by calling `scopedLock()` on the map.
    friend class LockGuard;
    class LockGuard {
      public:
        LockGuard(SynchronizedMap& map) : _map(map) {
            _map._m.lock();
        }
        ~LockGuard() {
            _map._m.unlock();
        }
      private:
        SynchronizedMap& _map;
    };

    // These are just passed through to the underlying object, but with the lock locked first.
    auto begin() {
        lock_guard <decltype(_m)> lock(_m);
        return _data.begin();
    }
    auto clear() {
        lock_guard <decltype(_m)> lock(_m);
        return _data.clear();
    }

    // Note: Key is copied, value is moved.
    template <typename V, typename W>
    auto emplace(V& first, W&& second) {
        lock_guard <decltype(_m)> lock(_m);
        return _data.emplace(first, move(second));
    }
    auto empty() {
        lock_guard <decltype(_m)> lock(_m);
        return _data.empty();
    }
    auto end() {
        lock_guard <decltype(_m)> lock(_m);
        return _data.end();
    }
    template <typename V>
    auto erase(V item) {
        lock_guard <decltype(_m)> lock(_m);
        return _data.erase(item);
    }
    template <typename V>
    auto find(V item) {
        lock_guard <decltype(_m)> lock(_m);
        return _data.find(item);
    }
    auto size() {
        lock_guard <decltype(_m)> lock(_m);
        return _data.size();
    }
    LockGuard scopedLock() {
        return LockGuard(*this);
    }
    
  private :
    map<T, U> _data;
    recursive_mutex _m;
};
