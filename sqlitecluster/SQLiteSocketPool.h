#include <libstuff/STCPManager.h>
#include <chrono>

class SQLiteSocketPool {
  public:
    SQLiteSocketPool(const string& host);
    ~SQLiteSocketPool();

    // Returns an existing or new socket.
    unique_ptr<STCPManager::Socket> getSocket();

    // Makes an existing socket available to be used again.
    void returnSocket(unique_ptr<STCPManager::Socket>&& s);

    // The hostname for the socket.
    const string host;

    // The timeout after which a socket is closed if not used.
    const chrono::steady_clock::duration timeout = 10s;
  private:

    // Returns the number of sockets removed.
    size_t _pruneOldSockets();

    void _timeoutThreadFunc();
    atomic<bool> exit = false;
    mutex _poolMutex;
    list<pair<chrono::steady_clock::time_point, unique_ptr<STCPManager::Socket>>> _sockets;
    thread _timeoutThread;
};
