#pragma once
#include <libstuff/STCPManager.h>
#include <libstuff/SSocketPool.h>

class SMultiSocketPool {
  public:
    SMultiSocketPool();
    ~SMultiSocketPool();

    // Returns an existing or new socket.
    unique_ptr<STCPManager::Socket> getSocket(const string& host);

    // Makes an existing socket available to be used again.
    void returnSocket(unique_ptr<STCPManager::Socket>&& s, string host);
  private:
    mutex _poolMutex;
    map<string, unique_ptr<SSocketPool>> _pools;
};
