#pragma once

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/ssl.h>
#include <mbedtls/net_sockets.h>
#include <mbedtls/x509_crt.h>
#include <string>
#include <mutex>
#include <atomic>

using namespace std;
class SFastBuffer;

class SSSLState {
  public:

    SSSLState(const string& hostname);
    SSSLState(const string& hostname, int socket);
    ~SSSLState();

    int send(const char* buffer, int length);
    int send(const SFastBuffer& buffer);
    bool sendConsume(SFastBuffer& sendBuffer);
    int recv(char* buffer, int length);
    bool recvAppend(SFastBuffer& recvBuffer);

    mbedtls_entropy_context ec;
    mbedtls_ctr_drbg_context ctr_drbg;
    mbedtls_ssl_config conf;
    mbedtls_ssl_context ssl;
    mbedtls_net_context net_ctx;
    static mbedtls_x509_crt _cacert;

  private:
    static mutex _certificateMutex;
    static atomic<bool> _certificatesLoadingComplete;
    static atomic<bool> _certificatesLoaded;
    static void _loadCerts();
};
