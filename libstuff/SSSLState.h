#pragma once

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/ssl.h>
#include <string>

using namespace std;
class SFastBuffer;

struct SSSLState {
    int s;
    mbedtls_entropy_context ec;
    mbedtls_ctr_drbg_context ctr_drbg;
    mbedtls_ssl_config conf;
    mbedtls_ssl_context ssl;

    SSSLState(int s, const string& hostname);
    ~SSSLState();

    int send(const char* buffer, int length);
    int send(const SFastBuffer& buffer);
    bool sendConsume(SFastBuffer& sendBuffer);
    bool sendAll(const string& buffer);
    int recv(char* buffer, int length);
    bool recvAppend(SFastBuffer& recvBuffer);
    void shutdown();
};
