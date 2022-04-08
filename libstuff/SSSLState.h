#pragma once

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/ssl.h>
#include <string>

using namespace std;

class SX509;
class SFastBuffer;

struct SSSLState {
    // Attributes
    int64_t s;
    mbedtls_entropy_context ec;
    mbedtls_ctr_drbg_context ctr_drbg;
    mbedtls_ssl_config conf;
    mbedtls_ssl_context ssl;

    SSSLState();
    ~SSSLState();
};

// SSL helpers
extern SSSLState* SSSLOpen(int64_t s, SX509* x509);
extern int64_t SSSLSend(SSSLState* ssl, const char* buffer, int64_t length);
extern int64_t SSSLSend(SSSLState* ssl, const SFastBuffer& buffer);
extern bool SSSLSendConsume(SSSLState* ssl, SFastBuffer& sendBuffer);
extern bool SSSLSendAll(SSSLState* ssl, const string& buffer);
extern int64_t SSSLRecv(SSSLState* ssl, char* buffer, int64_t length);
extern bool SSSLRecvAppend(SSSLState* ssl, SFastBuffer& recvBuffer);
extern string SSSLGetState(SSSLState* ssl);
extern void SSSLShutdown(SSSLState* ssl);
extern void SSSLClose(SSSLState* ssl);
