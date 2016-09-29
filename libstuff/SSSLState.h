#pragma once
#include <mbedtls/ssl.h>
#include <mbedtls/entropy.h>

struct SSSLState {
  // Attributes
  int s;
  mbedtls_entropy_context ec;
  mbedtls_ssl_config conf;
  mbedtls_ssl_context ssl;

  SSSLState();
  ~SSSLState();
};

// SSL helpers
extern SSSLState *SSSLOpen(int s, SX509 *x509);
extern int SSSLSend(SSSLState *ssl, const char *buffer, int length);
extern int SSSLSend(SSSLState *ssl, const string &buffer);
extern bool SSSLSendConsume(SSSLState *ssl, string &sendBuffer);
extern bool SSSLSendAll(SSSLState *ssl, const string &buffer);
extern int SSSLRecv(SSSLState *ssl, char *buffer, int length);
extern bool SSSLRecvAppend(SSSLState *ssl, string &recvBuffer);
extern string SSSLGetState(SSSLState *ssl);
extern void SSSLShutdown(SSSLState *ssl);
extern void SSSLClose(SSSLState *ssl);

// Pre-computed DH-1024 prime
// **FIXME: This is XySSL default -- change
extern const char *g_S_dhm_P;
extern const char *g_S_dhm_G;
