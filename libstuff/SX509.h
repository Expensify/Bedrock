#pragma once

#include <mbedtls/pk.h>
#include <mbedtls/x509_crt.h>
#include <string>

using namespace std;

// A client TLS identity: our certificate and key, plus the CA the server must chain to.
struct SX509 {
    mbedtls_x509_crt cert;
    mbedtls_pk_context pk;
    mbedtls_x509_crt ca;
    bool hasCA = false;
};

extern SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt);
extern void SX509Close(SX509* x509);
