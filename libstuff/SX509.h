#pragma once
#include <mbedtls/pk.h>
#include <mbedtls/x509_crt.h>

// Wraps a X509 Cert
struct SX509 {
    // Attributes
    mbedtls_x509_crt cert;
    mbedtls_pk_context pk;
};

// X509 Certificates
extern SX509* SX509Open();

extern SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt);

extern SX509* SX509Open(const string& casCrt);

extern SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt, bool server, const string& casCrt);

extern void SX509Close(SX509* x509);
