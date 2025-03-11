#pragma once
#include <mbedtls/pk.h>
#include <mbedtls/x509_crt.h>
#include <string>

using namespace std;

// Wraps a X509 Cert
struct SX509 {
    // Attributes
    mbedtls_x509_crt srvcert;
    mbedtls_pk_context pk;
};

// X509 Certificates
extern SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt);
extern void SX509Close(SX509* x509);
