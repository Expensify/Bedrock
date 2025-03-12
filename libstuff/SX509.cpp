#include "SX509.h"

#include <cstring>
#include <mbedtls/certs.h>

#include <libstuff/libstuff.h>

// --------------------------------------------------------------------------
SX509* SX509Open() {
    // Initialize with defaults
    return SX509Open("", "", "");
}

// --------------------------------------------------------------------------
SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt) {
    // Use either the supplied credentials, or defaults for testing
    const char* pemPtr = (pem.empty() ? mbedtls_test_srv_key : pem.c_str());
    const char* srvCrtPtr = (srvCrt.empty() ? mbedtls_test_srv_crt : srvCrt.c_str());
    const char* caCrtPtr = (caCrt.empty() ? mbedtls_test_ca_crt : caCrt.c_str());

    // Just create a fake certificate from the PolarSSL defaults
    SX509* x509 = new SX509;
    mbedtls_x509_crt_init(&(x509->srvcert));
    mbedtls_pk_init(&(x509->pk));
    try {
        // Load and initialize this key
        if (mbedtls_pk_parse_key(&x509->pk, (unsigned char*)pemPtr, (int)strlen(pemPtr) + 1, NULL, 0)) {
            STHROW("parsing key");
        }
        if (mbedtls_x509_crt_parse(&x509->srvcert, (unsigned char*)srvCrtPtr, (int)strlen(srvCrtPtr) + 1)) {
            STHROW("parsing server certificate");
        }
        if (mbedtls_x509_crt_parse(&x509->srvcert, (unsigned char*)caCrtPtr, (int)strlen(caCrtPtr) + 1)) {
            STHROW("parsing CA certificate");
        }
        return x509;
    } catch (const SException& e) {
        // Failed
        SWARN("X509 creation failed while '" << e.what() << "', cancelling.");
        SX509Close(x509);
        return 0;
    }
}

// --------------------------------------------------------------------------
void SX509Close(SX509* x509) {
    // Clean up
    mbedtls_x509_crt_free(&x509->srvcert);
    mbedtls_pk_free(&x509->pk);
    delete x509;
}
