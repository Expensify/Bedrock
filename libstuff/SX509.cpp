#include "libstuff.h"
#include <mbedtls/certs.h>

// --------------------------------------------------------------------------
SX509* SX509Open() {
    // Initialize with defaults
    return SX509Open("", "", "", true, "");
}

SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt) {
    // initialize a default server state
    return SX509Open(pem, srvCrt, caCrt, true, "");
}

// open an SSL client cert chain using CAS/PEM
SX509* SX509Open(const string& casCrt) {
    // initialize a default client state
    return SX509Open(nullptr, nullptr, nullptr, false, casCrt);
}

// --------------------------------------------------------------------------
SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt, bool server, const string& casCrt) {
    const char* pemPtr;
    const char* srvCrtPtr;
    const char* caCrtPtr;
    const char* casCrtPtr;

    SX509* x509 = new SX509;

    // The server bool chooses between client and server defaults.
    if(server) {
        // Use either the supplied credentials, or defaults for testing
        pemPtr = (pem.empty() ? mbedtls_test_srv_key : pem.c_str());
        srvCrtPtr = (srvCrt.empty() ? mbedtls_test_srv_crt : srvCrt.c_str());
        caCrtPtr = (caCrt.empty() ? mbedtls_test_ca_crt : caCrt.c_str());

        mbedtls_pk_init(&(x509->pk));
    } else {
        casCrtPtr = (casCrt.empty() ? mbedtls_test_cas_pem : casCrt.c_str());
    }


    // Just create a fake certificate from the PolarSSL defaults
    
    mbedtls_x509_crt_init(&(x509->cert));
    
    try {
        if(server) {
            // Load and initialize this key
            if (mbedtls_pk_parse_key(&x509->pk, (unsigned char*)pemPtr, (int)strlen(pemPtr) + 1, NULL, 0)) {
                STHROW("MB parsing key");
            }
            if (mbedtls_x509_crt_parse(&x509->cert, (unsigned char*)srvCrtPtr, (int)strlen(srvCrtPtr) + 1)) {
                STHROW("MB parsing server certificate");
            }
            if (mbedtls_x509_crt_parse(&x509->cert, (unsigned char*)caCrtPtr, (int)strlen(caCrtPtr) + 1)) {
                STHROW("MB parsing CA certificate");
            }
        } else {
            if (mbedtls_x509_crt_parse(&(x509->cert), (unsigned char*)casCrtPtr, (int)strlen(casCrtPtr) + 1) ) {
                STHROW("MB Client Parse CAS Failed");
            }
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
    mbedtls_x509_crt_free(&x509->cert);
    mbedtls_pk_free(&x509->pk);
    delete x509;
}
