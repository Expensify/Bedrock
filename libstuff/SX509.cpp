#include "SX509.h"

#include <libstuff/libstuff.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/error.h>

SX509* SX509Open(const string& pem, const string& srvCrt, const string& caCrt)
{
    SX509* x509 = new SX509;
    mbedtls_x509_crt_init(&x509->cert);
    mbedtls_pk_init(&x509->pk);
    mbedtls_x509_crt_init(&x509->ca);

    mbedtls_entropy_context entropy;
    mbedtls_ctr_drbg_context drbg;
    mbedtls_entropy_init(&entropy);
    mbedtls_ctr_drbg_init(&drbg);

    auto bytes = [](const string& text) { return reinterpret_cast<const unsigned char*>(text.c_str()); };
    try {
        int result = mbedtls_ctr_drbg_seed(&drbg, mbedtls_entropy_func, &entropy, nullptr, 0);
        if (result) {
            STHROW("seeding RNG");
        }
        // mbedtls requires the PEM length to include the null terminator
        if (mbedtls_pk_parse_key(&x509->pk, bytes(pem), pem.size() + 1, nullptr, 0, mbedtls_ctr_drbg_random, &drbg)) {
            STHROW("parsing key");
        }
        if (mbedtls_x509_crt_parse(&x509->cert, bytes(srvCrt), srvCrt.size() + 1)) {
            STHROW("parsing client certificate");
        }
        if (!caCrt.empty()) {
            if (mbedtls_x509_crt_parse(&x509->ca, bytes(caCrt), caCrt.size() + 1)) {
                STHROW("parsing CA certificate");
            }
            x509->hasCA = true;
        }
    } catch (const SException& e) {
        mbedtls_ctr_drbg_free(&drbg);
        mbedtls_entropy_free(&entropy);
        SX509Close(x509);
        STHROW("X509 creation failed while " + string(e.what()));
    }
    mbedtls_ctr_drbg_free(&drbg);
    mbedtls_entropy_free(&entropy);
    return x509;
}

void SX509Close(SX509* x509)
{
    mbedtls_x509_crt_free(&x509->ca);
    mbedtls_pk_free(&x509->pk);
    mbedtls_x509_crt_free(&x509->cert);
    delete x509;
}
