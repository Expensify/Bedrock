/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    TestHTTPS.h
 * Path:    test/lib/TestHTTPS.h
 * Pair:    TestHTTPS.cpp
 *
 * INTENT
 *   A minimal SHTTPSManager subclass used by tests to make outbound HTTPS
 *   requests and read back their status code, without any product-specific
 *   response handling.
 *
 * OBJECTS
 *   TestHTTPS - trivial SHTTPSManager subclass; exposes sendRequest publicly and parses the
 *               response's status code out of the raw HTTP method line in _onRecv.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; test-only HTTPS helper alongside the rest of test/lib.
 *
 * NAMING QUALITY
 *   Consistent with SHTTPSManager's naming conventions (the overridden `_onRecv`).
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>

class TestHTTPS : public SHTTPSManager {
public:
    TestHTTPS(BedrockPlugin& plugin_) : SHTTPSManager(plugin_)
    {
    }

    virtual ~TestHTTPS();

    // SHTTPSManager API
    virtual bool _onRecv(Transaction& transaction) override;
    virtual unique_ptr<Transaction> sendRequest(const string& url, SData& request);
};
