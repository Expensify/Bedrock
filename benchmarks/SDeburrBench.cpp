#include <libstuff/SDeburr.h>
#include <libstuff/libstuff.h>
#include <test/lib/tpunit++.hpp>

#include <string>
#include <vector>
#include <iostream>

using namespace std;

struct SDeburrBench : tpunit::TestFixture {
    SDeburrBench() : tpunit::TestFixture(
        "SDeburr",
        TEST(SDeburrBench::benchShortASCII),
        TEST(SDeburrBench::benchLatin1),
        TEST(SDeburrBench::benchMixedLong),
        TEST(SDeburrBench::benchShortASCIIHigh),
        TEST(SDeburrBench::benchLatin1High),
        TEST(SDeburrBench::benchMixedLongHigh)
    ) {}

    static uint64_t runBench(const string& name, const vector<string>& inputs, int iterationsPerInput)
    {
        volatile size_t guard = 0;
        for (int i = 0; i < 100; ++i) {
            for (const auto& s : inputs) {
                guard += SDeburr::deburr(s).size();
            }
        }

        uint64_t start = STimeNow();
        size_t totalBytes = 0;
        for (int it = 0; it < iterationsPerInput; ++it) {
            for (const auto& s : inputs) {
                const string out = SDeburr::deburr(s);
                guard += out.size();
                totalBytes += s.size();
            }
        }
        uint64_t elapsedUs = STimeNow() - start;

        double seconds = static_cast<double>(elapsedUs) / 1'000'000.0;
        double mbProcessed = static_cast<double>(totalBytes) / (1024.0 * 1024.0);
        double mbps = seconds > 0.0 ? (mbProcessed / seconds) : 0.0;

        cout << "[SDeburrBench] " << name
             << ": inputs=" << inputs.size()
             << ", iters=" << iterationsPerInput
             << ", bytes=" << totalBytes
             << ", time_us=" << elapsedUs
             << ", throughput_MBps=" << mbps
             << endl;

        if (guard == 0) {
            cout << "[SDeburrBench] guard==0" << endl;
        }
        return elapsedUs;
    }

    void benchShortASCII()
    {
        const vector<string> inputs = {
            "Fabio", "WARIO", "#Pizza", "hello world", "expensify", "chat room #admins"
        };
        auto us = runBench("ShortASCII", inputs, 20000);
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLatin1()
    {
        const vector<string> inputs = {
            "Fábio", "Wário", "Crème Brûlée", "São Paulo", "smörgåsbord",
            "façade", "Æsir", "Œuvre", "Ångström"
        };
        auto us = runBench("Latin1", inputs, 10000);
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchMixedLong()
    {
        const vector<string> inputs = {
            "Crème Brûlée déjà vu – São Paulo smörgåsbord, 東京, pizza 🍕, résumé, coöperate, naïve, voilà."
        };
        auto us = runBench("MixedLong", inputs, 5000);
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchShortASCIIHigh()
    {
        const vector<string> inputs = {
            "Fabio", "WARIO", "#Pizza", "hello world", "expensify", "chat room #admins"
        };
        auto us = runBench("ShortASCIIHigh", inputs, 500000);
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLatin1High()
    {
        const vector<string> inputs = {
            "Fábio", "Wário", "Crème Brûlée", "São Paulo", "smörgåsbord",
            "façade", "Æsir", "Œuvre", "Ångström"
        };
        auto us = runBench("Latin1High", inputs, 500000);
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchMixedLongHigh()
    {
        const vector<string> inputs = {
            "Crème Brûlée déjà vu – São Paulo smörgåsbord, 東京, pizza 🍕, résumé, coöperate, naïve, voilà."
        };
        auto us = runBench("MixedLongHigh", inputs, 500000);
        ASSERT_GREATER_THAN(us, 0);
    }
} __SDeburrBench;


