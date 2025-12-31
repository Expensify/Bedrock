#include <libstuff/SDeburr.h>
#include "BenchmarkBase.h"

#include <string>
#include <vector>

using namespace std;

struct SDeburrBench : tpunit::TestFixture, BenchmarkBase
{
    SDeburrBench() : tpunit::TestFixture(
        "SDeburr",
        TEST(SDeburrBench::benchShortASCII),
        TEST(SDeburrBench::benchLatin1),
        TEST(SDeburrBench::benchMixedLong),
        TEST(SDeburrBench::benchShortASCIIHigh),
        TEST(SDeburrBench::benchLatin1High),
        TEST(SDeburrBench::benchMixedLongHigh)
        ), BenchmarkBase("SDeburr")
    {
    }

    void benchShortASCII()
    {
        const vector<string> inputs = {
            "Fabio", "WARIO", "#Pizza", "hello world", "expensify", "chat room #admins"
        };
        auto us = runBench("ShortASCII", inputs, 20000, [](const string& s) {
            return SDeburr::deburr(s);
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLatin1()
    {
        const vector<string> inputs = {
            "Fábio", "Wário", "Crème Brûlée", "São Paulo", "smörgåsbord",
            "façade", "Æsir", "Œuvre", "Ångström"
        };
        auto us = runBench("Latin1", inputs, 10000, [](const string& s) {
            return SDeburr::deburr(s);
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchMixedLong()
    {
        const vector<string> inputs = {
            "Crème Brûlée déjà vu – São Paulo smörgåsbord, 東京, pizza 🍕, résumé, coöperate, naïve, voilà."
        };
        auto us = runBench("MixedLong", inputs, 5000, [](const string& s) {
            return SDeburr::deburr(s);
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchShortASCIIHigh()
    {
        const vector<string> inputs = {
            "Fabio", "WARIO", "#Pizza", "hello world", "expensify", "chat room #admins"
        };
        auto us = runBench("ShortASCIIHigh", inputs, 500000, [](const string& s) {
            return SDeburr::deburr(s);
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLatin1High()
    {
        const vector<string> inputs = {
            "Fábio", "Wário", "Crème Brûlée", "São Paulo", "smörgåsbord",
            "façade", "Æsir", "Œuvre", "Ångström"
        };
        auto us = runBench("Latin1High", inputs, 500000, [](const string& s) {
            return SDeburr::deburr(s);
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchMixedLongHigh()
    {
        const vector<string> inputs = {
            "Crème Brûlée déjà vu – São Paulo smörgåsbord, 東京, pizza 🍕, résumé, coöperate, naïve, voilà."
        };
        auto us = runBench("MixedLongHigh", inputs, 500000, [](const string& s) {
            return SDeburr::deburr(s);
        });
        ASSERT_GREATER_THAN(us, 0);
    }
} __SDeburrBench;
