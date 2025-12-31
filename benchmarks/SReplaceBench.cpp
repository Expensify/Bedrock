#include <libstuff/libstuff.h>
#include "BenchmarkBase.h"

#include <string>
#include <vector>

using namespace std;

struct SReplaceBench : tpunit::TestFixture, BenchmarkBase
{
    SReplaceBench() : tpunit::TestFixture(
        "SReplace",
        TEST(SReplaceBench::benchNoMatches),
        TEST(SReplaceBench::benchFewMatches),
        TEST(SReplaceBench::benchManyMatches),
        TEST(SReplaceBench::benchLongFind),
        TEST(SReplaceBench::benchLongReplace),
        TEST(SReplaceBench::benchShortToLong),
        TEST(SReplaceBench::benchLongToShort)
        ), BenchmarkBase("SReplace")
    {
    }

    void benchNoMatches()
    {
        const vector<string> inputs = {
            "This is a test string with no matches at all",
            "Another string without the pattern we're looking for",
            "The quick brown fox jumps over the lazy dog"
        };
        auto us = runBench("NoMatches", inputs, 50000, [](const string& s) {
            return SReplace(s, "xyz", "abc");
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchFewMatches()
    {
        const vector<string> inputs = {
            "This is a test string with one match",
            "Another test with two test matches",
            "Three test matches test in test this string"
        };
        auto us = runBench("FewMatches", inputs, 50000, [](const string& s) {
            return SReplace(s, "test", "TEXT");
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchManyMatches()
    {
        const vector<string> inputs = {
            "x x x x x x x x x x x x x x x x x x x x",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bababababababababababababababababababa"
        };
        auto us = runBench("ManyMatches", inputs, 30000, [](const string& s) {
            return SReplace(s, "x", "y");
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLongFind()
    {
        const vector<string> inputs = {
            "This contains a verylongpatterntofindinthestring somewhere in the middle",
            "Another verylongpatterntofindinthestring here and verylongpatterntofindinthestring there"
        };
        auto us = runBench("LongFind", inputs, 50000, [](const string& s) {
            return SReplace(s, "verylongpatterntofindinthestring", "SHORT");
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLongReplace()
    {
        const vector<string> inputs = {
            "This x has x several x short x matches",
            "More x and x more x short x patterns x here"
        };
        auto us = runBench("LongReplace", inputs, 50000, [](const string& s) {
            return SReplace(s, "x", "verylongreplacementstring");
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchShortToLong()
    {
        const vector<string> inputs = {
            "a a a a a a a a a a a a a a a a a a a a",
            "Replace single chars with longer strings here"
        };
        auto us = runBench("ShortToLong", inputs, 30000, [](const string& s) {
            return SReplace(s, "a", "REPLACEMENT");
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLongToShort()
    {
        const vector<string> inputs = {
            "LONGWORD LONGWORD LONGWORD LONGWORD LONGWORD",
            "Multiple LONGWORD occurrences LONGWORD of LONGWORD this LONGWORD pattern"
        };
        auto us = runBench("LongToShort", inputs, 30000, [](const string& s) {
            return SReplace(s, "LONGWORD", "x");
        });
        ASSERT_GREATER_THAN(us, 0);
    }
} __SReplaceBench;
