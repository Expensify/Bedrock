#include <libstuff/libstuff.h>
#include "BenchmarkBase.h"

#include <string>
#include <vector>

using namespace std;

struct SReplaceAllBench : tpunit::TestFixture, BenchmarkBase
{
    SReplaceAllBench() : tpunit::TestFixture(
        "SReplaceAll",
        TEST(SReplaceAllBench::benchNoUnsafeChars),
        TEST(SReplaceAllBench::benchFewUnsafeChars),
        TEST(SReplaceAllBench::benchManyUnsafeChars),
        TEST(SReplaceAllBench::benchLongUnsafeList),
        TEST(SReplaceAllBench::benchURLSafe),
        TEST(SReplaceAllBench::benchAlphanumeric)
        ), BenchmarkBase("SReplaceAll")
    {
    }

    void benchNoUnsafeChars()
    {
        const vector<string> inputs = {
            "abcdefghijklmnopqrstuvwxyz",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "0123456789"
        };
        auto us = runBench("NoUnsafeChars", inputs, 100000, [](const string& s) {
            return SReplaceAll(s, "!@#$%", '_');
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchFewUnsafeChars()
    {
        const vector<string> inputs = {
            "hello world!",
            "test@example.com",
            "path/to/file.txt"
        };
        auto us = runBench("FewUnsafeChars", inputs, 100000, [](const string& s) {
            return SReplaceAll(s, " !@./", '_');
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchManyUnsafeChars()
    {
        const vector<string> inputs = {
            "!@#$%^&*()_+-=[]{}|;:',.<>?/",
            "lots!!!of!!!unsafe!!!chars!!!here!!!",
            "replace-all-these-special-chars-now!"
        };
        auto us = runBench("ManyUnsafeChars", inputs, 50000, [](const string& s) {
            return SReplaceAll(s, "!@#$%^&*()_+-=[]{}|;:',.<>?/", '_');
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchLongUnsafeList()
    {
        const vector<string> inputs = {
            "This is a normal sentence with some punctuation.",
            "Another test string with various characters!",
            "Testing the performance of character replacement"
        };
        auto us = runBench("LongUnsafeList", inputs, 50000, [](const string& s) {
            return SReplaceAll(s, "!@#$%^&*()_+-=[]{}|;:',.<>?/`~\"\\", '_');
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchURLSafe()
    {
        const vector<string> inputs = {
            "https://example.com/path?query=value&key=data",
            "user@domain.com/some/path/with/slashes",
            "file:///path/to/local/file.txt"
        };
        auto us = runBench("URLSafe", inputs, 50000, [](const string& s) {
            return SReplaceAll(s, ":/?#[]@!$&'()*+,;=", '_');
        });
        ASSERT_GREATER_THAN(us, 0);
    }

    void benchAlphanumeric()
    {
        const vector<string> inputs = {
            "Keep only letters and numbers 12345!",
            "Remove all special characters @#$%",
            "test_string-with-various.separators"
        };
        auto us = runBench("Alphanumeric", inputs, 50000, [](const string& s) {
            return SReplaceAll(s, " !@#$%^&*()_+-=[]{}|;:',.<>?/", '_');
        });
        ASSERT_GREATER_THAN(us, 0);
    }
} __SReplaceAllBench;
