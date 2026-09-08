/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    FastHTTPParsing.cpp
 * Path:    test/tests/FastHTTPParsing.cpp
 *
 * INTENT
 *   Unit tests for SFastBuffer::startsWithHTTPRequest(), the streaming
 *   detector that tells a socket handler whether a full HTTP request
 *   (headers plus declared content-length body) has accumulated in a
 *   growing receive buffer yet.
 *
 * OBJECTS
 *   FastHTTPParsing  - tpunit::TestFixture; exercises simple success/fail
 *                       cases, both CRLF and LF line endings, buffers fed
 *                       in split chunks, and re-detection after an SData
 *                       deserialize/consumeFront cycle "resets" the buffer
 *                       for a second pipelined request.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits: it lives under test/tests alongside other command/protocol
 *   tests, though the name reads as testing "fast HTTP parsing" broadly
 *   when it in fact tests one specific predicate (startsWithHTTPRequest).
 *
 * NAMING QUALITY
 *   Consistent with sibling test fixtures (PascalCase struct name, global
 *   `__FastHTTPParsing` instance). Method names (simpleSuccess, blank,
 *   splitSeparators, reset) are clear and test-specific.
 * ─────────────────────────────────────────────────────────────────────*/

#include <libstuff/SFastBuffer.h>
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct FastHTTPParsing : tpunit::TestFixture
{
    FastHTTPParsing() : tpunit::TestFixture(true, "FastHTTPParsing",
                                            TEST(FastHTTPParsing::simpleSuccess),
                                            TEST(FastHTTPParsing::simpleFail),
                                            TEST(FastHTTPParsing::blank),
                                            TEST(FastHTTPParsing::noHeaders),
                                            TEST(FastHTTPParsing::splitSeparators),
                                            TEST(FastHTTPParsing::reset))
    {
    }

    // We test both supported line ends everywhere.
    list<string> lineEnds = {"\r\n", "\n"};

    void simpleSuccess()
    {
        for (const auto& end : lineEnds) {
            SFastBuffer fb("GET / HTTP/1.1" + end +
                           "Content-Length: 0" + end +
                           end);
            ASSERT_TRUE(fb.startsWithHTTPRequest());
        }
    }

    void simpleFail()
    {
        for (const auto& end : lineEnds) {
            SFastBuffer fb("GET / HTTP/1.1" + end +
                           "Content-Length: 0" + end);
            ASSERT_FALSE(fb.startsWithHTTPRequest());
        }
    }

    void blank()
    {
        SFastBuffer fb;
        ASSERT_FALSE(fb.startsWithHTTPRequest());
    }

    void noHeaders()
    {
        for (const auto& end : lineEnds) {
            SFastBuffer fb("GET / HTTP/1.1" + end +
                           end);
            ASSERT_TRUE(fb.startsWithHTTPRequest());
        }
    }

    void splitSeparators()
    {
        SFastBuffer fb;
        string start = "GET / HTTP/1.1\r\nContent-Length: 0";
        fb.append(start.c_str(), start.size());
        ASSERT_FALSE(fb.startsWithHTTPRequest());
        fb.append("\r", 1);
        ASSERT_FALSE(fb.startsWithHTTPRequest());
        fb.append("\n", 1);
        ASSERT_FALSE(fb.startsWithHTTPRequest());
        fb.append("\r", 1);
        ASSERT_FALSE(fb.startsWithHTTPRequest());
        fb.append("\n", 1);
        ASSERT_TRUE(fb.startsWithHTTPRequest());

        fb.clear();
        string start2 = "GET / HTTP/1.1\nContent-Length: 0";
        fb.append(start2.c_str(), start.size());
        fb.append("\n", 1);
        ASSERT_FALSE(fb.startsWithHTTPRequest());
        fb.append("\n", 1);
        ASSERT_TRUE(fb.startsWithHTTPRequest());
    }

    void reset()
    {
        for (const auto& end : lineEnds) {
            SFastBuffer fb("GET / HTTP/1.1" + end +
                           "Content-Length: 0" + end +
                           end + "GET");
            ASSERT_TRUE(fb.startsWithHTTPRequest());

            SData request;
            int requestSize = request.deserialize(fb);
            fb.consumeFront(requestSize);

            ASSERT_EQUAL(string(fb.c_str()), string("GET"));
            ASSERT_FALSE(fb.startsWithHTTPRequest());

            string restOf2ndRequest = " / HTTP/1.1" + end +
                "Content-Length: 1" + end +
                end +
                "A";
            fb.append(restOf2ndRequest.c_str(), restOf2ndRequest.size());
            ASSERT_TRUE(fb.startsWithHTTPRequest());
            requestSize = request.deserialize(fb);
            fb.consumeFront(requestSize);
            ASSERT_TRUE(fb.empty());

            ASSERT_EQUAL(request.content, "A");
            ASSERT_EQUAL(request["Content-length"], "1");
        }
    }
} __FastHTTPParsing;
