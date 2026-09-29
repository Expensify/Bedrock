#include <test/lib/ConsoleOutputWriter.h>
#include <test/lib/tpunit++.hpp>
#include <cstdlib>
#include <iostream>
#include <sstream>

struct ConsoleOutputWriterTest : tpunit::TestFixture
{
    ConsoleOutputWriterTest()
        : tpunit::TestFixture("ConsoleOutputWriter",
                              TEST(ConsoleOutputWriterTest::testCompactOutput),
                              TEST(ConsoleOutputWriterTest::testVerboseOutput))
    {
    }

    void testCompactOutput()
    {
        if (std::getenv("TPUNIT_TEST_CAPTURE_OUTPUT")) {
            std::cout << "capture test stdout" << std::endl;
            std::cerr << "capture test stderr" << std::endl;
            printf("capture test printf\n");
            std::system("printf 'capture test child\\n'");
        }
        std::ostringstream stream;
        tpunit::ConsoleOutputWriter writer(stream);
        writer.fixtureStarted(0, "Example", true);
        writer.testFinished(0, "Example", "passing", true, 3ms, "");
        writer.assertionFailed("Example", "failing", 1, "example.cpp", 42, "    explanation\n");
        writer.testFinished(0, "Example", "failing", false, 7ms, "");

        EXPECT_EQUAL("--------------\n\033[32m\xE2\x9C\x93\033[0m\n"
                     "   assertion #1 at example.cpp:42\n    explanation\n"
                     "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C failing (7ms)\n\n", stream.str());
    }

    void testVerboseOutput()
    {
        std::ostringstream stream;
        tpunit::ConsoleOutputWriter writer(stream, true);
        writer.testFinished(0, "Example", "passing", true, 5001ms, "");
        writer.comparisonFailed("Example", "failing", "one", "two", false);
        writer.comparisonFailed("Example", "failing", "one", "one", true);
        writer.exceptionCaught("Example", "failing", 1, "failing", "problem", "    context\n");
        writer.testFinished(0, "Example", "failing", false, 1ms, "");

        EXPECT_EQUAL("\xE2\x9C\x85 passing (5001ms \xF0\x9F\x90\x8C)\n"
                     "one != two\n"
                     "one == one\n"
                     "   exception #1 from failing with cause: problem\n    context\n"
                     "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C failing (1ms)\n", stream.str());
    }
} __ConsoleOutputWriterTest;
