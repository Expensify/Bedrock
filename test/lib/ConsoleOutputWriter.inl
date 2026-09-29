#include <algorithm>
#include <iostream>
#include <sstream>

namespace tpunit {

inline ConsoleOutputWriter::ConsoleOutputWriter(bool verbose) : ConsoleOutputWriter(std::cout, verbose)
{
}

inline ConsoleOutputWriter::ConsoleOutputWriter(std::ostream& output, bool verbose)
    : output(output), verbose(verbose)
{
}

inline void ConsoleOutputWriter::breakCheckLine()
{
    if (!verbose && shortOutputColumn > 0) {
        output << '\n';
        shortOutputColumn = 0;
    }
}

inline void ConsoleOutputWriter::runStarted(const std::vector<PlannedFixture>&)
{
}

inline void ConsoleOutputWriter::fixtureStarted(size_t, const std::string&, bool singleThreaded)
{
    if (singleThreaded) {
        output << "--------------\n";
    }
}

inline void ConsoleOutputWriter::fixtureFinished(size_t, const std::string&, std::chrono::milliseconds)
{
}

inline void ConsoleOutputWriter::fixtureSetupFailed(size_t, const std::string& fixture)
{
    output << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C initializing " << fixture << ". Skipping tests." << std::endl;
}

inline void ConsoleOutputWriter::fixtureTeardownFailed(size_t, const std::string& fixture)
{
    output << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C cleaning up " << fixture << "." << std::endl;
}

inline void ConsoleOutputWriter::testStarted(size_t, const std::string&, const std::string&)
{
}

inline void ConsoleOutputWriter::testFinished(size_t, const std::string&, const std::string& test, bool passed,
                                       std::chrono::milliseconds duration, const std::string& bufferedInfo)
{
    std::ostringstream time;
    time << '(' << duration;
    if (duration > std::chrono::milliseconds(5000)) {
        time << " \xF0\x9F\x90\x8C";
    }
    time << ')';
    if (passed) {
        if (verbose) {
            output << "\xE2\x9C\x85 " << test << ' ' << time.str() << '\n';
        } else {
            if (shortOutputColumn >= 80) {
                breakCheckLine();
            }
            output << "\033[32m\xE2\x9C\x93\033[0m" << std::flush;
            ++shortOutputColumn;
        }
    } else {
        if (!verbose) {
            breakCheckLine();
        }
        output << bufferedInfo << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C " << test << ' ' << time.str()
        << (verbose ? "\n" : "\n\n");
    }
}

inline void ConsoleOutputWriter::assertionFailed(const std::string&, const std::string&, int number,
                                          const std::string& file, int line, const std::string& bufferedInfo)
{
    breakCheckLine();
    output << "   assertion #" << number << " at " << file << ':' << line << '\n' << bufferedInfo;
}

inline void ConsoleOutputWriter::exceptionCaught(const std::string&, const std::string&, int number,
                                          const std::string& method, const std::string& cause,
                                          const std::string& bufferedInfo)
{
    breakCheckLine();
    output << "   exception #" << number << " from " << method << " with cause: " << cause << '\n' << bufferedInfo;
}

inline void ConsoleOutputWriter::trace(const std::string&, const std::string&, int number,
                                const std::string& file, int line, const std::string& message,
                                const std::string& bufferedInfo)
{
    breakCheckLine();
    output << "   trace #" << number << " at " << file << ':' << line << ": " << message << '\n' << bufferedInfo;
}

inline void ConsoleOutputWriter::comparisonFailed(const std::string&, const std::string&,
                                           const std::string& lhs, const std::string& rhs, bool isEqual)
{
    breakCheckLine();
    output << lhs << (isEqual ? " == " : " != ") << rhs << '\n';
}

inline void ConsoleOutputWriter::diagnostic(const std::string&, const std::string&, const std::string& message)
{
    output << message << std::endl;
}

inline void ConsoleOutputWriter::invalidPattern(const std::string& pattern)
{
    output << "Invalid pattern: " << pattern << ", skipping." << std::endl;
}

inline void ConsoleOutputWriter::unnamedFixture()
{
    output << "test has no name???" << std::endl;
}

inline void ConsoleOutputWriter::unmatchedPattern(const std::string& pattern)
{
    output << "\xE2\x9D\x8C Could not find any test matching, make sure the test name is right: " << pattern << '\n';
}

inline void ConsoleOutputWriter::threadShutdown(int threadID)
{
    output << "Thread " << threadID << " caught shutdown exception, exiting.\n";
}

inline void ConsoleOutputWriter::runFinished(const RunResult& result)
{
    output << "\n[ TEST RESULTS ] Passed: " << result.passes << ", Failed: " << result.failures << '\n';
    if (!result.failureNames.empty()) {
        output << "\nFailures:\n";
        for (const auto& failure : result.failureNames) {
            output << failure << '\n';
        }
    }
    output << "\nSlowest Test Classes: " << std::endl;
    long long totalTestTime = 0;
    for (const auto& testTime : result.testTimes) {
        totalTestTime += testTime.first.count();
    }
    auto testTimes = result.testTimes;
    std::stable_sort(testTimes.begin(), testTimes.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    for (size_t i = 0; i < std::min(size_t(10), testTimes.size()); ++i) {
        const auto& testTime = testTimes[testTimes.size() - i - 1];
        output << testTime.first << ": " << testTime.second << " : "
        << (static_cast<double>(testTime.first.count()) / totalTestTime) * 100.0
        << "% of total test time" << std::endl;
    }
    output << "Total test time across threads: " << totalTestTime << "ms" << std::endl;
}
}
