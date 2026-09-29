#include <test/lib/NcursesOutputWriter.h>
#include <test/lib/ConsoleOutputWriter.h>
#include <ncurses.h>
#include <algorithm>
#include <sys/ioctl.h>
#include <unistd.h>
#include <atomic>
#include <cstdio>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>

using namespace tpunit;
using namespace std::chrono;

struct NcursesOutputWriter::Impl
{
    struct Fixture
    {
        std::string name;
        size_t total = 0;
        size_t completed = 0;
        bool running = false;
        steady_clock::time_point started;
    };
    std::mutex mutex;
    std::vector<Fixture> fixtures;
    std::vector<std::string> failures;
    std::vector<std::string> failureDetails;
    std::vector<std::string> messages;
    std::map<std::thread::id, std::string> pending;
    std::set<std::string> failureNames;
    std::vector<std::pair<milliseconds, std::string>> testTimes;
    steady_clock::time_point started = steady_clock::now();
    size_t completed = 0;
    int passed = 0;
    int failed = 0;
    int summaryPasses = 0;
    int summaryFailures = 0;
    bool planned = false;
    bool hasResult = false;
    std::atomic<bool> stopped{false};
    std::thread renderer;
    FILE* tty = nullptr;
    FILE* capture = nullptr;
    SCREEN* screen = nullptr;
    int oldStdout = -1;
    int oldStderr = -1;
    bool finished = false;

    static std::string clockText(seconds duration)
    {
        auto count = duration.count();
        char text[32];
        snprintf(text, sizeof(text), "%02lld:%02lld", static_cast<long long>(count / 60), static_cast<long long>(count % 60));
        return text;
    }

    static std::string safe(const std::string& text)
    {
        std::string result;
        for (unsigned char c : text) {
            if (c >= 32 && c != 127) {
                result += static_cast<char>(c);
            }
        }
        return result;
    }

    void draw()
    {
        std::vector<Fixture> current;
        std::vector<std::string> currentFailures;
        steady_clock::time_point runStart;
        size_t done;
        int passes, failedTests;
        bool hasPlan;
        {
            std::lock_guard lock(mutex);
            current = fixtures;
            currentFailures = failures;
            runStart = started;
            done = completed;
            passes = passed;
            failedTests = failed;
            hasPlan = planned;
        }
        if (!hasPlan) {
            return;
        }
        struct winsize size {};
        if (ioctl(fileno(tty), TIOCGWINSZ, &size) == 0 && size.ws_row && size.ws_col &&
            (size.ws_row != LINES || size.ws_col != COLS)) {
            resizeterm(size.ws_row, size.ws_col);
        }
        int height, width;
        getmaxyx(stdscr, height, width);
        if (height < 2 || width < 2) {
            return;
        }
        erase();
        auto now = steady_clock::now();
        auto line = [&](int row, const std::string& text) {
            if (row >= 0 && row < height) {
                mvaddnstr(row, 0, safe(text).c_str(), width - 1);
            }
        };
        std::ostringstream header;
        header << "Elapsed " << clockText(duration_cast<seconds>(now - runStart)) << "  " << done << '/' << current.size()
        << " completed (" << (current.empty() ? 100 : 100 * done / current.size()) << "%)"
        << "  Failed: " << failedTests << "  Passed: " << passes;
        line(0, header.str());
        int failureHeight = currentFailures.empty() ? 0 : std::min(height - 2, std::max(2, height / 3));
        std::vector<Fixture> active;
        for (const auto& fixture : current) {
            if (fixture.running) {
                active.push_back(fixture);
            }
        }
        std::sort(active.begin(), active.end(), [](const auto& a, const auto& b) {
            return a.started < b.started;
        });
        int availableRows = std::max(0, height - 2 - (failureHeight ? failureHeight + 1 : 0));
        bool overflow = static_cast<int>(active.size()) > availableRows && availableRows > 0;
        int shown = std::min(static_cast<int>(active.size()), availableRows - (overflow ? 1 : 0));
        for (int i = 0; i < shown; ++i) {
            const auto& fixture = active[i];
            std::ostringstream row;
            row << clockText(duration_cast<seconds>(now - fixture.started)) << "  " << fixture.completed
            << '/' << fixture.total << "  " << fixture.name;
            line(i + 2, row.str());
        }
        if (overflow) {
            line(2 + shown, "+" + std::to_string(active.size() - shown) + " more running");
        }
        if (failureHeight) {
            int failureStart = shown || overflow ? 3 + shown + (overflow ? 1 : 0) : 2;
            line(failureStart, "Failed tests (" + std::to_string(currentFailures.size()) + "):");
            int slots = height - failureStart - 1;
            size_t offset = currentFailures.size() > static_cast<size_t>(slots) ? currentFailures.size() - slots : 0;
            for (int i = 0; i < slots && offset + i < currentFailures.size(); ++i) {
                line(failureStart + 1 + i, currentFailures[offset + i]);
            }
        }
        refresh();
    }

    void append(const std::string& message)
    {
        std::lock_guard lock(mutex);
        pending[std::this_thread::get_id()] += message;
    }

    void failure(const std::string& name)
    {
        failures.push_back(name);
        ++failed;
        auto& details = pending[std::this_thread::get_id()];
        failureDetails.push_back(name + "\n" + details);
        details.clear();
    }
};

bool NcursesOutputWriter::available()
{
    return isatty(STDIN_FILENO) && isatty(STDOUT_FILENO);
}

NcursesOutputWriter::NcursesOutputWriter() : impl(std::make_unique<Impl>())
{
    impl->tty = fopen("/dev/tty", "w");
    if (!impl->tty) {
        throw std::runtime_error("Could not open terminal");
    }
    fcntl(fileno(impl->tty), F_SETFD, FD_CLOEXEC);
    impl->screen = newterm(nullptr, impl->tty, stdin);
    if (!impl->screen) {
        // A local terminal may advertise a newer TERM than the VM has in its terminfo database.
        // Modern terminals support the xterm-256color control sequences we use here.
        impl->screen = newterm("xterm-256color", impl->tty, stdin);
    }
    if (!impl->screen) {
        fclose(impl->tty);
        impl->tty = nullptr;
        throw std::runtime_error("Could not initialize ncurses");
    }
    cbreak();
    noecho();
    curs_set(0);
    impl->capture = tmpfile();
    impl->oldStdout = dup(STDOUT_FILENO);
    impl->oldStderr = dup(STDERR_FILENO);
    if (!impl->capture || impl->oldStdout < 0 || impl->oldStderr < 0) {
        endwin();
        delscreen(impl->screen);
        fclose(impl->tty);
        if (impl->capture) {
            fclose(impl->capture);
        }
        if (impl->oldStdout >= 0) {
            close(impl->oldStdout);
        }
        if (impl->oldStderr >= 0) {
            close(impl->oldStderr);
        }
        throw std::runtime_error("Could not capture test output");
    }
    fcntl(impl->oldStdout, F_SETFD, FD_CLOEXEC);
    fcntl(impl->oldStderr, F_SETFD, FD_CLOEXEC);
    fcntl(fileno(impl->capture), F_SETFD, FD_CLOEXEC);
    std::cout.flush();
    std::cerr.flush();
    fflush(nullptr);
    dup2(fileno(impl->capture), STDOUT_FILENO);
    dup2(fileno(impl->capture), STDERR_FILENO);
    impl->renderer = std::thread([this]() {
        set_term(impl->screen);
        while (!impl->stopped) {
            impl->draw();
            std::this_thread::sleep_for(100ms);
        }
        impl->draw();
        endwin();
    });
}

NcursesOutputWriter::~NcursesOutputWriter()
{
    finish();
}

void NcursesOutputWriter::finish()
{
    if (impl->finished) {
        return;
    }
    impl->finished = true;
    impl->stopped = true;
    impl->renderer.join();
    delscreen(impl->screen);
    fclose(impl->tty);
    std::cout.flush();
    std::cerr.flush();
    fflush(nullptr);
    dup2(impl->oldStdout, STDOUT_FILENO);
    dup2(impl->oldStderr, STDERR_FILENO);
    close(impl->oldStdout);
    close(impl->oldStderr);
    lseek(fileno(impl->capture), 0, SEEK_SET);
    char buffer[8192];
    ssize_t count;
    while ((count = read(fileno(impl->capture), buffer, sizeof(buffer))) > 0) {
        size_t written = 0;
        while (written < static_cast<size_t>(count)) {
            ssize_t amount = write(STDOUT_FILENO, buffer + written, count - written);
            if (amount <= 0) {
                break;
            }
            written += amount;
        }
    }
    fclose(impl->capture);
    for (const auto& detail : impl->failureDetails) {
        std::cout << detail << '\n';
    }
    for (const auto& message : impl->messages) {
        std::cout << message << '\n';
    }
    if (impl->hasResult) {
        ConsoleOutputWriter console;
        console.runFinished({impl->summaryPasses, impl->summaryFailures, impl->failureNames, impl->testTimes});
    }
}

void NcursesOutputWriter::runStarted(const std::vector<PlannedFixture>& plan)
{
    std::lock_guard lock(impl->mutex);
    impl->fixtures.clear();
    impl->planned = true;
    for (const auto& fixture : plan) {
        impl->fixtures.push_back({fixture.name, fixture.testCount});
    }
    impl->started = steady_clock::now();
    impl->completed = 0;
    impl->passed = impl->failed = 0;
    impl->failures.clear();
    impl->pending.clear();
}

void NcursesOutputWriter::fixtureStarted(size_t id, const std::string&, bool)
{
    std::lock_guard lock(impl->mutex);
    impl->fixtures.at(id).running = true;
    impl->fixtures.at(id).started = steady_clock::now();
}

void NcursesOutputWriter::fixtureFinished(size_t id, const std::string&, milliseconds)
{
    std::lock_guard lock(impl->mutex);
    impl->fixtures.at(id).running = false;
    ++impl->completed;
}

void NcursesOutputWriter::fixtureSetupFailed(size_t, const std::string& fixture)
{
    std::lock_guard lock(impl->mutex);
    impl->failure(fixture + "::BEFORE_CLASS");
}

void NcursesOutputWriter::fixtureTeardownFailed(size_t, const std::string& fixture)
{
    std::lock_guard lock(impl->mutex);
    impl->failure(fixture + "::AFTER_CLASS");
}

void NcursesOutputWriter::testStarted(size_t, const std::string&, const std::string&)
{
    std::lock_guard lock(impl->mutex);
    impl->pending[std::this_thread::get_id()].clear();
}

void NcursesOutputWriter::testFinished(size_t id, const std::string& fixture, const std::string& test, bool passed,
                                       milliseconds, const std::string& bufferedInfo)
{
    std::lock_guard lock(impl->mutex);
    ++impl->fixtures.at(id).completed;
    impl->pending[std::this_thread::get_id()] += bufferedInfo;
    if (passed) {
        ++impl->passed;
        if (!impl->pending[std::this_thread::get_id()].empty()) {
            impl->messages.push_back(impl->pending[std::this_thread::get_id()]);
        }
        impl->pending[std::this_thread::get_id()].clear();
    } else {
        impl->failure(fixture + "::" + test);
    }
}

void NcursesOutputWriter::assertionFailed(const std::string&, const std::string&, int number,
                                          const std::string& file, int line, const std::string& bufferedInfo)
{
    impl->append("  assertion #" + std::to_string(number) + " at " + file + ":" + std::to_string(line) + "\n" + bufferedInfo);
}

void NcursesOutputWriter::exceptionCaught(const std::string&, const std::string&, int number,
                                          const std::string& method, const std::string& cause,
                                          const std::string& bufferedInfo)
{
    impl->append("  exception #" + std::to_string(number) + " from " + method + " with cause: " + cause + "\n" + bufferedInfo);
}

void NcursesOutputWriter::trace(const std::string&, const std::string&, int, const std::string&,
                                int, const std::string& message, const std::string& bufferedInfo)
{
    impl->append("  trace: " + message + "\n" + bufferedInfo);
}

void NcursesOutputWriter::comparisonFailed(const std::string&, const std::string&,
                                           const std::string& lhs, const std::string& rhs, bool isEqual)
{
    impl->append(lhs + (isEqual ? " == " : " != ") + rhs + "\n");
}

void NcursesOutputWriter::diagnostic(const std::string&, const std::string&, const std::string& message)
{
    impl->append(message + "\n");
}

void NcursesOutputWriter::invalidPattern(const std::string& pattern)
{
    std::lock_guard lock(impl->mutex);
    impl->messages.push_back("Invalid pattern: " + pattern + ", skipping.");
}

void NcursesOutputWriter::unnamedFixture()
{
    std::lock_guard lock(impl->mutex);
    impl->messages.push_back("test has no name???");
}

void NcursesOutputWriter::unmatchedPattern(const std::string& pattern)
{
    std::lock_guard lock(impl->mutex);
    impl->messages.push_back("Could not find any test matching, make sure the test name is right: " + pattern);
}

void NcursesOutputWriter::threadShutdown(int threadID)
{
    std::lock_guard lock(impl->mutex);
    impl->messages.push_back("Thread " + std::to_string(threadID) + " caught shutdown exception, exiting.");
}

void NcursesOutputWriter::runFinished(const RunResult& result)
{
    std::lock_guard lock(impl->mutex);
    impl->hasResult = true;
    impl->summaryPasses = result.passes;
    impl->summaryFailures = result.failures;
    impl->failureNames = result.failureNames;
    impl->testTimes = result.testTimes;
}
