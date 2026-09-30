#include <test/output/NcursesOutputWriter.h>
#include <test/lib/ConsoleOutputWriter.h>
#include <ncurses.h>
#include <algorithm>
#include <sys/ioctl.h>
#include <unistd.h>
#include <atomic>
#include <cstdio>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>

using namespace tpunit;
using namespace std;
using namespace chrono;

struct NcursesOutputWriter::Impl
{
    struct Fixture
    {
        string name;
        size_t total = 0;
        size_t completed = 0;
        bool running = false;
        steady_clock::time_point started;
    };
    mutex mutex;
    vector<Fixture> fixtures;
    vector<string> failures;
    vector<string> failureDetails;
    vector<string> messages;
    map<thread::id, string> pending;
    set<string> failureNames;
    vector<pair<milliseconds, string>> testTimes;
    steady_clock::time_point started = steady_clock::now();
    size_t completed = 0;
    int passed = 0;
    int failed = 0;
    int summaryPasses = 0;
    int summaryFailures = 0;
    bool planned = false;
    bool hasResult = false;
    atomic<bool> stopped{false};
    thread renderer;
    FILE* tty = nullptr;
    FILE* capture = nullptr;
    SCREEN* screen = nullptr;
    int oldStdout = -1;
    int oldStderr = -1;
    bool finished = false;

    static string clockText(seconds duration)
    {
        auto count = duration.count();
        char text[32];
        snprintf(text, sizeof(text), "%02lld:%02lld", static_cast<long long>(count / 60), static_cast<long long>(count % 60));
        return text;
    }

    static string safe(const string& text)
    {
        string result;
        for (unsigned char c : text) {
            if (c >= 32 && c != 127) {
                result += static_cast<char>(c);
            }
        }
        return result;
    }

    void draw()
    {
        vector<Fixture> current;
        vector<string> currentFailures;
        steady_clock::time_point runStart;
        size_t done;
        int passes, failedTests;
        bool hasPlan;
        {
            lock_guard lock(mutex);
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
        auto line = [&](int row, const string& text) {
            if (row >= 0 && row < height) {
                mvaddnstr(row, 0, safe(text).c_str(), width - 1);
            }
        };
        size_t completedCases = 0;
        size_t totalCases = 0;
        for (const auto& fixture : current) {
            completedCases += fixture.completed;
            totalCases += fixture.total;
        }
        bool compactHeader = width < 70;
        int elapsedWidth = compactHeader ? 8 : 11;
        int classesWidth = compactHeader ? 9 : 13;
        int casesWidth = compactHeader ? 9 : 14;
        int completeWidth = compactHeader ? 9 : 12;
        int failedWidth = compactHeader ? 7 : 9;
        ostringstream labels, values;
        labels << left << setw(elapsedWidth) << "ELAPSED" << setw(classesWidth) << "CLASSES"
        << setw(casesWidth) << (compactHeader ? "CASES" : "TEST CASES")
        << setw(completeWidth) << (compactHeader ? "COMPLETE" : "% COMPLETE")
        << setw(failedWidth) << "FAILED" << "PASSED";
        values << left << setw(elapsedWidth) << clockText(duration_cast<seconds>(now - runStart))
        << setw(classesWidth) << (to_string(done) + "/" + to_string(current.size()))
        << setw(casesWidth) << (to_string(completedCases) + "/" + to_string(totalCases))
        << setw(completeWidth) << (to_string(current.empty() ? 100 : 100 * done / current.size()) + "%")
        << setw(failedWidth) << failedTests << passes;
        attron(A_BOLD);
        line(0, labels.str());
        attroff(A_BOLD);
        line(1, values.str());
        if (height >= 7) {
            mvhline(2, 0, ACS_HLINE, width - 1);
        }
        int failureHeight = currentFailures.empty() ? 0 : min(height - 3, max(2, height / 3));
        vector<Fixture> active;
        for (const auto& fixture : current) {
            if (fixture.running) {
                active.push_back(fixture);
            }
        }
        sort(active.begin(), active.end(), [](const auto& a, const auto& b) {
            return a.started < b.started;
        });
        int availableRows = max(0, height - 3 - (failureHeight ? failureHeight + 1 : 0));
        bool overflow = static_cast<int>(active.size()) > availableRows && availableRows > 0;
        int shown = min(static_cast<int>(active.size()), availableRows - (overflow ? 1 : 0));
        for (int i = 0; i < shown; ++i) {
            const auto& fixture = active[i];
            ostringstream row;
            row << clockText(duration_cast<seconds>(now - fixture.started)) << "  "
            << right << setw(3) << fixture.completed << '/' << setw(3) << fixture.total
            << "  " << fixture.name;
            line(i + 3, row.str());
        }
        if (overflow) {
            line(3 + shown, "+" + to_string(active.size() - shown) + " more running");
        }
        if (failureHeight) {
            int failureStart = shown || overflow ? 4 + shown + (overflow ? 1 : 0) : (height >= 6 ? 4 : 3);
            line(failureStart, "Failed tests (" + to_string(currentFailures.size()) + "):");
            int slots = height - failureStart - 1;
            size_t offset = currentFailures.size() > static_cast<size_t>(slots) ? currentFailures.size() - slots : 0;
            for (int i = 0; i < slots && offset + i < currentFailures.size(); ++i) {
                line(failureStart + 1 + i, currentFailures[offset + i]);
            }
        }
        refresh();
    }

    void append(const string& message)
    {
        lock_guard lock(mutex);
        pending[this_thread::get_id()] += message;
    }

    void failure(const string& name)
    {
        failures.push_back(name);
        ++failed;
        auto& details = pending[this_thread::get_id()];
        failureDetails.push_back(name + "\n" + details);
        details.clear();
    }
};

bool NcursesOutputWriter::available()
{
    return isatty(STDIN_FILENO) && isatty(STDOUT_FILENO);
}

NcursesOutputWriter::NcursesOutputWriter() : impl(make_unique<Impl>())
{
    impl->tty = fopen("/dev/tty", "w");
    if (!impl->tty) {
        throw runtime_error("Could not open terminal");
    }
    fcntl(fileno(impl->tty), F_SETFD, FD_CLOEXEC);
    impl->screen = newterm(nullptr, impl->tty, stdin);
    if (!impl->screen) {
        // A local terminal may advertise a newer TERM than the VM has in its terminfo database.
        // Modern terminals support the xterm-256color control sequences we use here.
        static char fallbackTerm[] = "xterm-256color";
        impl->screen = newterm(fallbackTerm, impl->tty, stdin);
    }
    if (!impl->screen) {
        fclose(impl->tty);
        impl->tty = nullptr;
        throw runtime_error("Could not initialize ncurses");
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
        throw runtime_error("Could not capture test output");
    }
    fcntl(impl->oldStdout, F_SETFD, FD_CLOEXEC);
    fcntl(impl->oldStderr, F_SETFD, FD_CLOEXEC);
    fcntl(fileno(impl->capture), F_SETFD, FD_CLOEXEC);
    cout.flush();
    cerr.flush();
    fflush(nullptr);
    dup2(fileno(impl->capture), STDOUT_FILENO);
    dup2(fileno(impl->capture), STDERR_FILENO);
    impl->renderer = thread([this]() {
        set_term(impl->screen);
        while (!impl->stopped) {
            impl->draw();
            this_thread::sleep_for(100ms);
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
    cout.flush();
    cerr.flush();
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
        cout << detail << '\n';
    }
    for (const auto& message : impl->messages) {
        cout << message << '\n';
    }
    if (impl->hasResult) {
        ConsoleOutputWriter console;
        console.runFinished({impl->summaryPasses, impl->summaryFailures, impl->failureNames, impl->testTimes});
    }
}

void NcursesOutputWriter::runStarted(const vector<PlannedFixture>& plan)
{
    lock_guard lock(impl->mutex);
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

void NcursesOutputWriter::fixtureStarted(size_t id, const string&, bool)
{
    lock_guard lock(impl->mutex);
    impl->fixtures.at(id).running = true;
    impl->fixtures.at(id).started = steady_clock::now();
}

void NcursesOutputWriter::fixtureFinished(size_t id, const string&, milliseconds)
{
    lock_guard lock(impl->mutex);
    impl->fixtures.at(id).running = false;
    ++impl->completed;
}

void NcursesOutputWriter::fixtureSetupFailed(size_t, const string& fixture)
{
    lock_guard lock(impl->mutex);
    impl->failure(fixture + "::BEFORE_CLASS");
}

void NcursesOutputWriter::fixtureTeardownFailed(size_t, const string& fixture)
{
    lock_guard lock(impl->mutex);
    impl->failure(fixture + "::AFTER_CLASS");
}

void NcursesOutputWriter::testStarted(size_t, const string&, const string&)
{
    lock_guard lock(impl->mutex);
    impl->pending[this_thread::get_id()].clear();
}

void NcursesOutputWriter::testFinished(size_t id, const string& fixture, const string& test, bool passed,
                                       milliseconds, const string& bufferedInfo)
{
    lock_guard lock(impl->mutex);
    ++impl->fixtures.at(id).completed;
    impl->pending[this_thread::get_id()] += bufferedInfo;
    if (passed) {
        ++impl->passed;
        if (!impl->pending[this_thread::get_id()].empty()) {
            impl->messages.push_back(impl->pending[this_thread::get_id()]);
        }
        impl->pending[this_thread::get_id()].clear();
    } else {
        impl->failure(fixture + "::" + test);
    }
}

void NcursesOutputWriter::assertionFailed(const string&, const string&, int number,
                                          const string& file, int line, const string& bufferedInfo)
{
    impl->append("  assertion #" + to_string(number) + " at " + file + ":" + to_string(line) + "\n" + bufferedInfo);
}

void NcursesOutputWriter::exceptionCaught(const string&, const string&, int number,
                                          const string& method, const string& cause,
                                          const string& bufferedInfo)
{
    impl->append("  exception #" + to_string(number) + " from " + method + " with cause: " + cause + "\n" + bufferedInfo);
}

void NcursesOutputWriter::trace(const string&, const string&, int, const string&,
                                int, const string& message, const string& bufferedInfo)
{
    impl->append("  trace: " + message + "\n" + bufferedInfo);
}

void NcursesOutputWriter::comparisonFailed(const string&, const string&,
                                           const string& lhs, const string& rhs, bool isEqual)
{
    impl->append(lhs + (isEqual ? " == " : " != ") + rhs + "\n");
}

void NcursesOutputWriter::diagnostic(const string&, const string&, const string& message)
{
    impl->append(message + "\n");
}

void NcursesOutputWriter::invalidPattern(const string& pattern)
{
    lock_guard lock(impl->mutex);
    impl->messages.push_back("Invalid pattern: " + pattern + ", skipping.");
}

void NcursesOutputWriter::unnamedFixture()
{
    lock_guard lock(impl->mutex);
    impl->messages.push_back("test has no name???");
}

void NcursesOutputWriter::unmatchedPattern(const string& pattern)
{
    lock_guard lock(impl->mutex);
    impl->messages.push_back("Could not find any test matching, make sure the test name is right: " + pattern);
}

void NcursesOutputWriter::threadShutdown(int threadID)
{
    lock_guard lock(impl->mutex);
    impl->messages.push_back("Thread " + to_string(threadID) + " caught shutdown exception, exiting.");
}

void NcursesOutputWriter::runFinished(const RunResult& result)
{
    lock_guard lock(impl->mutex);
    impl->hasResult = true;
    impl->summaryPasses = result.passes;
    impl->summaryFailures = result.failures;
    impl->failureNames = result.failureNames;
    impl->testTimes = result.testTimes;
}
