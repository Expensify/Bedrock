#include "libstuff.h"
#include <sqlitecluster/SQLiteNode.h>
#include <execinfo.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

thread_local function<void()> SSignalHandlerDieFunc;
void SSetSignalHandlerDieFunc(function<void()>&& func) {
    SSignalHandlerDieFunc = move(func);
}

// 64kb emergency stack location.
constexpr auto sigStackSize{1024*64};
char __SIGSTACK[sigStackSize];

// The function to call in our thread that handles signals.
void _SSignal_signalHandlerThreadFunc();

// The function to call in threads handling their own signals. This is only used for exception signals like SEGV
// and FPE.
void _SSignal_StackTrace(int signum, siginfo_t *info, void *ucontext);

// A boolean indicating whether or not we've initialized our signal thread.
atomic_flag _SSignal_threadInitialized = ATOMIC_FLAG_INIT;

// Set to true to stop the signal thread.
atomic<bool> _SSignal_threadStopFlag(false);

// The signals we've received since the last time this was cleared.
atomic<uint64_t> _SSignal_pendingSignalBitMask(0);

// The thread that will wait for process-wide signals. It will be immediately detached once created and live until
// the process exits otherwise.
thread _SSignal_signalThread;

// Each thread gets an int it can store a signal number in. Since all signals caught by threads result in
// `abort()`, this records the original signal number until the signal handler for abort has a chance to log it.
thread_local int _SSignal_threadCaughtSignalNumber = 0;

bool SCheckSignal(int signum) {
    uint64_t signals = _SSignal_pendingSignalBitMask.load();
    signals >>= signum;
    bool result = signals & 1;
    return result;
}

bool SGetSignal(int signum) {
    uint64_t signals = _SSignal_pendingSignalBitMask.fetch_and(~(1 << signum));
    signals >>= signum;
    bool result = signals & 1;
    return result;
}

uint64_t SGetSignals() {
    return _SSignal_pendingSignalBitMask.load();
}

string SGetSignalDescription() {
    list<string> descriptions;
    for (int i = 0; i < 64; i++) {
        if (SCheckSignal(i)) {
            descriptions.push_back(strsignal(i));
        }
    }
    return SComposeList(descriptions);
}

void SClearSignals() {
    _SSignal_pendingSignalBitMask.store(0);
}

void SInitializeSignals() {
    // Our default die function does nothing.
    SSignalHandlerDieFunc = [](){};

    // Clear the thread-local signal number.
    _SSignal_threadCaughtSignalNumber = 0;

    stack_t stackInfo {&__SIGSTACK, 0, sigStackSize};
    sigaltstack(&stackInfo, 0);

    // Make a set of all signals except certain exceptions. These exceptions will cause an `abort()` and attempt to log
    // a stack trace before exiting. All other signals will get passed to the signal handling thread.
    sigset_t signals;
    sigfillset(&signals);
    sigdelset(&signals, SIGSEGV);
    sigdelset(&signals, SIGABRT);
    sigdelset(&signals, SIGFPE);
    sigdelset(&signals, SIGILL);
    sigdelset(&signals, SIGBUS);

    // Block all signals not specified above.
    sigprocmask(SIG_BLOCK, &signals, 0);

    // This is the signal action structure we'll use to specify what to listen for.
    struct sigaction newAction = {0};

    // The old style handler is explicitly null
    newAction.sa_handler = nullptr;

    // The new style handler is _SSignal_StackTrace.
    newAction.sa_sigaction = &_SSignal_StackTrace;

    // While we're inside the signal handler, we want to block any other signals from occurring until we return.
    sigset_t allSignals;
    sigfillset(&allSignals);
    newAction.sa_mask = allSignals;

    // And set the handlers for the few signals we care about in each thread.
    sigaction(SIGSEGV, &newAction, 0);
    sigaction(SIGABRT, &newAction, 0);
    sigaction(SIGFPE, &newAction, 0);
    sigaction(SIGILL, &newAction, 0);
    sigaction(SIGBUS, &newAction, 0);

    // If we haven't started the signal handler thread, start it now.
    bool threadAlreadyStarted = _SSignal_threadInitialized.test_and_set();
    if (!threadAlreadyStarted) {
        _SSignal_signalThread = thread(_SSignal_signalHandlerThreadFunc);
    }
}

void _SSignal_signalHandlerThreadFunc() {
    // Initialize logging for this thread.
    SLogSetThreadName("signal");
    SLogSetThreadPrefix("xxxxxx ");

    // Make a set of all signals.
    sigset_t signals;
    sigfillset(&signals);

    // Now we wait for any signal to occur.
    while (true) {

        // Wait for a signal to appear.
        siginfo_t siginfo = {0};
        struct timespec timeout;
        timeout.tv_sec = 0;
        timeout.tv_nsec = 100'000'000; // 100ms in ns.
        int result = -1;
        while (result == -1) {
            result = sigtimedwait(&signals, &siginfo, &timeout);
            if (_SSignal_threadStopFlag) {
                // Done.
                SINFO("Stopping signal handler thread.");
                return;
            }
        }
        int signum = siginfo.si_signo;

        if (result > 0) {
            // Do the same handling for these functions here as any other thread.
            if (signum == SIGSEGV || signum == SIGABRT || signum == SIGFPE || signum == SIGILL || signum == SIGBUS) {
                _SSignal_StackTrace(signum, nullptr, nullptr);
            } else {
                // Handle every other signal just by setting the mask. Anyone that cares can look them up.
                SINFO("Got Signal: " << strsignal(signum) << "(" << signum << ").");
                _SSignal_pendingSignalBitMask.fetch_or(1 << signum);
            }
        }
    }
}

void SStopSignalThread() {
    _SSignal_threadStopFlag = true;
    if (_SSignal_threadInitialized.test_and_set()) {
        // Send ourselves a signal to interrupt our thread.
        SINFO("Joining signal thread.");
        _SSignal_signalThread.join();
        _SSignal_threadInitialized.clear();
    }
}

void _SSignal_StackTrace(int signum, siginfo_t *info, void *ucontext) {
    if (signum == SIGSEGV || signum == SIGABRT || signum == SIGFPE || signum == SIGILL || signum == SIGBUS) {
        // If we haven't already saved a signal number, we'll do it now. Any signal we catch here will generate a
        // second ABORT signal, and we don't want that to overwrite this value, so we only set it if unset.
        if (!_SSignal_threadCaughtSignalNumber) {
            _SSignal_threadCaughtSignalNumber = signum;

            // What we'd like to do here is log a stack trace to syslog. Unfortunately, neither computing the stack
            // trace nor logging to to syslog are signal safe, so we try a couple things, doing as little as possible,
            // and hope that they work (they usually do, though it's not guaranteed).

            // Build the callstack. Not signal-safe, so hopefully it works.
            void* callstack[100];
            int depth = backtrace(callstack, 100);

            // Log it to a file. Everything in this block should be signal-safe, if we managed to generate the
            // backtrace in the first place.
            int fd = creat("/tmp/bedrock_crash.log", 0666);
            if (fd != -1) {
                backtrace_symbols_fd(callstack, depth, fd);
                close(fd);
            }

            // Then try and log it to syslog. Neither backtrace_symbols() nor syslog() are signal-safe, either, so this
            // also might not do what we hope.
            SWARN("Signal " << strsignal(_SSignal_threadCaughtSignalNumber) << "(" << _SSignal_threadCaughtSignalNumber
                  << ") caused crash, logging stack trace.");
            vector<string> stack = SGetCallstack(depth, callstack);
            for (const auto& frame : stack) {
                SWARN(frame);
            }

            // Call our die function and then reset it.
            SWARN("Calling DIE function.");
            SSignalHandlerDieFunc();
            SSignalHandlerDieFunc = [](){};
            SWARN("DIE function returned.");
            if (SQLiteNode::KILLABLE_SQLITE_NODE) {
                SWARN("Killing peer connections.");
                SQLiteNode::KILLABLE_SQLITE_NODE->kill();
            }
        }

        // If we weren't already in ABORT, we'll call that. The second call will skip the above callstack generation.
        if (signum != SIGABRT) {
            SWARN("Aborting.");
            abort();
        } else {
            SWARN("Already in ABORT.");
        }
    } else {
        SALERT("Non-signal thread got signal " << strsignal(signum) << "(" << signum << "), which wasn't expected");
    }
}
