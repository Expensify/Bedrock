#include "libstuff.h"
#include <execinfo.h> // for backtrace

atomic_flag SSignal::_threadInitialized = ATOMIC_FLAG_INIT;
atomic<uint64_t> SSignal::_pendingSignalBitMask(0);
thread SSignal::_signalThread;
thread_local int SSignal::_threadCaughtSignalNumber = 0;

bool SSignal::checkSignal(int signum) {
    uint64_t signals = _pendingSignalBitMask.load();
    signals >>= signum;
    bool result = signals & 1;
    return result;
}

bool SSignal::getSignal(int signum) {
    uint64_t signals = _pendingSignalBitMask.fetch_and(~(1 << signum));
    signals >>= signum;
    bool result = signals & 1;
    return result;
}

uint64_t SSignal::getSignals() {
    return _pendingSignalBitMask.load();
}

string SSignal::getSignalDescription() {
    list<string> descriptions;
    for (int i = 0; i < 64; i++) {
        if (checkSignal(i)) {
            descriptions.push_back(strsignal(i));
        }
    }
    return SComposeList(descriptions);
}

void SSignal::clearSignals() {
    _pendingSignalBitMask.store(0);
}

void SSignal::initializeSignals() {
    // Clear the thread-local signal number.
    _threadCaughtSignalNumber = 0;

    // Make a set of all signals except certain exceptions. These exceptions will cause an `abort()` and attempt to log
    // a stack trace before esiting. All other signals will get passed to the signal handling thread.
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
    struct sigaction newAction;

    // The old style handler is explicitly null
    newAction.sa_handler = nullptr;

    // The new style handler is _signalHandler.
    newAction.sa_sigaction = &_signalHandler;

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
    bool threadAlreadyStarted = _threadInitialized.test_and_set();
    if (!threadAlreadyStarted) {
        _signalThread = thread(_signalHandlerThreadFunc);
        _signalThread.detach();
    }
}

void SSignal::_signalHandlerThreadFunc() {
    // Initialize logging for this thread.
    SLogSetThreadName("signal");
    SLogSetThreadPrefix("xxxxx ");

    // Make a set of all signals.
    sigset_t signals;
    sigfillset(&signals);

    // Now we wait for any signal to occur.
    while (true) {
        // Wait for a signal to appear.
        int signum = 0;
        int result = sigwait(&signals, &signum);
        if (!result) {
            // Do the same handling for these functions here as any other thread.
            if (signum == SIGSEGV || signum == SIGABRT || signum == SIGFPE || signum == SIGILL || signum == SIGBUS) {
                _signalHandler(signum, nullptr, nullptr);
            } else {
                // Handle every other signal just by setting the mask. Anyone that cares can look them up.
                SINFO("Got Signal: " << strsignal(signum) << "(" << signum << ").");
                _pendingSignalBitMask.fetch_or(1 << signum);
            }
        }
    }
}

void SSignal::_signalHandler(int signum, siginfo_t *info, void *ucontext) {
    if (signum == SIGSEGV || signum == SIGABRT || signum == SIGFPE || signum == SIGILL || signum == SIGBUS) {
        // If we haven't already saved a signal number, we'll do it now. Any signal we catch here will generate a
        // second ABORT signal, and we don't want that to overwrite this value, so we only set it if unset.
        if (!_threadCaughtSignalNumber) {
            _threadCaughtSignalNumber = signum;
        }
        if (signum == SIGABRT) {
            // What we'd like to do here is log a stack trace to syslog. Unfortunately, neither computing the stack
            // trace nor logging to to syslog are signal safe, so we try a couple things, doing as little as possible,
            // and hope that they work (they usually do, though it's not guaranteed).

            // Build the callstack. Not signal-safe, so hopefully it works.
            void* callstack[100];
            int depth = backtrace(callstack, 100);

            // Log it to a file.Everything in this block should be signal-safe, if we managed to generate the
            // backtrace in the first place.
            int fd = creat("/tmp/bedrock_crash.log", 0666);
            if (fd != -1) {
                backtrace_symbols_fd(callstack, depth, fd);
                close(fd);
            }

            // Then try and log it to syslog. Neither backtrace_symbols() nor syslog() are signal-safe, either, so this
            // also might not do what we hope.
            SWARN("Signal " << strsignal(_threadCaughtSignalNumber) << "(" << _threadCaughtSignalNumber
                  << ") generated ABORT, logging stack trace.");
            char** symbols = backtrace_symbols(callstack, depth);
            for (int c = 0; c < depth; ++c) {
                SWARN(symbols[c]);
            }

            // NOTE: The best thing to do here is probably just record core files, which should work as expected. It's
            // possible that we might have data that we don't want getting written to disk, but `memset` is signal
            // safe, so we could keep a list of addresses to 0, and do that here before returning. Then the core file
            // wouldn't contain that data.
        } else {
            // We just call abort here, so that we'll generate a second signal that will be picked up above.
            abort();
        }
    } else {
        SALERT("Non-signal thread got signal " << strsignal(signum) << "(" << signum << "), which wasn't expected");
    }
}
