#include "libstuff.h"

atomic_flag SSignal::_threadInitialized = ATOMIC_FLAG_INIT;
atomic<uint64_t> SSignal::_pendingSignalBitMask(0);
thread SSignal::_signalThread;

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
    // Make a set of all signals except SIGSEGV and SIGABRT, we only handle those signals in individual threads,
    // otherwise, signals get passed to the signal handler thread.
    sigset_t signals;
    sigfillset(&signals);
    sigdelset(&signals, SIGSEGV);
    sigdelset(&signals, SIGABRT);
    sigdelset(&signals, SIGFPE);
    sigdelset(&signals, SIGILL);
    sigdelset(&signals, SIGBUS);

    // Block all the signals on this thread except SIGSEGV and SIGABRT. They'll be handled by the signal handler thread
    // instead.
    sigprocmask(SIG_BLOCK, &signals, 0);

    // This is the signal action structure we'll use to specify what to listen for.
    struct sigaction newAction;

    // The old style handler is explicitly null
    newAction.sa_handler = nullptr;

    // The new style handler is _signalHandler.
    newAction.sa_sigaction = &_signalHandler;

    // While we're inside the signal handler, we want to block any other signals from occurring until we return.
    // Block all other signals while we're inside the signal handler.
    sigset_t allSignals;
    sigfillset(&allSignals);
    newAction.sa_mask = allSignals;

    // And set the handlers for the two signals we care about.
    sigaction(SIGSEGV, &newAction, 0);
    sigaction(SIGABRT, &newAction, 0);
    sigaction(SIGFPE, &newAction, 0);
    sigaction(SIGILL, &newAction, 0);
    sigaction(SIGBUS, &newAction, 0);

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

    // Now we wait for any signal to occur. Each thread should handle it's own SEGV or ABRT, but there's nothing
    // stopping those from happening here as well, so we also handle them here.
    while (true) {
        // Wait for a signal to appear.
        int signum = 0;
        int result = sigwait(&signals, &signum);
        if (!result) {
            // Do the same handling for these functions here as any other thread gets.
            if (signum == SIGSEGV || signum == SIGABRT || signum == SIGFPE || signum == SIGILL || signum == SIGBUS) {
                _signalHandler(signum, nullptr, nullptr);
            } else {
                // Handle every other signal just by setting the mask. Functions that care can look them up.
                SINFO("Got Signal: " << strsignal(signum) << "(" << signum << ").");
                _pendingSignalBitMask.fetch_or(1 << signum);
            }
        } else {
            // Error. Should be EINVAL if we supplied an invalid signal number.
        }
    }
}

void SSignal::_signalHandler(int signum, siginfo_t *info, void *ucontext) {
    if (signum == SIGSEGV || signum == SIGABRT || signum == SIGFPE || signum == SIGILL || signum == SIGBUS) {
        if (signum == SIGABRT) {
            SWARN("Got SIGABRT, logging stack trace.");
            // backtrace_symbols_fd(); // Should be safe in signal handler.
            SLogStackTrace();
        } else if (signum == SIGSEGV) {
            SWARN("Got " << strsignal(signum) << ", calling abort().");
            abort();
        }
    } else {
        SWARN("Non-signal thread got signal " << strsignal(signum) << ", which wasn't expected");
    }
}
