// SSignal.cpp
#include "libstuff.h"

// Defines some signals missing on various platforms
#ifdef __APPLE__
#define SIGPOLL SIGTERM
#define SIGSTKFLT SIGTERM
#define SIGCLD SIGTERM
#define SIGPWR SIGTERM
#define SIGUNUSED SIGTERM
#define SIGLOST SIGTERM
#elif __linux__
#define SIGEMT SIGTERM
#define SIGINFO SIGTERM
#define SIGLOST SIGTERM
#endif

// Store in a bitmask for which signals sent
uint64_t _SSignal_sentBitmask = 0;

// --------------------------------------------------------------------------
bool SCatchSignal(int signum) {
    // Signals are stored in the bitmask shifted by their signal number.  See
    // if this signal was sent.
    if (_SSignal_sentBitmask & (1 << signum)) {
        // It was!  Clear this signal so we don't re-catch it
        SHMMM("Caught signal '" << SGetSignalName(signum) << "'");
        _SSignal_sentBitmask &= ~(1 << signum);
        return true;
    }

    // Didn't catch this one
    return false;
}

// --------------------------------------------------------------------------
void SClearSignals() {
    // Clear all signals
    _SSignal_sentBitmask = 0;
}

// --------------------------------------------------------------------------
uint64_t SGetSignals() {
    // Just return the bitmask
    return _SSignal_sentBitmask;
}
// --------------------------------------------------------------------------
void SSendSignal(int signum) {
    // Store the signal
    //
    // **NOTE: We don't do any logging or call any system calls here because
    //         this signal function is also used as the signal handler, and
    //         this will interrupt a thread at any time -- including while
    //         inside a non-reentrant system call.
    _SSignal_sentBitmask |= (1 << signum);

    // If we get SIGABRT or SIGSEGV, we want to exit abnormally. That is to say, either explicitly call abort() or let
    // it continue naturally. 
    // We don't call `SERROR` because that calls `exit(1)`, which can interrupt our signal handling process and never
    // return. Instead, we explictly let this function return, so that abort() can complete, and we can generate core
    // files.
    if (signum == SIGABRT) {
        SWARN("Got SIGABRT, logging stack trace.");
        SLogStackTrace();
    } else if (signum == SIGSEGV) {
        SWARN("Got SIGSEGV, logging stack trace.");
        SLogStackTrace();
        abort();
    }
}

// --------------------------------------------------------------------------
string SGetSignalName(int signum) {
// There is some ambiguity on a per-system basis, so rather than using a
// name table, just use if
#define GETSIGNALNAME(_NAME_)                                                                                          \
    if (signum == _NAME_)                                                                                              \
        return #_NAME_;
    GETSIGNALNAME(SIGHUP);
    GETSIGNALNAME(SIGINT);
    GETSIGNALNAME(SIGQUIT);
    GETSIGNALNAME(SIGILL);
    GETSIGNALNAME(SIGABRT);
    GETSIGNALNAME(SIGFPE);
    GETSIGNALNAME(SIGKILL);
    GETSIGNALNAME(SIGSEGV);
    GETSIGNALNAME(SIGPIPE);
    GETSIGNALNAME(SIGALRM);
    GETSIGNALNAME(SIGTERM);
    GETSIGNALNAME(SIGUSR1);
    GETSIGNALNAME(SIGUSR2);
    GETSIGNALNAME(SIGCHLD);
    GETSIGNALNAME(SIGCONT);
    GETSIGNALNAME(SIGSTOP);
    GETSIGNALNAME(SIGTSTP);
    GETSIGNALNAME(SIGTTIN);
    GETSIGNALNAME(SIGTTOU);
    GETSIGNALNAME(SIGBUS);
    GETSIGNALNAME(SIGPOLL);
    GETSIGNALNAME(SIGPROF);
    GETSIGNALNAME(SIGSYS);
    GETSIGNALNAME(SIGTRAP);
    GETSIGNALNAME(SIGURG);
    GETSIGNALNAME(SIGVTALRM);
    GETSIGNALNAME(SIGXCPU);
    GETSIGNALNAME(SIGXFSZ);
    GETSIGNALNAME(SIGIOT);
    GETSIGNALNAME(SIGEMT);
    GETSIGNALNAME(SIGSTKFLT);
    GETSIGNALNAME(SIGIO);
    GETSIGNALNAME(SIGCLD);
    GETSIGNALNAME(SIGPWR);
    GETSIGNALNAME(SIGINFO);
    GETSIGNALNAME(SIGLOST);
    GETSIGNALNAME(SIGWINCH);
    GETSIGNALNAME(SIGUNUSED);
    return "(unknown#" + SToStr(signum) + ")";
}

// --------------------------------------------------------------------------
string SGetSignalNames(uint64_t sigmask) {
    // Just loop across and accumulate what signals are ready to be caught, but
    // don't actually catch them right now.
    list<string> signalList;
    for (int signum = 0; signum < 64; ++signum) {
        // Did we catch this signal?
        if (_SSignal_sentBitmask & (1 << signum)) {
            // Yep!
            signalList.push_back(SGetSignalName(signum));
        }
    }
    return SComposeList(signalList);
}

// --------------------------------------------------------------------------
void _SInitializeSignals() {
    // Catch all signals (or, rather, every signal we are able to catch).
    for (int signum = 0; signum < 64; ++signum) {
        signal(signum, SSendSignal);
    }
}
