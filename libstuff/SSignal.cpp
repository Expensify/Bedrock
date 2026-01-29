#include "libstuff.h"
#include "SSynchronizedQueue.h"
#include <sqlitecluster/SQLiteNode.h>
#include <cxxabi.h>
#include <execinfo.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <format>

// ucontext.h is needed for signal recovery on Linux
#if defined(__linux__)
#include <ucontext.h>
#endif

thread_local function<string()> SSignalHandlerDieFunc;
void SSetSignalHandlerDieFunc(function<string()>&& func)
{
    SSignalHandlerDieFunc = move(func);
}

// 64kb emergency stack location.
constexpr auto sigStackSize{1024 * 64};
char __SIGSTACK[sigStackSize];

void* SSIGNAL_NOTIFY_INTERRUPT;

// The function to call in our thread that handles signals.
void _SSignal_signalHandlerThreadFunc();

// The function to call in threads handling their own signals. This is only used for exception signals like SEGV
// and FPE.
void _SSignal_StackTrace(int signum, siginfo_t* info, void* ucontext);

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

// Thread-local flag indicating this thread should convert signals to exceptions instead of aborting.
// Used by SThread to enable graceful recovery from SIGSEGV/SIGFPE.
thread_local bool _SSignal_threadIsRecoverable = false;

// Thread-local flag to prevent recursive recovery attempts (e.g., if trampoline itself crashes).
thread_local bool _SSignal_inTrampoline = false;

// Thread-local storage for crash info to be passed to the trampoline.
struct SSignalCrashInfo {
    int signum;
    void* faultAddress;
    void* instructionPointer;
    void* callstack[32];
    int callstackDepth;
    bool hasCrashInfo;
};
thread_local SSignalCrashInfo _SSignal_crashInfo = {0, nullptr, nullptr, {}, 0, false};

// The number of termination signals received so far.
atomic<uint64_t> _SSignal_terminationCount(0);
uint64_t STerminationSignalCount()
{
    return _SSignal_terminationCount.load();
}

bool SCheckSignal(int signum)
{
    uint64_t signals = _SSignal_pendingSignalBitMask.load();
    signals >>= signum;
    bool result = signals & 1;
    return result;
}

bool SGetSignal(int signum)
{
    uint64_t signals = _SSignal_pendingSignalBitMask.fetch_and(~(1 << signum));
    signals >>= signum;
    bool result = signals & 1;
    return result;
}

uint64_t SGetSignals()
{
    return _SSignal_pendingSignalBitMask.load();
}

string SGetSignalDescription()
{
    list<string> descriptions;
    for (int i = 0; i < 64; i++) {
        if (SCheckSignal(i)) {
            descriptions.push_back(strsignal(i));
        }
    }
    return SComposeList(descriptions);
}

void SClearSignals()
{
    _SSignal_pendingSignalBitMask.store(0);
}

void SSetThreadRecoverable(bool recoverable)
{
    _SSignal_threadIsRecoverable = recoverable;
}

bool SIsThreadRecoverable()
{
    return _SSignal_threadIsRecoverable;
}

void SInitializeSignals()
{
    // Our default die function does nothing.
    SSignalHandlerDieFunc = [](){
        return "";
    };

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
    struct sigaction newAction = {};

    // The old style handler is explicitly null
    newAction.sa_handler = nullptr;
    newAction.sa_flags = SA_ONSTACK;

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

void _SSignal_signalHandlerThreadFunc()
{
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

                if (signum == SIGTERM || signum == SIGINT) {
                    _SSignal_terminationCount.fetch_add(1);
                }
            }
        }

        if (SSIGNAL_NOTIFY_INTERRUPT) {
            static_cast<SSynchronizedQueue<bool>*>(SSIGNAL_NOTIFY_INTERRUPT)->push(true);
        }
    }
}

void SStopSignalThread()
{
    _SSignal_threadStopFlag = true;
    if (_SSignal_threadInitialized.test_and_set()) {
        // Send ourselves a signal to interrupt our thread.
        SINFO("Joining signal thread.");
        _SSignal_signalThread.join();
        _SSignal_threadInitialized.clear();
    }
}

// Platform-specific helpers for ucontext manipulation.
// These allow us to redirect execution after a signal to a trampoline function.
#if defined(__linux__)
static void _SSignal_SetInstructionPointer(ucontext_t* ctx, void* target) {
    #if defined(__x86_64__)
        ctx->uc_mcontext.gregs[REG_RIP] = (greg_t)target;
    #elif defined(__aarch64__)
        ctx->uc_mcontext.pc = (unsigned long)target;
    #else
        #error "Unsupported architecture for signal recovery"
    #endif
}

static void* _SSignal_GetInstructionPointer(ucontext_t* ctx) {
    #if defined(__x86_64__)
        return (void*)ctx->uc_mcontext.gregs[REG_RIP];
    #elif defined(__aarch64__)
        return (void*)ctx->uc_mcontext.pc;
    #else
        return nullptr;
    #endif
}
#endif

// Trampoline function that runs after the signal handler returns.
// This executes on the original thread's stack and throws an SSignalException.
// The signal handler redirects execution here by modifying the instruction pointer in ucontext.
__attribute__((noinline, noreturn))
void _SSignal_Trampoline() {
    // Mark that we're in the trampoline to prevent recursive recovery attempts.
    _SSignal_inTrampoline = true;

    // Copy crash info from TLS to stack before throwing.
    SSignalCrashInfo info = _SSignal_crashInfo;
    _SSignal_crashInfo.hasCrashInfo = false;

    // Throw the exception - this will unwind to the catch block in SThread.
    throw SSignalException(
        info.signum,
        info.faultAddress,
        info.instructionPointer,
        info.callstack,
        info.callstackDepth
    );
}

void _SSignal_StackTrace(int signum, siginfo_t* info, void* ucontext)
{
#if defined(__linux__) && (defined(__x86_64__) || defined(__aarch64__))
    // Check if this is a recoverable signal in a recoverable thread.
    // SIGABRT is not recoverable - it's usually called intentionally or as a result of another crash.
    bool isRecoverableSignal = (signum == SIGSEGV || signum == SIGFPE || signum == SIGBUS || signum == SIGILL);

    if (isRecoverableSignal && _SSignal_threadIsRecoverable && !_SSignal_inTrampoline && ucontext != nullptr) {
        ucontext_t* ctx = static_cast<ucontext_t*>(ucontext);

        // Store crash information in thread-local storage for the trampoline.
        _SSignal_crashInfo.signum = signum;
        _SSignal_crashInfo.faultAddress = info ? info->si_addr : nullptr;
        _SSignal_crashInfo.instructionPointer = _SSignal_GetInstructionPointer(ctx);

        // Capture backtrace while we have the original context.
        // Note: backtrace() is not strictly signal-safe but usually works.
        _SSignal_crashInfo.callstackDepth = backtrace(
            _SSignal_crashInfo.callstack,
            sizeof(_SSignal_crashInfo.callstack) / sizeof(void*)
        );
        _SSignal_crashInfo.hasCrashInfo = true;

        SWARN("Signal " << strsignal(signum) << "(" << signum << ") in recoverable thread, "
              << "redirecting to exception trampoline. Fault address: " << (info ? info->si_addr : nullptr));

        // Modify the instruction pointer to point to our trampoline.
        // When the signal handler returns, execution will resume at the trampoline.
        _SSignal_SetInstructionPointer(ctx, (void*)&_SSignal_Trampoline);

        // Return from signal handler - execution will resume at trampoline on original thread stack.
        return;
    }
#endif

    if (signum == SIGSEGV || signum == SIGABRT || signum == SIGFPE || signum == SIGILL || signum == SIGBUS) {
        // If we haven't already saved a signal number, we'll do it now. Any signal we catch here will generate a
        // second ABORT signal, and we don't want that to overwrite this value, so we only set it if unset.
        if (!_SSignal_threadCaughtSignalNumber) {
            _SSignal_threadCaughtSignalNumber = signum;

            SWARN("Signal " << strsignal(_SSignal_threadCaughtSignalNumber) << "(" << _SSignal_threadCaughtSignalNumber << ") caused crash, logging stack trace.");

            // What we'd like to do here is log a stack trace to syslog. Unfortunately, neither computing the stack
            // trace nor logging to to syslog are signal safe, so we try a couple things, doing as little as possible,
            // and hope that they work (they usually do, though it's not guaranteed).

            // Build the callstack. Not signal-safe, so hopefully it works.
            void** callstack{0};
            int max_depth = 10;
            int depth{0};
            while (true) {
                if (callstack) {
                    free(callstack);
                }
                callstack = (void**) malloc(sizeof(void*) * max_depth);
                depth = backtrace(callstack, max_depth);
                if (depth == max_depth) {
                    max_depth *= 2;
                } else {
                    break;
                }
            }

            if (depth > 40) {
                SWARN("Stack depth is " << depth << " only logging first and last 20 frames.");
            }

            // We mainly depend on syslog to investigate crashes, but when performance is real bad, sometimes we lose those logs.
            // For cases like those, we'll also save the stack trace for the crash in a file.
            int fd = creat(format("/tmp/bedrock_crash_{}.log", STimeNow()).c_str(), 0666);
            for (int i = 0; i < depth; i++) {
                if (depth > 40 && i >= 20 && i < depth - 20) {
                    // Skip frames in the middle of large stacks.
                    continue;
                }
                char** frame{0};
                frame = backtrace_symbols(&(callstack[i]), 1);
                int status{0};
                char* front = strchr(frame[0], '(') + 1;
                char* end = strchr(front, '+');
                char copy[1000];
                memset(copy, 0, 1000);
                strncpy(copy, front, min((size_t) 999, (size_t) (end - front)));
                char* demangled = abi::__cxa_demangle(copy, 0, 0, &status);
                char* tolog = status ? copy : demangled;
                if (tolog[0] == '\0') {
                    tolog = frame[0];
                }
                string fullLogLine = format("Frame #{}: {}", i, tolog);
                SWARN(fullLogLine);
                if (fd != -1) {
                    fullLogLine = format("{}{}", fullLogLine, "\n");
                    write(fd, fullLogLine.c_str(), strlen(fullLogLine.c_str()));
                }
                free(frame);
            }
            // Done.
            free(callstack);

            // Call our die function and then reset it.
            SWARN("Calling DIE function.");
            string logMessage = SSignalHandlerDieFunc();
            if (!logMessage.empty()) {
                SALERT(logMessage);
            }
            SSignalHandlerDieFunc = [](){
                return "";
            };
            SWARN("DIE function returned.");

            // Finish writing the crash file with the request details if it exists
            if (fd != -1 && !logMessage.empty()) {
                logMessage += "\n";
                write(fd, logMessage.c_str(), strlen(logMessage.c_str()));
            }
            close(fd);

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

// SSignalException implementation
SSignalException::SSignalException(int signum,
                                   void* faultAddress,
                                   void* instructionPointer,
                                   void* const* callstack,
                                   int depth)
    : _signum(signum),
      _faultAddress(faultAddress),
      _instructionPointer(instructionPointer),
      _depth(min(depth, CALLSTACK_LIMIT))
{
    if (callstack && _depth > 0) {
        memcpy(_callstack, callstack, _depth * sizeof(void*));
    }
}

const char* SSignalException::what() const noexcept {
    if (_whatMessage.empty()) {
        // Build message lazily to avoid issues in constructor context.
        try {
            _whatMessage = "Signal " + string(signalName()) + " at address " + SToHex((uint64_t)_faultAddress);
        } catch (...) {
            _whatMessage = "Signal exception";
        }
    }
    return _whatMessage.c_str();
}

const char* SSignalException::signalName() const noexcept {
    switch (_signum) {
        case SIGSEGV: return "SIGSEGV";
        case SIGFPE:  return "SIGFPE";
        case SIGBUS:  return "SIGBUS";
        case SIGILL:  return "SIGILL";
        default:      return "UNKNOWN";
    }
}

vector<string> SSignalException::stackTrace() const noexcept {
    return SGetCallstack(_depth, _callstack);
}

void SSignalException::logStackTrace() const noexcept {
    try {
        SWARN("Signal " << signalName() << " at fault address "
              << SToHex((uint64_t)_faultAddress)
              << ", instruction " << SToHex((uint64_t)_instructionPointer));
        for (const auto& frame : stackTrace()) {
            SWARN("  " << frame);
        }
    } catch (...) {
        // Logging failed, but we're noexcept so just ignore.
    }
}
