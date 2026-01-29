#pragma once

#include <thread>
#include <future>
#include <tuple>
#include <utility>
#include <type_traits>

#include "libstuff.h"

using namespace std;

// SThread is a thread wrapper intended to be used in the same way as thread,
// except that it will trap exceptions (including signal-generated exceptions like SIGSEGV)
// and pass them back to the caller as part of a promise.
template<class F, class ... Args>
auto SThread(F&& f, Args&&... args)
{
    // Create type aliases for the function, argument list, and return types.
    // These are decayed as per decay (https://en.cppreference.com/w/cpp/types/decay.html)
    // which makes the same sort of type conversions that the compiler makes when passign by value.
    using Fn = decay_t<F>;

    // We create a tuple from the passed args to allow passing variadic arguments to our lambda below.
    using DecayedArgsTuple = tuple<decay_t<Args> ...>;

    // Create an alias to the return type of the passed function with the passed args:
    using return_type = invoke_result_t<Fn, decay_t<Args> ...>;

    // Now we create the promise and future we will need to return the result from this invocation. We get
    // the future here, because we will pass the promise by move to the thread lambda, and that will leave the
    // promise argument invalid after that point.
    promise<return_type> prom;
    auto fut = prom.get_future();

    // Now we can create the callable function and it's arguments that we will pass to our lambda.
    Fn fn(forward<F>(f));
    DecayedArgsTuple argTuple(forward<Args>(args)...);

    // Finally we can create our new thread and pass it our function and arguments.
    thread t(
        [p = move(prom), fn = move(fn), argTuple = move(argTuple)]() mutable {
        // Initialize signal handling for this thread and mark it as recoverable.
        // This allows signals like SIGSEGV/SIGFPE to be converted to SSignalException
        // instead of aborting the process.
        SInitializeSignals();
        SSetThreadRecoverable(true);

        // Set up the recovery point for signal handling using sigsetjmp.
        // If a signal occurs, the handler will call siglongjmp and sigsetjmp will
        // "return" with the signal number instead of 0.
        int signalCaught = sigsetjmp(*SGetRecoveryPoint(), 1);
        SSetRecoveryPointActive(true);

        if (signalCaught != 0) {
            // We got here via siglongjmp from the signal handler.
            // signalCaught contains the signal number.
            SSetRecoveryPointActive(false);
            SSetThreadRecoverable(false);

            // Build the exception from the crash info stored by the signal handler.
            SSignalException ex = SBuildSignalException();

            SWARN("Signal exception in SThread: " << ex.what());
            ex.logStackTrace();
            p.set_exception(make_exception_ptr(ex));
            return;
        }

        try {
            // We call `apply` to use our argments from a tuple as if they were a list of discrete arguments.
            // This is effectively like calling `invoke` and passing the arguments separately.
            // We check the return type of our function as we will either need to pass the result of the function to
            // set_value() or not depending on whether the funtion returns anything.
            if constexpr (is_void_v<return_type> ) {
                apply(move(fn), move(argTuple));
                p.set_value();
            } else {
                p.set_value(apply(move(fn), move(argTuple)));
            }
        } catch (const SSignalException& e) {
            // Signal-generated exception - log stack trace before propagating.
            SWARN("Signal exception in SThread: " << e.what());
            e.logStackTrace();
            p.set_exception(current_exception());
        } catch (const exception& e) {
            SWARN("Uncaught exception in SThread: " << e.what());
            p.set_exception(current_exception());
        } catch (...) {
            SWARN("Uncaught exception in SThread: unknown type");
            p.set_exception(current_exception());
        }

        // Restore non-recoverable state (belt-and-suspenders, thread is ending anyway).
        SSetRecoveryPointActive(false);
        SSetThreadRecoverable(false);
    }
    );

    // Now our function has started and we can return to the caller. We pass pack the thread object so that the caller can wait
    // for it to complete, and also the future, so that the caller can check if there were any exceptions.
    return make_pair(move(t), move(fut));
}
