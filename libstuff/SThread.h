/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SThread.h
 * Path:    libstuff/SThread.h
 *
 * INTENT
 *   Free-function template that launches a thread the same way
 *   std::thread does, but captures any exception the callable throws and
 *   delivers it back to the caller through a std::future instead of
 *   letting it call terminate.
 *
 * OBJECTS
 *   SThread  - variadic function template; wraps `f(args...)` in a
 *              try/catch inside the new thread, routes the return value
 *              (or the caught exception) through a promise/future pair,
 *              and returns {thread, future} as a pair for the caller to
 *              join/wait on.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   [CANDIDATE] `SThread` is a lower-case-callable free function template
 *   named like a type (PascalCase, S-prefix normally reserved for classes
 *   in this repo) — it reads as "construct an SThread object" rather than
 *   "call a function", which is a bit misleading given repo convention.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once

#include <thread>
#include <future>
#include <tuple>
#include <utility>
#include <type_traits>

#include "libstuff.h"

using namespace std;

// SThread is a thread wrapper intended to be used in the same way as thread,
// except that it will trap exceptions and pass them back to the caller as part of a promise.
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
        } catch (const exception& e) {
            SWARN("Uncaught exception in SThread: " << e.what());
            p.set_exception(current_exception());
        } catch (...) {
            SWARN("Uncaught exception in SThread: unknown type");
            p.set_exception(current_exception());
        }
    }
    );

    // Now our function has started and we can return to the caller. We pass pack the thread object so that the caller can wait
    // for it to complete, and also the future, so that the caller can check if there were any exceptions.
    return make_pair(move(t), move(fut));
}
