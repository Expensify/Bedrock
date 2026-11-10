#pragma once

#include <thread>
#include <future>
#include <memory>
#include <pthread.h>
#include <system_error>
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

// SDetachedThread starts a thread in the same way as thread, except that the thread is created already detached. Use it
// instead of thread::detach(): before glibc 2.43, pthread_detach can read the thread's memory after the thread has exited
// and freed it, crashing the caller (https://sourceware.org/bugzilla/show_bug.cgi?id=19951).
// Like thread, it throws system_error if the thread can't be started, and an exception escaping `f` calls terminate.
template<class F, class ... Args>
void SDetachedThread(F&& f, Args&&... args)
{
    // As with thread, the function and its arguments are copied or moved on the calling thread.
    using DecayedArgsTuple = tuple<decay_t<Args> ...>;
    using Task = pair<decay_t<F>, DecayedArgsTuple>;
    auto task = make_unique<Task>(forward<F>(f), DecayedArgsTuple(forward<Args>(args)...));

    pthread_attr_t attributes;
    int error = pthread_attr_init(&attributes);
    if (!error) {
        error = pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_DETACHED);
        if (!error) {
            pthread_t threadID;
            error = pthread_create(&threadID, &attributes, [](void* taskPointer) -> void* {
                unique_ptr<Task> threadTask(static_cast<Task*>(taskPointer));
                apply(move(threadTask->first), move(threadTask->second));
                return nullptr;
            }, task.get());
        }
        pthread_attr_destroy(&attributes);
    }
    if (error) {
        throw system_error(error, generic_category());
    }

    // The new thread owns the task now, and may have already finished and deleted it.
    task.release();
}
