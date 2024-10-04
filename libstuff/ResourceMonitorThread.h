#include "libstuff/libstuff.h"
#include <thread>

using namespace std;

// This class is a wrapper around the default thread. We use it to collect the thread CPU usage. That allows us
// to investigate if we have any threads using more resources than it should, which can cause CPU usage peaks in
// the cluster.
class ResourceMonitorThread: public thread
{
public:
    // When calling this constructor, if you're passing a class member function as the `f` parameter and that
    // function receives parameters, you will need to wrap your function call in a lambda, doing something like:
    // ResourceMonitorThread([=, this]{ this->memberFunction(param1, param2);});
    template<typename F, typename... Args>
    ResourceMonitorThread(F&& f, Args&&... args):
      thread(ResourceMonitorThread::wrapper<F&&, Args&&...>, forward<F&&>(f), forward<Args&&>(args)...){};
private:
    static thread_local uint64_t startTime;
    static thread_local double cpuStartTime;

    static void before();
    static void after();

    template<typename F, typename... Args>
    static void wrapper(F&& f, Args&&... args) {
        before();
        std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
        after();
    }
};