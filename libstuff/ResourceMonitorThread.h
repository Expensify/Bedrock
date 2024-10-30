#include "libstuff/libstuff.h"
#include <thread>

using namespace std;

// This class is a wrapper around the default thread. We use it to collect the thread CPU usage. That allows us
// to investigate if we have any threads using more resources than it should, which can cause CPU usage peaks in
// the cluster.
class ResourceMonitorThread : public thread
{
public:
    // When calling this constructor, if you're passing a class member function as the `f` parameter and that
    // function receives parameters, you will need to wrap your function call in a lambda, doing something like:
    // ResourceMonitorThread([=, this]{ this->memberFunction(param1, param2);});
    template<typename F, typename... Args>
    ResourceMonitorThread(F&& f, Args&&... args):
      thread(ResourceMonitorThread::wrapper<F&&, Args&&...>, forward<F&&>(f), forward<Args&&>(args)...){};
private:
    thread_local static uint64_t threadStartTime;
    thread_local static double cpuStartTime;

    static void beforeProcessStart();
    static void afterProcessFinished();

    template<typename F, typename... Args>
    static void wrapper(F&& f, Args&&... args) {
        beforeProcessStart();
        invoke(forward<F>(f), forward<Args>(args)...);
        afterProcessFinished();
    }
};