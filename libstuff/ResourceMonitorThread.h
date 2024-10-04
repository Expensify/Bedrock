#include "libstuff/libstuff.h"
#include <thread>

using namespace std;

class ResourceMonitorThread: public thread
{
public:
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