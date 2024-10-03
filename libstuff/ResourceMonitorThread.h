#include "libstuff/libstuff.h"
#include <thread>

using namespace std;

class ResourceMonitorThread: public thread
{
public:
    template<typename F, typename... Args,
	     typename = _Require<__not_<is_same<__remove_cvref_t<F>, thread>>>>
      explicit
    ResourceMonitorThread(F&& __f, Args&&... __args): thread(wrapper<F, Args...>, forward<F>(__f), forward<Args>(__args)...){};

    string getThreadName();
    virtual ~ResourceMonitorThread();

private:
    static thread_local uint64_t startTime;
    static thread_local double cpuStartTime;

    static void before();
    static void after();
    template<typename F, typename... Args>
    const static inline function<void(F&& f, Args&&... args)> wrapper = [](F&& f, Args&&... args){
        before();
        f(args...);
        after();
    };

};

