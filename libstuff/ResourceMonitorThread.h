#include "libstuff/libstuff.h"
#include <thread>

using namespace std;

class ResourceMonitorThread: public thread
{
public:
    template<typename F, typename... Args,
	     typename = _Require<__not_<is_same<__remove_cvref_t<F>, thread>>>>
      explicit
    ResourceMonitorThread(F&& f, Args&&... args): thread(&wrapper<F, Args...>, forward<F>(f), forward<Args>(args)...){};

private:
    static thread_local uint64_t startTime;
    static thread_local double cpuStartTime;

    static void before();
    static void after();
    template<typename F, typename... Args>
    inline static function<void(F&& f, Args&&... args)> wrapper = [](F&& f, Args&&... args){
        before();
        std::invoke(f, std::forward<Args>(args)...);
        after();
    };
};

