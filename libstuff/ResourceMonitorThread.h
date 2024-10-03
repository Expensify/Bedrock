#include "libstuff/libstuff.h"
#include <thread>

using namespace std;

class ResourceMonitorThread: public thread
{
public:
    template<typename _Callable, typename... _Args,
	     typename = _Require<__not_<is_same<__remove_cvref_t<_Callable>, thread>>>>
      explicit
    ResourceMonitorThread(_Callable&& __f, _Args&&... __args): thread(forward<_Callable>(__f), forward<_Args>(__args)...){
        startTime = STimeNow();
        cpuStartTime = SGetCPUUserTime();
    };

    virtual ~ResourceMonitorThread();

private:
    uint64_t startTime = 0;
    double cpuStartTime = 0;
};
