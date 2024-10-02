#include "ResourceMonitorThread.h"
#include "libstuff/libstuff.h"
#include "format"
#include "thread"

ResourceMonitorThread::ResourceMonitorThread(_Callable&& __f, _Args&&... __args) : thread(__f, __args)
{
    startTime = STimeNow();
    cpuStartTime = SGetCPUUserTime();
};

ResourceMonitorThread::~ResourceMonitorThread(){
    const uint64_t threadEndTime = STimeNow() - startTime;
    const double CPUUserTime = SGetCPUUserTime() - cpuStartTime;
    const double cpuUserPercentage = (CPUUserTime / static_cast<double>(threadEndTime)) * 100;
    const pid_t tid = syscall(SYS_gettid); 
    SINFO(format("Thread finished. pID: '{}', CPUTime: '{}µs', CPUPercentage: '{}%' ", tid, cpuUserPercentage, CPUUserTime));
}