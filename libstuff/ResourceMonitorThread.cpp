#include "ResourceMonitorThread.h"
#include "libstuff/libstuff.h"
#include "format"


void ResourceMonitorThread::before(){
    startTime = STimeNow();
    cpuStartTime = SGetCPUUserTime();
}

void ResourceMonitorThread::after(){
    const uint64_t threadEndTime = STimeNow() - startTime;
    const double CPUUserTime = SGetCPUUserTime() - cpuStartTime;
    const double cpuUserPercentage = (CPUUserTime / static_cast<double>(threadEndTime)) * 100;
    const pid_t tid = syscall(SYS_gettid);
    SINFO(format("Thread finished. pID: '{}', CPUTime: '{}µs', CPUPercentage: '{}%' , start: {}, cpu start: {}", tid, CPUUserTime, cpuUserPercentage, startTime, cpuStartTime));
}
