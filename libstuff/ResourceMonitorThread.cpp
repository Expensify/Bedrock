#include "ResourceMonitorThread.h"
#include "libstuff/libstuff.h"
#include "format"
#include <cmath>

thread_local uint64_t ResourceMonitorThread::startTime;
thread_local double ResourceMonitorThread::cpuStartTime;

void ResourceMonitorThread::before(){
    startTime = STimeNow();
    cpuStartTime = SGetCPUUserTime();
}

void ResourceMonitorThread::after(){
    const uint64_t threadEndTime = STimeNow() - startTime;
    const double CPUUserTime = SGetCPUUserTime() - cpuStartTime;
    const double cpuUserPercentage = round((CPUUserTime / static_cast<double>(threadEndTime)) * 100 * 1000) / 1000;

    // Any thread that takes more than 100ms and uses more than 50% CPU during it's lifetime will be logged. These parameters
    // are completely arbitrary, and might need some fine-tuning in the future based on our future investigations.
    // If we don't add this, we'll log millions log log lines per hour that won't be useful to investigate anything.
    if (CPUUserTime > 100'000 && cpuUserPercentage > 50) {
        const pid_t tid = syscall(SYS_gettid);
        SINFO(format("Thread finished. pID: '{}', CPUTime: '{}µs', CPUPercentage: '{}%'", tid, CPUUserTime, cpuUserPercentage, startTime, cpuStartTime));
    }
}
