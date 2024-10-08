#include "ResourceMonitorThread.h"
#include "libstuff/libstuff.h"
#include <format>
#include <cmath>

thread_local uint64_t ResourceMonitorThread::threadStartTime;
thread_local double ResourceMonitorThread::cpuStartTime;

void ResourceMonitorThread::beforeProcessStart() {
    threadStartTime = STimeNow();
    cpuStartTime = SGetCPUUserTime();
}

void ResourceMonitorThread::afterProcessFinished() {
    const uint64_t threadUserTime = STimeNow() - threadStartTime;
    const double cpuUserTime = SGetCPUUserTime() - cpuStartTime;

    // This shouldn't happen since the time to start/finish a thread should take more than a microsecond, but to be 
    // sure we're not dividing by 0 and causing crashes, let's add an if here and return if threadEndTime is 0.
    if (threadUserTime == 0) {
        return;
    }
    const double cpuUserPercentage = round((cpuUserTime / static_cast<double>(threadUserTime)) * 100 * 1000) / 1000;
    const pid_t tid = syscall(SYS_gettid);
    SINFO(format("Thread finished. pID: '{}', CPUTime: '{}µs', CPUPercentage: '{}%'", tid, cpuUserTime, cpuUserPercentage));
}
