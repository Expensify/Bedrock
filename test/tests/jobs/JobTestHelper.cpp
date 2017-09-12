#include <libstuff/libstuff.h>
#include "JobTestHelper.h"

time_t JobTestHelper::getTimestampForDateTimeString(string datetime) {
    struct tm tm;
    strptime(datetime.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
    return mktime(&tm);
}
