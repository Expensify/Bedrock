#include <libstuff/libstuff.h>
#include "JobTestHelper.h"

time_t JobTestHelper::getTimestampForDateTimeString(const string& datetime)
{
    struct tm tm = {0};
    strptime(datetime.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
    return mktime(&tm);
}
