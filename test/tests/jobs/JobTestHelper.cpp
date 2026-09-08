/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    JobTestHelper.cpp
 * Path:    test/tests/jobs/JobTestHelper.cpp
 * Pair:    JobTestHelper.h
 *
 * INTENT
 *   Implements JobTestHelper; see the header.
 *
 * OBJECTS
 *   No file-local additions beyond the header's declared API.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with the header.
 * ─────────────────────────────────────────────────────────────────────*/

#include <libstuff/libstuff.h>
#include "JobTestHelper.h"

time_t JobTestHelper::getTimestampForDateTimeString(const string& datetime)
{
    struct tm tm = {0};
    strptime(datetime.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
    return mktime(&tm);
}
