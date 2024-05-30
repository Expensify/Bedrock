#include "libstuff.h"

#include <ctime>
#include <sys/time.h>

uint64_t STimeNow() {
    // Get the time = microseconds since 00:00:00 UTC, January 1, 1970
    timeval time;
    gettimeofday(&time, 0);
    return ((uint64_t)time.tv_sec * 1000000 + (uint64_t)time.tv_usec);
}

string SComposeTime(const string& format, uint64_t when) {
    // Convert from high-precision time (usec) to standard low-precision time (sec), then format and return
    const time_t loWhen = (time_t)(when / STIME_US_PER_S);
    char buf[256] = {};
    struct tm result = {};
    gmtime_r(&loWhen, &result);
    size_t length = strftime(buf, sizeof(buf), format.c_str(), &result);
    return string(buf, length);
}

int SDaysInMonth(int year, int month) {
    // 30 days hath September...
    if (month == 4 || month == 6 || month == 9 || month == 11) {
        return 30;
    } else if (month == 2) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) ? 29 : 28;
    } else {
        return 31;
    }
}

uint64_t STimeThisMorning() {
    // Get today's date in GMT, zero out the hour, convert into a Unix
    // timestamp, and then into a libstuff timestamp.
    time_t loNow;
    time(&loNow);
    uint64_t hiNow = STimeNow();
    struct tm gmt = *gmtime(&loNow);
    gmt.tm_hour = 0;
    gmt.tm_min = 0;
    gmt.tm_sec = 0;
    time_t gmtTime = timegm(&gmt);
    int64_t hiLoDelta = hiNow - loNow * STIME_US_PER_S;
    return gmtTime * STIME_US_PER_S + hiLoDelta;
}

timeval SToTimeval(uint64_t when) {
    // Just split by high and low bits
    return {(time_t)(when / STIME_US_PER_S), (suseconds_t)(when % STIME_US_PER_S)};
}

string SCURRENT_TIMESTAMP_MS() {
    return STIMESTAMP_MS(STimeNow());
}

string SFirstOfMonth(const string& timeStamp, const int64_t& offset) {

    list<string> parts = SParseList(timeStamp, '-');

    // Initialize to all 0's
    struct tm t = {0};  
    int64_t year;

    try {
        // This is year - 1900
        year = stoull(parts.front(), 0, 10) - 1900;
    } catch (const invalid_argument& e) {
        STHROW("500 Error parsing year");
    } catch (const out_of_range& e) {
        STHROW("500 Error parsing year");
    }

    // Pop the year off
    parts.pop_front();

    try {
        int64_t month = stoull(parts.front(), 0, 10) - 1;
        int64_t yearInMonths = year * 12 + month;
        yearInMonths += offset;
        t.tm_year = yearInMonths / 12;
        t.tm_mon = yearInMonths % 12;
    } catch (const invalid_argument& e) {
        STHROW("500 Error parsing month");
    } catch (const out_of_range& e) {
        STHROW("500 Error parsing month");
    }

    t.tm_mday = 1;
    
    char buf[256] = {};
    size_t length = strftime(buf, sizeof(buf), "%Y-%m-%d", &t);
    return string(buf, length);
}
