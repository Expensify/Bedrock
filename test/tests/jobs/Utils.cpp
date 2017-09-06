time_t getTimestampForDateTimeString(string datetime) {
    struct tm tm;
    strptime(datetime.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
    return mktime(&tm);
}
