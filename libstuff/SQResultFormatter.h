#pragma once
#include "SQResult.h"
class SQResultFormatter {
public:
    // SQLite supports the following:
    // ascii box csv column html insert json line list markdown quote table tabs tcl
    // We support the following six:
    enum class FORMAT
    {
        COLUMN,
        CSV,
        TABS,
        JSON,
        QUOTE,
        LIST,
    };

    // Formatting options.
    class FORMAT_OPTIONS {
public:
        bool header = true;
        string nullvalue;
        string separator = "|";
    };
    static FORMAT_OPTIONS defaultOptions;

    static string format(const SQResult& result, FORMAT format, const FORMAT_OPTIONS& options = defaultOptions);

private:
    static string formatColumn(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatCSV(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatTabs(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatQuote(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatJSON(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatList(const SQResult& result, const FORMAT_OPTIONS& options);
};
