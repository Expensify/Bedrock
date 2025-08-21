#pragma once
#include "SQResult.h"
class SQResultFormatter {
public:
  // SQLite supports the following:
  // ascii box csv column html insert json line list markdown quote table tabs tcl
  // We support the following five:
    enum class FORMAT{
        COLUMN,
        CSV,
        TABS,
        JSON,
        QUOTE,
    };

    // Formatting options.
    class FORMAT_OPTIONS {
      public:
        bool header = true;
        string nullvalue;
        string separator = "|";
    };
    static FORMAT_OPTIONS default_options;

    static string format(const SQResult& result, FORMAT format, const FORMAT_OPTIONS& options = default_options);

private:
    static string formatColumn(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatCSV(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatTabs(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatQuote(const SQResult& result, const FORMAT_OPTIONS& options);
    static string formatJSON(const SQResult& result, const FORMAT_OPTIONS& options);
};