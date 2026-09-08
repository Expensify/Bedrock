/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SQResultFormatter.h
 * Path:    libstuff/SQResultFormatter.h
 * Pair:    SQResultFormatter.cpp
 *
 * INTENT
 *   Renders an SQResult into one of six sqlite3-shell-style text formats
 *   (column, CSV, tabs, JSON, quote, list) for CLI-style display or export.
 *
 * OBJECTS
 *   SQResultFormatter::FORMAT          - enum class of the six supported output formats.
 *   SQResultFormatter::FORMAT_OPTIONS  - nested class bundling header/nullvalue/separator options.
 *   SQResultFormatter                  - static-only class: format() dispatcher plus one
 *     formatColumn/CSV/Tabs/Quote/JSON/List() per FORMAT value.
 *
 * OUT OF PLACE
 *   [CANDIDATE] FORMAT_OPTIONS::nullvalue and FORMAT_OPTIONS::separator are declared but
 *   never read by any formatXXX() implementation in the .cpp - dead configuration surface.
 *
 * NAME/LOCATION FIT
 *   Fits; lives beside SQResult, the type it formats.
 *
 * NAMING QUALITY
 *   FORMAT_OPTIONS is written in the SCREAMING_CASE normally reserved for macros/constants
 *   in this codebase, not the PascalCase used for every other nested class/struct.
 * ─────────────────────────────────────────────────────────────────────*/

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
