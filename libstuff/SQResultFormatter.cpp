#include "SQResultFormatter.h"
#include <libstuff/libstuff.h>
string SQResultFormatter::format(const SQResult& result, SQResultFormatter::FORMAT format, const SQResultFormatter::FORMAT_OPTIONS& options) {
    switch (format) {
        case FORMAT::COLUMN:
            return formatColumn(result, options);
        case FORMAT::CSV:
            return formatCSV(result, options);
        case FORMAT::TABS:
            return formatTabs(result, options);
        case FORMAT::JSON:
            return formatJSON(result, options);
        case FORMAT::QUOTE:
            return formatQuote(result, options);
    }
}

string SQResultFormatter::formatJSON(const SQResult& result, const FORMAT_OPTIONS& options) {
    // Just output as a simple object
    // **NOTE: This probably isn't super fast, but could be easily optimized
    //         if it ever became necessary.
    STable output;
    if (options.header) {
        output["headers"] = SComposeJSONArray(result.headers);
    }
    vector<string> jsonRows;
    for (size_t c = 0; c < result.size(); ++c)
        jsonRows.push_back(SComposeJSONArray(result[c]));
    output["rows"] = SComposeJSONArray(jsonRows);
    return SComposeJSONObject(output);
}

string SQResultFormatter::formatColumn(const SQResult& result, const FORMAT_OPTIONS& options) {
    // Match the native format of sqlite3.
    vector<size_t> maxLengths(result.headers.size());
    for (size_t i = 0; i < result.headers.size(); i++) {
        maxLengths[i] = result.headers[i].size();
    }
    for (size_t i = 0; i < result.size(); i++) {
        for (size_t j = 0; j < result[i].size(); j++) {
            if (result[i][j].length() > maxLengths[j]) {
                maxLengths[j] = result[i][j].length();
            }
        }
    }

    // Create the output string
    string output;

    // Append the headers.
    for (size_t i = 0; i < result.headers.size(); i++) {
        string entry = result.headers[i];
        entry.resize(maxLengths[i], ' ');
        if (i != 0) {
            output += "  ";
        }
        output += entry;
    }
    output += "\n";

    // Now create the separator line.
    for (size_t i = 0; i < maxLengths.size(); i++) {
        string sep;
        sep.resize(maxLengths[i], '-');
        if (i != 0) {
            output += "  ";
        }
        output += sep;
    }
    output += "\n";

    // Finally, do each row.
    for (size_t i = 0; i < result.size(); i++) {
        for (size_t j = 0; j < result[i].size(); j++) {
            string entry = result[i][j];
            entry.resize(maxLengths[j], ' ');
            if (j != 0) {
                output += "  ";
            }
            output += entry;
        }
        output += "\n";
    }

    // Done.
    return output;
}

string SQResultFormatter::formatQuote(const SQResult& result, const FORMAT_OPTIONS& options) {
    auto isNumeric = [](const string& s) -> bool {
        if (s.empty()) return false;
        size_t i = 0;
        // optional sign
        if (s[i] == '+' || s[i] == '-') {
            if (++i >= s.size()) return false;
        }
        bool anyDigits = false;
        // integer part
        while (i < s.size() && isdigit(static_cast<unsigned char>(s[i]))) {
            anyDigits = true;
            ++i;
        }
        // decimal part
        if (i < s.size() && s[i] == '.') {
            ++i;
            while (i < s.size() && isdigit(static_cast<unsigned char>(s[i]))) {
                anyDigits = true;
                ++i;
            }
        }
        if (!anyDigits) return false;
        // exponent part
        if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
            ++i;
            if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i;
            bool expDigits = false;
            while (i < s.size() && isdigit(static_cast<unsigned char>(s[i]))) {
                expDigits = true;
                ++i;
            }
            if (!expDigits) return false;
        }
        return i == s.size();
    };

    auto quoteSQL = [](const string& s) -> string {
        string out;
        out.reserve(s.size() + 2);
        out.push_back('\'');
        for (char c : s) {
            if (c == '\'') {
                // SQL escape by doubling the quote
                out.push_back('\'');
                out.push_back('\'');
            } else {
                out.push_back(c);
            }
        }
        out.push_back('\'');
        return out;
    };

    const char delimiter = ','; // sqlite default separator (respects our CSV/TSV pattern)
    string output;

    // Emit headers if present (consistent with our CSV/TSV behavior)
    if (!result.headers.empty()) {
        for (size_t i = 0; i < result.headers.size(); ++i) {
            if (i) output.push_back(delimiter);
            output += quoteSQL(result.headers[i]);
        }
        output.push_back('\n');
    }

    for (const auto& row : result) {
        size_t cols = result.headers.empty() ? row.size() : result.headers.size();
        for (size_t j = 0; j < cols; ++j) {
            if (j) output.push_back(delimiter);
            string field = (j < row.size()) ? row[j] : string();

            // Literal NULL (unquoted) if exactly "NULL"
            if (field == "NULL") {
                output += "NULL";
                continue;
            }

            // Numbers are bare (ASCII text, no quotes)
            if (isNumeric(field)) {
                output += field;
                continue;
            }

            // Everything else is a SQL string literal
            output += quoteSQL(field);
        }
        output.push_back('\n');
    }

    return output;
}

string SQResultFormatter::formatCSV(const SQResult& result, const FORMAT_OPTIONS& options) {
    // Standard CSV + sqlite3 shell defaults:
    //  - Separator: comma
    //  - Quote a field if it contains comma, double-quote, CR, or LF
    //  - Escape embedded double-quotes by doubling them
    //  - Emit a header row if headers exist (matches our TSV/QUOTE behavior)

    auto needsQuoting = [](const string& s) -> bool {
        for (char c : s) {
            if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                return true;
            }
        }
        return false;
    };

    auto quoteCSV = [&](const string& s) -> string {
        if (!needsQuoting(s)) {
            return s;
        }
        string out;
        out.reserve(s.size() + 2);
        out.push_back('"');
        for (char c : s) {
            if (c == '"') {
                // CSV escaping: double the quote
                out.push_back('"');
                out.push_back('"');
            } else {
                out.push_back(c);
            }
        }
        out.push_back('"');
        return out;
    };

    const char delimiter = ',';
    string output;

    // Header row (if present)
    if (!result.headers.empty()) {
        for (size_t i = 0; i < result.headers.size(); ++i) {
            if (i) output.push_back(delimiter);
            output += quoteCSV(result.headers[i]);
        }
        output.push_back('\n');
    }

    // Data rows
    for (const auto& row : result) {
        // If headers are present, cap at header count (sqlite shell does this implicitly
        // as it prints per-column of the statement); otherwise, print all row fields.
        size_t cols = result.headers.empty() ? row.size() : result.headers.size();
        for (size_t j = 0; j < cols; ++j) {
            if (j) output.push_back(delimiter);
            const string& field = (j < row.size()) ? row[j] : string();
            output += quoteCSV(field);
        }
        output.push_back('\n');
    }

    return output;
}

string SQResultFormatter::formatTabs(const SQResult& result, const FORMAT_OPTIONS& options) {
    return "";
}
