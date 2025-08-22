#include "SQResultFormatter.h"
#include <libstuff/libstuff.h>

SQResultFormatter::FORMAT_OPTIONS SQResultFormatter::defaultOptions{};

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
        case FORMAT::LIST:
            return formatList(result, options);
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
    // Match the native format of sqlite3 and handle embedded newlines by
    // splitting cells into physical lines and aligning continuation lines
    // under their respective columns.

    // --- Unicode-aware width helpers ---
    auto utf8Next = [](const string& s, size_t& i) -> uint32_t {
        unsigned char c = static_cast<unsigned char>(s[i++]);
        if (c < 0x80) return c;
        uint32_t cp = 0;
        int extra = 0;
        if ((c & 0xE0) == 0xC0) { cp = c & 0x1F; extra = 1; }
        else if ((c & 0xF0) == 0xE0) { cp = c & 0x0F; extra = 2; }
        else if ((c & 0xF8) == 0xF0) { cp = c & 0x07; extra = 3; }
        else { return 0xFFFD; }
        while (extra-- && i < s.size()) {
            unsigned char t = static_cast<unsigned char>(s[i]);
            if ((t & 0xC0) != 0x80) break;
            cp = (cp << 6) | (t & 0x3F);
            ++i;
        }
        return cp;
    };

    auto isCombining = [](uint32_t cp) -> bool {
        // Common zero-width: combining marks, variation selectors, ZWJ/ZWNJ
        if ((cp >= 0x0300 && cp <= 0x036F) ||
            (cp >= 0x1AB0 && cp <= 0x1AFF) ||
            (cp >= 0x1DC0 && cp <= 0x1DFF) ||
            (cp >= 0x20D0 && cp <= 0x20FF) ||
            (cp >= 0xFE20 && cp <= 0xFE2F) ||
            cp == 0x200D || cp == 0x200C ||
            (cp >= 0xFE00 && cp <= 0xFE0F)) {
            return true;
        }
        return false;
    };

    auto isWide = [](uint32_t cp) -> bool {
        // Treat most CJK and emoji as double-width for terminal-like alignment
        if ((cp >= 0x1100 && cp <= 0x115F) ||
            (cp >= 0x2329 && cp <= 0x232A) ||
            (cp >= 0x2E80 && cp <= 0xA4CF) ||
            (cp >= 0xAC00 && cp <= 0xD7A3) ||
            (cp >= 0xF900 && cp <= 0xFAFF) ||
            (cp >= 0xFE10 && cp <= 0xFE19) ||
            (cp >= 0xFE30 && cp <= 0xFE6F) ||
            (cp >= 0xFF00 && cp <= 0xFF60) ||
            (cp >= 0xFFE0 && cp <= 0xFFE6) ||
            // Emoji blocks
            (cp >= 0x1F300 && cp <= 0x1FAFF) ||
            (cp >= 0x1F900 && cp <= 0x1F9FF) ||
            (cp >= 0x2600 && cp <= 0x26FF) ||
            (cp >= 0x2700 && cp <= 0x27BF)) {
            return true;
        }
        return false;
    };

    auto displayWidth = [&](const string& s) -> size_t {
        size_t w = 0; size_t i = 0;
        while (i < s.size()) {
            uint32_t cp = utf8Next(s, i);
            if (cp == 0) continue; // safety
            if (cp < 0x20 || cp == 0x7F) continue; // control -> width 0
            if (isCombining(cp)) continue;         // zero-width combining
            w += isWide(cp) ? 2 : 1;
        }
        return w;
    };

    auto padToWidth = [&](string& s, size_t target) {
        size_t w = displayWidth(s);
        if (w < target) s.append(target - w, ' ');
    };

    auto expandTabs = [&](const string& s) -> string {
        string out;
        out.reserve(s.size());
        size_t i = 0;
        size_t pos = 0; // visual column within the cell
        while (i < s.size()) {
            size_t before = i;
            uint32_t cp = utf8Next(s, i);
            if (cp == '\t') {
                size_t spaces = 8 - (pos % 8);
                out.append(spaces, ' ');
                pos += spaces;
            } else {
                // append original bytes for this codepoint
                out.append(s, before, i - before);
                if (cp >= 0x20 && cp != 0x7F && !isCombining(cp)) {
                    pos += isWide(cp) ? 2 : 1;
                }
            }
        }
        return out;
    };

    auto splitLines = [](const string& s) -> vector<string> {
        vector<string> lines;
        size_t start = 0;
        for (size_t i = 0; i <= s.size(); ++i) {
            if (i == s.size() || s[i] == '\n') {
                string line = s.substr(start, i - start);
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                lines.push_back(line);
                start = i + 1;
            }
        }
        if (lines.empty()) lines.emplace_back();
        return lines;
    };

    // Determine column widths: maximum subline length across header and all rows
    vector<size_t> maxLengths(result.headers.size());
    for (size_t i = 0; i < result.headers.size(); i++) {
        maxLengths[i] = displayWidth(expandTabs(result.headers[i]));
    }
    for (size_t i = 0; i < result.size(); i++) {
        for (size_t j = 0; j < result[i].size(); j++) {
            const auto parts = splitLines(result[i][j]);
            for (const auto& part : parts) {
                string expanded = expandTabs(part);
                size_t w = displayWidth(expanded);
                if (w > maxLengths[j]) {
                    maxLengths[j] = w;
                }
            }
        }
    }

    // Create the output string
    string output;

    if (options.header) {
        // Build header line in a buffer so we can trim trailing spaces
        string line;
        for (size_t i = 0; i < result.headers.size(); i++) {
            string entry = expandTabs(result.headers[i]);
            if (i + 1 < result.headers.size()) {
                padToWidth(entry, maxLengths[i]);
            }
            if (i != 0) {
                line += "  ";
            }
            line += entry;
        }
        while (!line.empty() && line.back() == ' ') line.pop_back();
        output += line;
        output += "\n";

        // Separator line (also trim just in case)
        line.clear();
        for (size_t i = 0; i < maxLengths.size(); i++) {
            string sep(maxLengths[i], '-');
            if (i != 0) {
                line += "  ";
            }
            line += sep;
        }
        while (!line.empty() && line.back() == ' ') line.pop_back();
        output += line;
        output += "\n";
    }

    // Render each logical row as one or more physical lines based on embedded newlines.
    for (size_t i = 0; i < result.size(); i++) {
        // Pre-split each column into lines and find the tallest cell for this row.
        vector<vector<string>> perColLines(result[i].size());
        size_t maxRowLines = 1;
        for (size_t j = 0; j < result[i].size(); j++) {
            perColLines[j] = splitLines(result[i][j]);
            if (perColLines[j].size() > maxRowLines) {
                maxRowLines = perColLines[j].size();
            }
        }

        for (size_t k = 0; k < maxRowLines; ++k) {
            string line;
            for (size_t j = 0; j < result[i].size(); j++) {
                string entry = (k < perColLines[j].size()) ? perColLines[j][k] : string();
                entry = expandTabs(entry);
                if (j + 1 < result[i].size()) {
                    padToWidth(entry, maxLengths[j]);
                }
                if (j != 0) {
                    line += "  ";
                }
                line += entry;
            }
            while (!line.empty() && line.back() == ' ') line.pop_back();
            output += line;
            output += "\n";
        }
    }

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
    //  - Quote a field if it contains comma, double-quote, CR, LF, any ASCII whitespace/control, or any non-ASCII byte
    //  - Escape embedded double-quotes by doubling them
    //  - Emit "" for the empty string (distinct from SQL NULL, which remains empty between separators)
    //  - Emit a header row if headers exist (matches our TSV/QUOTE behavior)

    auto needsQuoting = [](const string& s) -> bool {
        if (s.empty()) return true; // sqlite shell prints "" for empty strings
        for (unsigned char uc : s) {
            if (uc == ',' || uc == '"' || uc == '\n' || uc == '\r') return true;
            if (uc <= ' ') return true;     // spaces, tabs, other ASCII whitespace/control
            if (uc >= 0x80) return true;    // non-ASCII bytes (e.g., accented chars, emoji bytes)
        }
        return false;
    };

    auto quoteCSV = [&](const string& s) -> string {
        if (s.empty()) {
            return "\"\""; // empty string becomes ""
        }
        if (!needsQuoting(s)) {
            return s;
        }
        string out;
        out.reserve(s.size() + 2);
        out.push_back('"');
        for (char c : s) {
            if (c == '"') {
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

    if (options.header) {
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
    // Mimic sqlite3 shell `.mode tabs`:
    //  - Separator is a single TAB character
    //  - No field quoting/escaping; fields are written verbatim
    //  - Emit a header row if headers exist

    const char delimiter = '\t';
    string output;

    if (options.header) {
        for (size_t i = 0; i < result.headers.size(); ++i) {
            if (i) output.push_back(delimiter);
            output += result.headers[i];
        }
        output.push_back('\n');
    }

    // Data rows
    for (const auto& row : result) {
        // If headers are present, output up to header count; otherwise, all fields.
        size_t cols = result.headers.empty() ? row.size() : result.headers.size();
        // Trim trailing empty fields so we don't print a trailing tab.
        size_t last = cols;
        while (last > 0) {
            const string& f = (last - 1 < row.size()) ? row[last - 1] : string();
            if (!f.empty()) break;
            --last;
        }
        for (size_t j = 0; j < last; ++j) {
            if (j) output.push_back(delimiter);
            const string& field = (j < row.size()) ? row[j] : string();
            output += field;
        }
        output.push_back('\n');
    }

    return output;
}

string SQResultFormatter::formatList(const SQResult& result, const FORMAT_OPTIONS& options) {
    // Mimic sqlite3 shell `.mode list`:
    //  - Columns separated by a pipe ("|")
    //  - Each row on a single line
    //  - No quoting or escaping; fields are written verbatim
    //  - Emit a header row if headers exist (consistent with our other formatters)

    const char delimiter = '|';
    string output;

    if (options.header) {
        for (size_t i = 0; i < result.headers.size(); i++) {
            if (i) {
                output.push_back(delimiter);
            }
            output += result.headers[i];
        }
        output.push_back('\n');
    }

    // Data rows
    for (const auto& row : result) {
        for (size_t i = 0; i < row.size(); i++) {
            if (i) {
                output.push_back(delimiter);
            }
            output += row[i];
        }
        output.push_back('\n');
    }

    return output;
}
