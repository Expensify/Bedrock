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
    // This probably isn't super fast, but could be easily optimized if it ever became necessary.
    STable output;
    if (options.header) {
        output["headers"] = SComposeJSONArray(result.headers);
    }
    vector<string> jsonRows;
    for (size_t rowIndex = 0; rowIndex < result.size(); ++rowIndex) {
        jsonRows.push_back(SComposeJSONArray(result[rowIndex]));
    }
    output["rows"] = SComposeJSONArray(jsonRows);
    return SComposeJSONObject(output);
}

string SQResultFormatter::formatColumn(const SQResult& result, const FORMAT_OPTIONS& options) {
    // Match the native format of sqlite3 and handle embedded newlines by
    // splitting cells into physical lines and aligning continuation lines
    // under their respective columns.

    // --- Unicode-aware width helpers ---
    auto utf8Next = [](const string& input, size_t& byteIndex) -> uint32_t {
        unsigned char firstByte = static_cast<unsigned char>(input[byteIndex]);
        byteIndex += 1;

        if (firstByte < 0x80) {
            return firstByte;
        }

        uint32_t codePoint = 0;
        int continuationBytesRemaining = 0;

        if ((firstByte & 0xE0) == 0xC0) {
            codePoint = firstByte & 0x1F;
            continuationBytesRemaining = 1;
        } else if ((firstByte & 0xF0) == 0xE0) {
            codePoint = firstByte & 0x0F;
            continuationBytesRemaining = 2;
        } else if ((firstByte & 0xF8) == 0xF0) {
            codePoint = firstByte & 0x07;
            continuationBytesRemaining = 3;
        } else {
            return 0xFFFD;
        }

        while (continuationBytesRemaining > 0 && byteIndex < input.size()) {
            unsigned char trailByte = static_cast<unsigned char>(input[byteIndex]);
            if ((trailByte & 0xC0) != 0x80) {
                break;
            }
            codePoint = (codePoint << 6) | (trailByte & 0x3F);
            byteIndex += 1;
            continuationBytesRemaining -= 1;
        }

        return codePoint;
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

    auto displayWidth = [&](const string& input) -> size_t {
        size_t displayWidthCount = 0;
        size_t byteIndex = 0;
        while (byteIndex < input.size()) {
            uint32_t codePoint = utf8Next(input, byteIndex);
            if (codePoint == 0) {
                continue; // safety
            }
            if (codePoint < 0x20 || codePoint == 0x7F) {
                continue; // control -> width 0
            }
            if (isCombining(codePoint)) {
                continue; // zero-width combining
            }
            if (isWide(codePoint)) {
                displayWidthCount += 2;
            } else {
                displayWidthCount += 1;
            }
        }
        return displayWidthCount;
    };

    auto padToWidth = [&](string& input, size_t targetWidth) {
        size_t currentWidth = displayWidth(input);
        if (currentWidth < targetWidth) {
            input.append(targetWidth - currentWidth, ' ');
        }
    };

    auto expandTabs = [&](const string& input) -> string {
        string expandedOutput;
        expandedOutput.reserve(input.size());
        size_t byteIndex = 0;
        size_t visualColumn = 0; // visual column within the cell
        while (byteIndex < input.size()) {
            size_t codePointStart = byteIndex;
            uint32_t codePoint = utf8Next(input, byteIndex);
            if (codePoint == '\t') {
                size_t spacesToInsert = 8 - (visualColumn % 8);
                expandedOutput.append(spacesToInsert, ' ');
                visualColumn += spacesToInsert;
            } else {
                expandedOutput.append(input, codePointStart, byteIndex - codePointStart);
                if (codePoint >= 0x20 && codePoint != 0x7F && !isCombining(codePoint)) {
                    if (isWide(codePoint)) {
                        visualColumn += 2;
                    } else {
                        visualColumn += 1;
                    }
                }
            }
        }
        return expandedOutput;
    };

    auto splitLines = [](const string& input) -> vector<string> {
        vector<string> lines;
        size_t lineStart = 0;
        for (size_t i = 0; i <= input.size(); i++) {
            if (i == input.size() || input[i] == '\n') {
                string line = input.substr(lineStart, i - lineStart);
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                lines.push_back(line);
                lineStart = i + 1;
            }
        }
        if (lines.empty()) {
            lines.emplace_back();
        }
        return lines;
    };

    // Determine column widths: maximum subline length across header and all rows
    vector<size_t> maxColumnDisplayWidths(result.headers.size());
    for (size_t i = 0; i < result.headers.size(); i++) {
        maxColumnDisplayWidths[i] = displayWidth(expandTabs(result.headers[i]));
    }
    for (size_t i = 0; i < result.size(); i++) {
        for (size_t j = 0; j < result[i].size(); j++) {
            const auto sublines = splitLines(result[i][j]);
            for (const auto& subline : sublines) {
                string expandedSubline = expandTabs(subline);
                size_t sublineWidth = displayWidth(expandedSubline);
                if (sublineWidth > maxColumnDisplayWidths[j]) {
                    maxColumnDisplayWidths[j] = sublineWidth;
                }
            }
        }
    }

    // Create the output string
    string output;

    if (options.header) {
        // Build header line in a buffer so we can trim trailing spaces
        string headerLine;
        for (size_t i = 0; i < result.headers.size(); i++) {
            string headerCell = expandTabs(result.headers[i]);
            if (i + 1 < result.headers.size()) {
                padToWidth(headerCell, maxColumnDisplayWidths[i]);
            }
            if (i != 0) {
                headerLine += "  ";
            }
            headerLine += headerCell;
        }
        while (!headerLine.empty() && headerLine.back() == ' ') {
            headerLine.pop_back();
        }
        output += headerLine;
        output += "\n";

        // Separator line (also trim just in case)
        headerLine.clear();
        for (size_t i = 0; i < maxColumnDisplayWidths.size(); i++) {
            string separator(maxColumnDisplayWidths[i], '-');
            if (i != 0) {
                headerLine += "  ";
            }
            headerLine += separator;
        }
        while (!headerLine.empty() && headerLine.back() == ' ') {
            headerLine.pop_back();
        }
        output += headerLine;
        output += "\n";
    }

    // Render each logical row as one or more physical lines based on embedded newlines.
    for (size_t i = 0; i < result.size(); i++) {
        // Pre-split each column into lines and find the tallest cell for this row.
        vector<vector<string>> linesPerColumn(result[i].size());
        size_t maxPhysicalLinesInRow = 1;
        for (size_t j = 0; j < result[i].size(); j++) {
            linesPerColumn[j] = splitLines(result[i][j]);
            if (linesPerColumn[j].size() > maxPhysicalLinesInRow) {
                maxPhysicalLinesInRow = linesPerColumn[j].size();
            }
        }

        for (size_t k = 0; k < maxPhysicalLinesInRow; ++k) {
            string renderedLine;
            for (size_t j = 0; j < result[i].size(); j++) {
                string cellText = (k < linesPerColumn[j].size()) ? linesPerColumn[j][k] : string();
                cellText = expandTabs(cellText);
                if (j + 1 < result[i].size()) {
                    padToWidth(cellText, maxColumnDisplayWidths[j]);
                }
                if (j != 0) {
                    renderedLine += "  ";
                }
                renderedLine += cellText;
            }
            while (!renderedLine.empty() && renderedLine.back() == ' ') {
                renderedLine.pop_back();
            }
            output += renderedLine;
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

    if (options.header) {
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
