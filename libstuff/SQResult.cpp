#include <libstuff/libstuff.h>
#include "SQResult.h"

string SQResult::serializeToJSON() const {
    // Just output as a simple object
    // **NOTE: This probably isn't super fast, but could be easily optimized
    //         if it ever became necessary.
    STable output;
    output["headers"] = SComposeJSONArray(headers);
    vector<string> jsonRows;
    for (size_t c = 0; c < rows.size(); ++c)
        jsonRows.push_back(SComposeJSONArray(rows[c]));
    output["rows"] = SComposeJSONArray(jsonRows);
    return SComposeJSONObject(output);
}

string SQResult::serializeToText() const {
    // Just output as human readable text
    // **NOTE: This could be prettied up *a lot*
    string output = SComposeList(headers, " | ") + "\n";
    for (size_t c = 0; c < rows.size(); ++c)
        output += SComposeList(rows[c], " | ") + "\n";
    return output;
}

string SQResult::serialize(const string& format) const {
    // Output the appropriate type
    if (SIEquals(format, "json"))
        return serializeToJSON();
    else
        return serializeToText();
}

bool SQResult::deserialize(const string& json) {
    // Reset ourselves to start
    clear();

    // If there are any problems, clean up whatever we've parsed
    try {
        // Verify we have the basic components
        STable content = SParseJSONObject(json);
        if (!SContains(content, "headers")) {
            STHROW("Missing 'headers'");
        }
        if (!SContains(content, "rows")) {
            STHROW("Missing 'rows'");
        }

        // Add the headers
        list<string> jsonHeaders = SParseJSONArray(content["headers"]);
        headers.insert(headers.end(), jsonHeaders.begin(), jsonHeaders.end());

        // Add the rows
        list<string> jsonRows = SParseJSONArray(content["rows"]);
        rows.resize(jsonRows.size());
        int rowIndex = 0;
        for (string& jsonRowStr : jsonRows) {
            // Get the row and make sure it has the right number of columns
            list<string> jsonRow = SParseJSONArray(jsonRowStr);
            if (jsonRow.size() != headers.size()) {
                STHROW("Incorrect number of columns in row");
            }

            // Insert the values
            vector<string>& row = rows[rowIndex++];
            row.insert(row.end(), jsonRow.begin(), jsonRow.end());
        }

        // Success!
        return true;
    } catch (const SException& e) {
        SDEBUG("Failed to deserialize JSON-encoded SQResult (" << e.what() << "): " << json);
    }

    // Failed, reset and report failure
    clear();
    return false;
}

bool SQResult::empty() const {
    return rows.empty();
}

size_t SQResult::size() const {
    return rows.size();
}

void SQResult::clear() {
    headers.clear();
    rows.clear();
}

vector<string>& SQResult::operator[](size_t rowNum) {
    try {
        return rows.at(rowNum);
    } catch (const out_of_range& e) {
        STHROW("Out of range");
    }
}

const vector<string>& SQResult::operator[](size_t rowNum) const {
    try {
        return rows.at(rowNum);
    } catch (const out_of_range& e) {
        STHROW("Out of range");
    }
}
