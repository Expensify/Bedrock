#include "libstuff.h"

// --------------------------------------------------------------------------
string SQResult::serializeToJSON() const
{
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

// --------------------------------------------------------------------------
string SQResult::serializeToText() const
{
    // Just output as human readable text
    // **NOTE: This could be prettied up *a lot*
    string output = SComposeList(headers, " | ") + "\n";
    for (size_t c = 0; c < rows.size(); ++c)
        output += SComposeList(rows[c], " | ") + "\n";
    return output;
}

// --------------------------------------------------------------------------
string SQResult::serialize(const string& format) const
{
    // Output the appropriate type
    if (SIEquals(format, "json"))
        return serializeToJSON();
    else
        return serializeToText();
}
