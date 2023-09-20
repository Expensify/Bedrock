#include "SQLiteUtils.h"

#include <libstuff/libstuff.h>
#include <libstuff/SRandom.h>
#include <sqlitecluster/SQLite.h>

int64_t SQLiteUtils::getRandomID(const SQLite& db, const string& tableName, const string& column) {
    int64_t newID = 0;
    while (!newID) {
        // Select a random number.
        newID = SRandom::rand64();

        // We've taken an unsigned number, and stuffed it into a signed value, so it could be negative.
        // If it's *the most negative value*, just making it positive doesn't work, cause integer math, and
        // two's-compliment causing it to overflow back to the number you started with. So we disallow that value.
        if (newID == INT64_MIN) {
            newID++;
        }

        // Ok, now we can take the absolute value, and know we have a positive value that fits in our int64_t.
        newID = labs(newID);
        string result = db.read("SELECT " + column + " FROM " + tableName + " WHERE " + column + " = " + to_string(newID) + ";");
        if (!result.empty()) {
            // This one exists! Pick a new one.
            newID = 0;
        }
    }
    return newID;
}
