#pragma once

#include <libstuff/SQResult.h>

namespace JournalTestHelper {
struct LegacyHistory
{
    uint64_t lastCommitID;
    string lastHash;
    SQResult entries;
};

// Seed an offline database using legacy journaling, optionally leaving a blank final entry for migration.
LegacyHistory seedLegacyHistory(const string& filename, bool hctree, const string& table, int rows, bool blankTail = false);
}
