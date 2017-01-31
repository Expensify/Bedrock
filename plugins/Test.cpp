#include "Test.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{" << node->name << ":" << getName() << "} "

#define JOBS_DEFAULT_PRIORITY 500

void BedrockPlugin_Test::upgradeDatabase(BedrockNode* node, SQLite& db) {
    // Create or verify the jobs table
    bool ignore;
    SASSERT(db.verifyTable("test", "CREATE TABLE test ( "
                                   "id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                                   "value TEXT NOT NULL )",
                           ignore));
}

