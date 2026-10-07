#include <unistd.h>

#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

struct ConstraintErrorTempDBFile
{
    char filename[17] = "br_con_dbXXXXXXX";
    ConstraintErrorTempDBFile()
    {
        int fd = mkstemp(filename);
        close(fd);
    }

    ~ConstraintErrorTempDBFile()
    {
        unlink(filename);
        unlink((string(filename) + "-pagemap").c_str());
        unlink((string(filename) + "-log-0").c_str());
    }
};

struct ConstraintErrorTest : tpunit::TestFixture
{
    ConstraintErrorTest()
        : tpunit::TestFixture("ConstraintError",
                              TEST(ConstraintErrorTest::defaultDiagnostics),
                              TEST(ConstraintErrorTest::notNull),
                              TEST(ConstraintErrorTest::unique),
                              TEST(ConstraintErrorTest::primaryKey),
                              TEST(ConstraintErrorTest::check),
                              TEST(ConstraintErrorTest::foreignKey))
    {
    }

    void defaultDiagnostics()
    {
        const SQLite::constraint_error error;
        ASSERT_EQUAL(error.getExtendedResultCode(), SQLITE_CONSTRAINT);
        ASSERT_EQUAL(error.getMessage(), "constraint_error");
        ASSERT_EQUAL(string(error.what()), "constraint_error");
    }

    void verifyConstraint(const string& query, int expectedCode, const string& expectedMessage)
    {
        ConstraintErrorTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, 1000, 1, 0, BedrockTester::ENABLE_HCTREE);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "PRAGMA foreign_keys = ON;"), SQLITE_OK);
        ASSERT_TRUE(db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE));
        ASSERT_TRUE(db.write("CREATE TABLE constraintParent(id INTEGER PRIMARY KEY);"));
        ASSERT_TRUE(db.write("CREATE TABLE constraintTest(id INTEGER PRIMARY KEY, required TEXT NOT NULL, "
                             "value TEXT UNIQUE, amount INTEGER CHECK(amount > 0), "
                             "parentID INTEGER REFERENCES constraintParent(id));"));
        ASSERT_TRUE(db.write("INSERT INTO constraintTest VALUES(1, 'required', 'unique', 1, NULL);"));
        const string journalBefore = db.getUncommittedQuery();

        bool caught = false;
        try {
            db.write(query);
        } catch (const SQLite::constraint_error& error) {
            caught = true;
            ASSERT_EQUAL(db.getUncommittedQuery(), journalBefore);

            // Connection error state can change during rollback or a later query; the exception owns its diagnostics.
            db.rollback();
            ASSERT_EQUAL(db.read("SELECT 1;"), "1");
            ASSERT_EQUAL(error.getExtendedResultCode(), expectedCode);
            ASSERT_EQUAL(error.getMessage(), expectedMessage);
            ASSERT_EQUAL(string(error.what()), "constraint_error");
        }
        ASSERT_TRUE(caught);
    }

    void notNull()
    {
        verifyConstraint("INSERT INTO constraintTest VALUES(2, NULL, 'other', 1, NULL);",
                         SQLITE_CONSTRAINT_NOTNULL, "NOT NULL constraint failed: constraintTest.required");
    }

    void unique()
    {
        verifyConstraint("INSERT INTO constraintTest VALUES(2, 'required', 'unique', 1, NULL);",
                         SQLITE_CONSTRAINT_UNIQUE, "UNIQUE constraint failed: constraintTest.value");
    }

    void primaryKey()
    {
        verifyConstraint("INSERT INTO constraintTest VALUES(1, 'required', 'other', 1, NULL);",
                         SQLITE_CONSTRAINT_PRIMARYKEY, "UNIQUE constraint failed: constraintTest.id");
    }

    void check()
    {
        verifyConstraint("INSERT INTO constraintTest VALUES(2, 'required', 'other', 0, NULL);",
                         SQLITE_CONSTRAINT_CHECK, "CHECK constraint failed: amount > 0");
    }

    void foreignKey()
    {
        verifyConstraint("INSERT INTO constraintTest VALUES(2, 'required', 'other', 1, 99);",
                         SQLITE_CONSTRAINT_FOREIGNKEY, "FOREIGN KEY constraint failed");
    }
} __ConstraintErrorTest;
