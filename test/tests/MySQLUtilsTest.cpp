#include <test/lib/tpunit++.hpp>
#include <libstuff/libstuff.h>
#include <plugins/MySQLUtils.h>

using namespace std;

/*
 * MySQL Utils Unit Tests
 * 
 * These tests focus on the query parsing and transformation logic
 * in the MySQLUtils namespace, which is used by the MySQL plugin for handling:
 * - VERSION() function calls with aliases
 * - connection_id() function calls with aliases  
 * - information_schema table/view/column queries
 * - Table name extraction from complex SQL queries
 * 
 * To run these tests:
 * 1. From the Bedrock root directory: make test
 * 2. To run only MySQL utils tests: ./test/test -only MySQLUtils
 */

struct MySQLUtilsTest : tpunit::TestFixture {
    MySQLUtilsTest()
        : tpunit::TestFixture("MySQLUtils",
                              TEST(MySQLUtilsTest::testVersionQueryPatternMatching),
                              TEST(MySQLUtilsTest::testVersionQueryAliasExtraction),
                              TEST(MySQLUtilsTest::testConnectionIdQueryPatternMatching),
                              TEST(MySQLUtilsTest::testConnectionIdQueryAliasExtraction),
                              TEST(MySQLUtilsTest::testTableNameExtractionFromColumnsQuery),
                              TEST(MySQLUtilsTest::testTableNameExtractionEdgeCases),
                              TEST(MySQLUtilsTest::testInformationSchemaQueryDetection),
                              TEST(MySQLUtilsTest::testShowKeysQueryDetection),
                              TEST(MySQLUtilsTest::testShowKeysTableNameExtraction),
                              TEST(MySQLUtilsTest::testForeignKeyConstraintQueryDetection),
                              TEST(MySQLUtilsTest::testForeignKeyConstraintTableNameExtraction),
                              TEST(MySQLUtilsTest::testQueryPatternEdgeCases)) { }

    void testVersionQueryPatternMatching() {
        vector<string> matches;
        
        // Test basic VERSION() query
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT VERSION();", matches));
        
        // Test with different whitespace
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT  VERSION(  );", matches));
        
        // Test case insensitive
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("select version();", matches));
        
        // Test without semicolon
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT VERSION()", matches));
    }

    void testVersionQueryAliasExtraction() {
        vector<string> matches;
        
        // Test with AS alias
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT VERSION() AS v;", matches));
        ASSERT_TRUE(matches.size() > 1 && SToLower(matches[1]) == "v");
        
        // Test with AS alias different case
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT VERSION() as MyVersion;", matches));
        ASSERT_TRUE(matches.size() > 1 && SToLower(matches[1]) == "myversion");
        
        // Test with AS alias and extra whitespace
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT VERSION()  AS   serverVersion  ;", matches));
        ASSERT_TRUE(matches.size() > 1 && SToLower(matches[1]) == "serverversion");
        
        // Test invalid patterns should not match
        ASSERT_FALSE(MySQLUtils::parseVersionQuery("SELECT VERSION() FROM table;", matches));
        ASSERT_FALSE(MySQLUtils::parseVersionQuery("UPDATE VERSION();", matches));
    }

    void testConnectionIdQueryPatternMatching() {
        vector<string> matches;
        
        // Test basic connection_id() query
        ASSERT_TRUE(MySQLUtils::parseConnectionIdQuery("SELECT connection_id();", matches));
        
        // Test case insensitive
        ASSERT_TRUE(MySQLUtils::parseConnectionIdQuery("SELECT CONNECTION_ID();", matches));
        
        // Test with whitespace
        ASSERT_TRUE(MySQLUtils::parseConnectionIdQuery("SELECT  connection_id(  );", matches));
    }

    void testConnectionIdQueryAliasExtraction() {
        vector<string> matches;
        
        // Test with AS alias
        ASSERT_TRUE(MySQLUtils::parseConnectionIdQuery("SELECT connection_id() AS pid;", matches));
        ASSERT_TRUE(matches.size() > 1 && SToLower(matches[1]) == "pid");
        
        // Test with AS alias different case
        ASSERT_TRUE(MySQLUtils::parseConnectionIdQuery("SELECT CONNECTION_ID() as ConnID;", matches));
        ASSERT_TRUE(matches.size() > 1 && SToLower(matches[1]) == "connid");
        
        // Test invalid patterns should not match
        ASSERT_FALSE(MySQLUtils::parseConnectionIdQuery("SELECT connection_id FROM table;", matches));
        ASSERT_FALSE(MySQLUtils::parseConnectionIdQuery("INSERT connection_id();", matches));
    }

    void testTableNameExtractionFromColumnsQuery() {
        // Test basic table name extraction
        string query1 = "SELECT * FROM information_schema.columns WHERE table_name = 'accounts';";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query1) == "accounts");
        
        // Test with double quotes
        string query2 = "SELECT * FROM information_schema.columns WHERE table_name = \"users\";";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query2) == "users");
        
        // Test with whitespace around equals
        string query3 = "SELECT * FROM information_schema.columns WHERE table_name  =  'testTable';";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query3) == "testTable");
        
        // Test case preservation
        string query4 = "SELECT * FROM information_schema.columns WHERE table_name = 'CamelCaseTable';";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query4) == "CamelCaseTable");
    }

    void testTableNameExtractionEdgeCases() {
        // Test missing table_name
        string query1 = "SELECT * FROM information_schema.columns WHERE other_field = 'value';";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query1).empty());
        
        // Test malformed query (no closing quote)
        string query2 = "SELECT * FROM information_schema.columns WHERE table_name = 'accounts;";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query2).empty());
        
        // Test empty table name
        string query3 = "SELECT * FROM information_schema.columns WHERE table_name = '';";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query3).empty());
        
        // Test complex query with multiple conditions
        string query4 = "SELECT * FROM information_schema.columns WHERE table_schema = database() AND table_name = 'accounts' ORDER BY ordinal_position;";
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(query4) == "accounts");
    }

    void testInformationSchemaQueryDetection() {
        // Test tables query detection
        ASSERT_TRUE(MySQLUtils::isInformationSchemaTablesQuery("SELECT * FROM information_schema.tables"));
        ASSERT_TRUE(MySQLUtils::isInformationSchemaTablesQuery("select * from INFORMATION_SCHEMA.TABLES"));
        ASSERT_FALSE(MySQLUtils::isInformationSchemaTablesQuery("SELECT * FROM information_schema.views"));
        
        // Test views query detection
        ASSERT_TRUE(MySQLUtils::isInformationSchemaViewsQuery("SELECT * FROM information_schema.views"));
        ASSERT_TRUE(MySQLUtils::isInformationSchemaViewsQuery("select * from INFORMATION_SCHEMA.VIEWS"));
        ASSERT_FALSE(MySQLUtils::isInformationSchemaViewsQuery("SELECT * FROM information_schema.tables"));
        
        // Test columns query detection
        ASSERT_TRUE(MySQLUtils::isInformationSchemaColumnsQuery("SELECT * FROM information_schema.columns"));
        ASSERT_TRUE(MySQLUtils::isInformationSchemaColumnsQuery("select * from INFORMATION_SCHEMA.COLUMNS"));
        ASSERT_FALSE(MySQLUtils::isInformationSchemaColumnsQuery("SELECT * FROM information_schema.routines"));
    }

    void testQueryPatternEdgeCases() {
        vector<string> matches;
        
        // Test queries that should NOT match VERSION pattern
        ASSERT_FALSE(MySQLUtils::parseVersionQuery("SELECT VERSION() + 1;", matches));
        ASSERT_FALSE(MySQLUtils::parseVersionQuery("SELECT VERSION(), other_field;", matches));
        ASSERT_FALSE(MySQLUtils::parseVersionQuery("SELECT * FROM VERSION();", matches));
        
        // Test queries that should NOT match connection_id pattern  
        ASSERT_FALSE(MySQLUtils::parseConnectionIdQuery("SELECT connection_id() + 1;", matches));
        ASSERT_FALSE(MySQLUtils::parseConnectionIdQuery("SELECT connection_id(), other_field;", matches));
        ASSERT_FALSE(MySQLUtils::parseConnectionIdQuery("SELECT * FROM connection_id();", matches));
        
        // Test edge cases for table name extraction
        string badQuery1 = "WHERE table_name = 'accounts' AND table_name = 'users'";  // Multiple matches
        string result1 = MySQLUtils::extractTableNameFromColumnsQuery(badQuery1);
        ASSERT_TRUE(result1 == "accounts");  // Should get first match
        
        string badQuery2 = "SELECT table_name FROM tables";  // No equals sign
        ASSERT_TRUE(MySQLUtils::extractTableNameFromColumnsQuery(badQuery2).empty());
    }

    void testShowKeysQueryDetection() {
        // Test SHOW KEYS query detection
        ASSERT_TRUE(MySQLUtils::isShowKeysQuery("SHOW KEYS FROM bankAccounts"));
        ASSERT_TRUE(MySQLUtils::isShowKeysQuery("show keys from users"));
        ASSERT_TRUE(MySQLUtils::isShowKeysQuery("SHOW KEYS FROM `table_name`"));
        ASSERT_TRUE(MySQLUtils::isShowKeysQuery("SHOW KEYS FROM bankAccounts WHERE Key_name = 'PRIMARY'"));
        
        // Test queries that should NOT match
        ASSERT_FALSE(MySQLUtils::isShowKeysQuery("SHOW TABLES"));
        ASSERT_FALSE(MySQLUtils::isShowKeysQuery("SELECT * FROM bankAccounts"));
        ASSERT_FALSE(MySQLUtils::isShowKeysQuery("SHOW COLUMNS FROM bankAccounts"));
    }

    void testShowKeysTableNameExtraction() {
        // Test basic table name extraction
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM bankAccounts") == "bankAccounts");
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM users") == "users");
        
        // Test backtick-quoted table names
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `bankAccounts`") == "bankAccounts");
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `table-name`") == "table-name");
        
        // Test with WHERE clause
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `bankAccounts` WHERE Key_name = 'PRIMARY'") == "bankAccounts");
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM users WHERE Key_name = 'PRIMARY'") == "users");
        
        // Test with extra whitespace
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM   bankAccounts   ") == "bankAccounts");
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM\t`table_name`\t") == "table_name");
        
        // Test edge cases
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS").empty()); // No FROM
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM").empty()); // No table name
        ASSERT_TRUE(MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `").empty()); // Malformed backticks
    }

    void testForeignKeyConstraintQueryDetection() {
        // Test foreign key constraint query detection
        string complexQuery = 
            "SELECT cu.constraint_name as 'constraint_name', "
            "cu.column_name as 'column_name', "
            "cu.referenced_table_name as 'referenced_table_name' "
            "FROM information_schema.key_column_usage cu "
            "JOIN information_schema.referential_constraints rc "
            "on cu.constraint_name = rc.constraint_name "
            "WHERE table_schema = database() "
            "AND cu.table_name = 'bankAccounts'";
        
        ASSERT_TRUE(MySQLUtils::isForeignKeyConstraintQuery(complexQuery));
        
        // Test variations
        ASSERT_TRUE(MySQLUtils::isForeignKeyConstraintQuery("SELECT * FROM information_schema.key_column_usage JOIN information_schema.referential_constraints"));
        ASSERT_TRUE(MySQLUtils::isForeignKeyConstraintQuery("select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"));
        
        // Test queries that should NOT match
        ASSERT_FALSE(MySQLUtils::isForeignKeyConstraintQuery("SELECT * FROM information_schema.key_column_usage"));  // Missing referential_constraints
        ASSERT_FALSE(MySQLUtils::isForeignKeyConstraintQuery("SELECT * FROM information_schema.referential_constraints"));  // Missing key_column_usage
        ASSERT_FALSE(MySQLUtils::isForeignKeyConstraintQuery("SELECT * FROM information_schema.tables"));
        ASSERT_FALSE(MySQLUtils::isForeignKeyConstraintQuery("SELECT * FROM bankAccounts"));
    }

    void testForeignKeyConstraintTableNameExtraction() {
        // Test table name extraction from complex foreign key queries
        string complexQuery = 
            "SELECT cu.constraint_name as 'constraint_name', "
            "cu.column_name as 'column_name', "
            "cu.referenced_table_name as 'referenced_table_name' "
            "FROM information_schema.key_column_usage cu "
            "JOIN information_schema.referential_constraints rc "
            "WHERE table_schema = database() "
            "AND cu.table_name = 'bankAccounts' "
            "ORDER BY cu.ordinal_position";
        
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery(complexQuery) == "bankAccounts");
        
        // Test with different quote styles and table alias prefixes
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("WHERE cu.table_name = \"users\"") == "users");
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("AND table_name = 'accounts'") == "accounts");
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("WHERE alias.table_name = 'test'") == "test");
        
        // Test with whitespace variations
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("WHERE table_name  =  'testTable'") == "testTable");
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("AND\ttable_name\t=\t\"my_table\"") == "my_table");
        
        // Test case insensitive matching
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("WHERE TABLE_NAME = 'CamelCase'") == "CamelCase");
        
        // Test edge cases
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("SELECT * FROM tables").empty()); // No table_name pattern
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("WHERE other_field = 'value'").empty()); // Wrong field
        ASSERT_TRUE(MySQLUtils::extractTableNameFromForeignKeyQuery("WHERE table_name =").empty()); // No value
    }
} __MySQLUtilsTest;
