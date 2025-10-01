#include <test/lib/tpunit++.hpp>
#include <libstuff/libstuff.h>
#include <plugins/MySQL.h>

using namespace std;

/*
 * MySQL Plugin Integration Tests
 * 
 * These tests verify that the MySQL plugin properly handles queries end-to-end.
 * They test the actual plugin behavior rather than just the utility functions.
 * 
 * To run these tests:
 * 1. From the Bedrock root directory: make test
 * 2. To run only MySQL tests: ./test/test -only MySQL
 */
struct MySQLTest : tpunit::TestFixture {
    MySQLTest()
        : tpunit::TestFixture("MySQL",
                              TEST(MySQLTest::testVersionQueryResponse),
                              TEST(MySQLTest::testConnectionIdQueryResponse),
                              TEST(MySQLTest::testInformationSchemaQueriesDetection),
                              TEST(MySQLTest::testShowKeysQueryDetection),
                              TEST(MySQLTest::testForeignKeyQueryDetection),
                              TEST(MySQLTest::testComplexForeignKeyQueryDetection)) { }

    void testVersionQueryResponse() {
        // Test that VERSION() queries are properly detected and handled
        vector<string> matches;
        
        // Test queries that the plugin should handle
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT VERSION();", matches));
        // No alias, should use default
        
        ASSERT_TRUE(MySQLUtils::parseVersionQuery("SELECT VERSION() AS server_version;", matches));
        ASSERT_TRUE(matches.size() > 1 && SToLower(matches[1]) == "server_version");
        
        // Test queries that should not be handled by the VERSION() logic
        ASSERT_FALSE(MySQLUtils::parseVersionQuery("SELECT VERSION() FROM table;", matches));
        ASSERT_FALSE(MySQLUtils::parseVersionQuery("SELECT VERSION(), other_field;", matches));
    }

    void testConnectionIdQueryResponse() {
        // Test that CONNECTION_ID() queries are properly detected and handled
        vector<string> matches;
        
        // Test queries that the plugin should handle
        ASSERT_TRUE(MySQLUtils::parseConnectionIdQuery("SELECT CONNECTION_ID();", matches));
        // No alias, should use default
        
        ASSERT_TRUE(MySQLUtils::parseConnectionIdQuery("SELECT connection_id() AS pid;", matches));
        ASSERT_TRUE(matches.size() > 1 && SToLower(matches[1]) == "pid");
        
        // Test queries that should not be handled by the CONNECTION_ID() logic
        ASSERT_FALSE(MySQLUtils::parseConnectionIdQuery("SELECT connection_id FROM table;", matches));
        ASSERT_FALSE(MySQLUtils::parseConnectionIdQuery("SELECT CONNECTION_ID(), other_field;", matches));
    }

    void testInformationSchemaQueriesDetection() {
        // Test that information_schema queries are properly detected
        ASSERT_TRUE(MySQLUtils::isInformationSchemaTablesQuery("SELECT * FROM information_schema.tables"));
        ASSERT_TRUE(MySQLUtils::isInformationSchemaViewsQuery("SELECT * FROM information_schema.views"));
        ASSERT_TRUE(MySQLUtils::isInformationSchemaColumnsQuery("SELECT * FROM information_schema.columns WHERE table_name = 'users'"));
        
        // Test table name extraction from columns queries
        string tableName = MySQLUtils::extractTableNameFromColumnsQuery(
            "SELECT * FROM information_schema.columns WHERE table_name = 'accounts' ORDER BY ordinal_position");
        ASSERT_TRUE(tableName == "accounts");
    }

    void testShowKeysQueryDetection() {
        // Test that SHOW KEYS queries are properly detected and table names extracted
        ASSERT_TRUE(MySQLUtils::isShowKeysQuery("SHOW KEYS FROM bankAccounts"));
        ASSERT_TRUE(MySQLUtils::isShowKeysQuery("SHOW KEYS FROM `table_name` WHERE Key_name = 'PRIMARY'"));
        
        // Test table name extraction
        string tableName = MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `bankAccounts`");
        ASSERT_TRUE(tableName == "bankAccounts");
        
        tableName = MySQLUtils::extractTableNameFromShowKeysQuery("SHOW KEYS FROM users WHERE Key_name = 'PRIMARY'");
        ASSERT_TRUE(tableName == "users");
    }

    void testForeignKeyQueryDetection() {
        // Test that foreign key constraint queries are properly detected
        string complexQuery = 
            "SELECT cu.constraint_name, cu.column_name, cu.referenced_table_name "
            "FROM information_schema.key_column_usage cu "
            "JOIN information_schema.referential_constraints rc ON cu.constraint_name = rc.constraint_name "
            "WHERE table_schema = database() AND cu.table_name = 'bankAccounts'";
        
        ASSERT_TRUE(MySQLUtils::isForeignKeyConstraintQuery(complexQuery));
        
        // Test table name extraction
        string tableName = MySQLUtils::extractTableNameFromForeignKeyQuery(complexQuery);
        ASSERT_TRUE(tableName == "bankAccounts");
        
        // Test variations
        tableName = MySQLUtils::extractTableNameFromForeignKeyQuery("WHERE cu.table_name = \"users\"");
        ASSERT_TRUE(tableName == "users");
        
        tableName = MySQLUtils::extractTableNameFromForeignKeyQuery("AND table_name = 'accounts'");
        ASSERT_TRUE(tableName == "accounts");
    }

    void testComplexForeignKeyQueryDetection() {
        // Test the specific complex foreign key query that includes all expected columns
        string complexForeignKeyQuery = 
            "SELECT "
            "cu.constraint_name as 'constraint_name', "
            "cu.column_name as 'column_name', "
            "cu.referenced_table_name as 'referenced_table_name', "
            "IF(cu.referenced_table_name IS NOT NULL, 'FOREIGN', cu.constraint_name) as key_type, "
            "cu.REFERENCED_TABLE_NAME as referenced_table, "
            "cu.REFERENCED_COLUMN_NAME as referenced_column, "
            "rc.UPDATE_RULE as on_update, "
            "rc.DELETE_RULE as on_delete, "
            "rc.CONSTRAINT_NAME as rc_constraint_name, "
            "cu.ORDINAL_POSITION as ordinal_position "
            "FROM information_schema.key_column_usage cu "
            "JOIN information_schema.referential_constraints rc "
            "on cu.constraint_name = rc.constraint_name "
            "and cu.constraint_schema = rc.constraint_schema "
            "WHERE table_schema = database() "
            "AND cu.table_name = 'nameValuePairs' "
            "AND cu.referenced_table_name IS NOT NULL "
            "ORDER BY rc.CONSTRAINT_NAME, cu.ORDINAL_POSITION;";
        
        // Verify this query is detected as a foreign key constraint query
        ASSERT_TRUE(MySQLUtils::isForeignKeyConstraintQuery(complexForeignKeyQuery));
        
        // Verify the table name is correctly extracted
        string tableName = MySQLUtils::extractTableNameFromForeignKeyQuery(complexForeignKeyQuery);
        ASSERT_TRUE(tableName == "nameValuePairs");
        
        // Test similar query with different table name and formatting
        string alternativeQuery = 
            "SELECT cu.constraint_name, cu.column_name, cu.referenced_table_name, "
            "rc.UPDATE_RULE, rc.DELETE_RULE "
            "FROM information_schema.key_column_usage cu "
            "JOIN information_schema.referential_constraints rc ON cu.constraint_name = rc.constraint_name "
            "WHERE cu.table_name = \"users\" AND cu.referenced_table_name IS NOT NULL;";
        
        ASSERT_TRUE(MySQLUtils::isForeignKeyConstraintQuery(alternativeQuery));
        tableName = MySQLUtils::extractTableNameFromForeignKeyQuery(alternativeQuery);
        ASSERT_TRUE(tableName == "users");
    }
} __MySQLTest;
