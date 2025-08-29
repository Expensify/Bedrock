#include <test/lib/tpunit++.hpp>
#include <libstuff/libstuff.h>

using namespace std;

/*
 * MySQL Plugin Unit Tests
 * 
 * These tests focus on the query parsing and transformation logic
 * added to the MySQL plugin for handling:
 * - VERSION() function calls with aliases
 * - connection_id() function calls with aliases  
 * - information_schema table/view/column queries
 * - Table name extraction from complex SQL queries
 * 
 * To run these tests:
 * 1. From the Bedrock root directory: make test
 * 2. To run only MySQL tests: ./test/test -only MySQL
 */

// Helper functions to test MySQL query parsing logic
namespace MySQLTestHelpers {
    
    // Test VERSION() query pattern matching (mirrors actual MySQL.cpp implementation)
    bool matchesVersionQuery(const string& query, string& extractedAlias) {
        string upperQuery = SToUpper(query);
        // First check if pattern matches at all
        if (SREMatch("^SELECT\\s+VERSION\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, nullptr)) {
            // Default column name
            extractedAlias = "version()";
            
            // Check if there's an alias captured
            vector<string> matches;
            if (SREMatch("^SELECT\\s+VERSION\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, &matches) && matches.size() > 1) {
                extractedAlias = SToLower(matches[1]); // Use the alias if provided (matches[1] is the captured group)
            }
            return true;
        }
        return false;
    }
    
    // Test connection_id() query pattern matching (mirrors actual MySQL.cpp implementation)
    bool matchesConnectionIdQuery(const string& query, string& extractedAlias) {
        string upperQuery = SToUpper(query);
        // First check if pattern matches at all
        if (SREMatch("^SELECT\\s+CONNECTION_ID\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, nullptr)) {
            // Default column name
            extractedAlias = "connection_id()";
            
            // Check if there's an alias captured
            vector<string> matches;
            if (SREMatch("^SELECT\\s+CONNECTION_ID\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, &matches) && matches.size() > 1) {
                extractedAlias = SToLower(matches[1]); // Use the alias if provided (matches[1] is the captured group)
            }
            return true;
        }
        return false;
    }
    
    // Test table name extraction from information_schema.columns queries
    string extractTableNameFromColumnsQuery(const string& query) {
        string tableName;
        string upperQuery = SToUpper(query);
        
        // Find "TABLE_NAME = '" pattern
        size_t pos = upperQuery.find("TABLE_NAME");
        if (pos != string::npos) {
            // Find the equals sign
            size_t equalsPos = upperQuery.find("=", pos);
            if (equalsPos != string::npos) {
                // Find the opening quote after the equals
                size_t quoteStart = query.find_first_of("'\"", equalsPos);
                if (quoteStart != string::npos) {
                    char quoteChar = query[quoteStart];
                    // Find the closing quote
                    size_t quoteEnd = query.find(quoteChar, quoteStart + 1);
                    if (quoteEnd != string::npos) {
                        tableName = query.substr(quoteStart + 1, quoteEnd - quoteStart - 1);
                    }
                }
            }
        }
        return tableName;
    }
    
    // Test information schema query detection
    bool isInformationSchemaTablesQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.TABLES");
    }
    
    bool isInformationSchemaViewsQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.VIEWS");
    }
    
    bool isInformationSchemaColumnsQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.COLUMNS");
    }
    
    // Test SHOW KEYS query detection and table name extraction
    bool isShowKeysQuery(const string& query) {
        return SContains(SToUpper(query), "SHOW KEYS FROM");
    }
    
    string extractTableNameFromShowKeysQuery(const string& query) {
        string upperQuery = SToUpper(query);
        string tableName;
        
        // Look for patterns like "FROM `tablename`" or "FROM tablename"
        size_t fromPos = upperQuery.find("FROM");
        if (fromPos != string::npos) {
            size_t tableStart = query.find_first_not_of(" \t", fromPos + 4);
            if (tableStart != string::npos) {
                size_t tableEnd;
                if (query[tableStart] == '`') {
                    // Handle backtick-quoted table names like `bankAccounts`
                    tableStart++; // Skip opening backtick
                    tableEnd = query.find('`', tableStart);
                } else {
                    // Handle unquoted table names
                    tableEnd = query.find_first_of(" \t;", tableStart);
                    if (tableEnd == string::npos) tableEnd = query.length();
                }
                
                if (tableEnd != string::npos && tableEnd > tableStart) {
                    tableName = query.substr(tableStart, tableEnd - tableStart);
                }
            }
        }
        return tableName;
    }
    
    // Test foreign key constraint query detection
    bool isForeignKeyConstraintQuery(const string& query) {
        string upperQuery = SToUpper(query);
        return SContains(upperQuery, "INFORMATION_SCHEMA.KEY_COLUMN_USAGE") && 
               SContains(upperQuery, "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS");
    }
    
    string extractTableNameFromForeignKeyQuery(const string& query) {
        vector<string> matches;
        // Look for pattern "table_name = 'value'" or "cu.table_name = 'value'" (with optional table alias)
        if (SREMatch("(?:\\w+\\.)?table_name\\s*=\\s*['\"]([^'\"]+)['\"]", query, false, true, &matches) && matches.size() > 1) {
            return matches[1]; // matches[1] is the captured group, matches[0] is the entire match
        }
        return "";
    }
}

struct MySQLTest : tpunit::TestFixture {
    MySQLTest()
        : tpunit::TestFixture("MySQL",
                              TEST(MySQLTest::testVersionQueryPatternMatching),
                              TEST(MySQLTest::testVersionQueryAliasExtraction),
                              TEST(MySQLTest::testConnectionIdQueryPatternMatching),
                              TEST(MySQLTest::testConnectionIdQueryAliasExtraction),
                              TEST(MySQLTest::testTableNameExtractionFromColumnsQuery),
                              TEST(MySQLTest::testTableNameExtractionEdgeCases),
                              TEST(MySQLTest::testInformationSchemaQueryDetection),
                              TEST(MySQLTest::testShowKeysQueryDetection),
                              TEST(MySQLTest::testShowKeysTableNameExtraction),
                              TEST(MySQLTest::testForeignKeyConstraintQueryDetection),
                              TEST(MySQLTest::testForeignKeyConstraintTableNameExtraction),
                              TEST(MySQLTest::testQueryPatternEdgeCases)) { }

    void testVersionQueryPatternMatching() {
        string alias;
        
        // Test basic VERSION() query
        ASSERT_TRUE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION();", alias));
        ASSERT_TRUE(alias == "version()");
        
        // Test with different whitespace
        ASSERT_TRUE(MySQLTestHelpers::matchesVersionQuery("SELECT  VERSION(  );", alias));
        ASSERT_TRUE(alias == "version()");
        
        // Test case insensitive
        ASSERT_TRUE(MySQLTestHelpers::matchesVersionQuery("select version();", alias));
        ASSERT_TRUE(alias == "version()");
        
        // Test without semicolon
        ASSERT_TRUE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION()", alias));
        ASSERT_TRUE(alias == "version()");
    }

    void testVersionQueryAliasExtraction() {
        string alias;
        
        // Test with AS alias
        ASSERT_TRUE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION() AS v;", alias));
        ASSERT_TRUE(alias == "v");
        
        // Test with AS alias different case
        ASSERT_TRUE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION() as MyVersion;", alias));
        ASSERT_TRUE(alias == "myversion");
        
        // Test with AS alias and extra whitespace
        ASSERT_TRUE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION()  AS   serverVersion  ;", alias));
        ASSERT_TRUE(alias == "serverversion");
        
        // Test invalid patterns should not match
        ASSERT_FALSE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION() FROM table;", alias));
        ASSERT_FALSE(MySQLTestHelpers::matchesVersionQuery("UPDATE VERSION();", alias));
    }

    void testConnectionIdQueryPatternMatching() {
        string alias;
        
        // Test basic connection_id() query
        ASSERT_TRUE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT connection_id();", alias));
        ASSERT_TRUE(alias == "connection_id()");
        
        // Test case insensitive
        ASSERT_TRUE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT CONNECTION_ID();", alias));
        ASSERT_TRUE(alias == "connection_id()");
        
        // Test with whitespace
        ASSERT_TRUE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT  connection_id(  );", alias));
        ASSERT_TRUE(alias == "connection_id()");
    }

    void testConnectionIdQueryAliasExtraction() {
        string alias;
        
        // Test with AS alias
        ASSERT_TRUE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT connection_id() AS pid;", alias));
        ASSERT_TRUE(alias == "pid");
        
        // Test with AS alias different case
        ASSERT_TRUE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT CONNECTION_ID() as ConnID;", alias));
        ASSERT_TRUE(alias == "connid");
        
        // Test invalid patterns should not match
        ASSERT_FALSE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT connection_id FROM table;", alias));
        ASSERT_FALSE(MySQLTestHelpers::matchesConnectionIdQuery("INSERT connection_id();", alias));
    }

    void testTableNameExtractionFromColumnsQuery() {
        // Test basic table name extraction
        string query1 = "SELECT * FROM information_schema.columns WHERE table_name = 'accounts';";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query1) == "accounts");
        
        // Test with double quotes
        string query2 = "SELECT * FROM information_schema.columns WHERE table_name = \"users\";";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query2) == "users");
        
        // Test with whitespace around equals
        string query3 = "SELECT * FROM information_schema.columns WHERE table_name  =  'testTable';";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query3) == "testTable");
        
        // Test case preservation
        string query4 = "SELECT * FROM information_schema.columns WHERE table_name = 'CamelCaseTable';";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query4) == "CamelCaseTable");
    }

    void testTableNameExtractionEdgeCases() {
        // Test missing table_name
        string query1 = "SELECT * FROM information_schema.columns WHERE other_field = 'value';";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query1).empty());
        
        // Test malformed query (no closing quote)
        string query2 = "SELECT * FROM information_schema.columns WHERE table_name = 'accounts;";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query2).empty());
        
        // Test empty table name
        string query3 = "SELECT * FROM information_schema.columns WHERE table_name = '';";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query3).empty());
        
        // Test complex query with multiple conditions
        string query4 = "SELECT * FROM information_schema.columns WHERE table_schema = database() AND table_name = 'accounts' ORDER BY ordinal_position;";
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(query4) == "accounts");
    }

    void testInformationSchemaQueryDetection() {
        // Test tables query detection
        ASSERT_TRUE(MySQLTestHelpers::isInformationSchemaTablesQuery("SELECT * FROM information_schema.tables"));
        ASSERT_TRUE(MySQLTestHelpers::isInformationSchemaTablesQuery("select * from INFORMATION_SCHEMA.TABLES"));
        ASSERT_FALSE(MySQLTestHelpers::isInformationSchemaTablesQuery("SELECT * FROM information_schema.views"));
        
        // Test views query detection
        ASSERT_TRUE(MySQLTestHelpers::isInformationSchemaViewsQuery("SELECT * FROM information_schema.views"));
        ASSERT_TRUE(MySQLTestHelpers::isInformationSchemaViewsQuery("select * from INFORMATION_SCHEMA.VIEWS"));
        ASSERT_FALSE(MySQLTestHelpers::isInformationSchemaViewsQuery("SELECT * FROM information_schema.tables"));
        
        // Test columns query detection
        ASSERT_TRUE(MySQLTestHelpers::isInformationSchemaColumnsQuery("SELECT * FROM information_schema.columns"));
        ASSERT_TRUE(MySQLTestHelpers::isInformationSchemaColumnsQuery("select * from INFORMATION_SCHEMA.COLUMNS"));
        ASSERT_FALSE(MySQLTestHelpers::isInformationSchemaColumnsQuery("SELECT * FROM information_schema.routines"));
    }

    void testQueryPatternEdgeCases() {
        string alias;
        
        // Test queries that should NOT match VERSION pattern
        ASSERT_FALSE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION() + 1;", alias));
        ASSERT_FALSE(MySQLTestHelpers::matchesVersionQuery("SELECT VERSION(), other_field;", alias));
        ASSERT_FALSE(MySQLTestHelpers::matchesVersionQuery("SELECT * FROM VERSION();", alias));
        
        // Test queries that should NOT match connection_id pattern  
        ASSERT_FALSE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT connection_id() + 1;", alias));
        ASSERT_FALSE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT connection_id(), other_field;", alias));
        ASSERT_FALSE(MySQLTestHelpers::matchesConnectionIdQuery("SELECT * FROM connection_id();", alias));
        
        // Test edge cases for table name extraction
        string badQuery1 = "WHERE table_name = 'accounts' AND table_name = 'users'";  // Multiple matches
        string result1 = MySQLTestHelpers::extractTableNameFromColumnsQuery(badQuery1);
        ASSERT_TRUE(result1 == "accounts");  // Should get first match
        
        string badQuery2 = "SELECT table_name FROM tables";  // No equals sign
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromColumnsQuery(badQuery2).empty());
    }

    void testShowKeysQueryDetection() {
        // Test SHOW KEYS query detection
        ASSERT_TRUE(MySQLTestHelpers::isShowKeysQuery("SHOW KEYS FROM bankAccounts"));
        ASSERT_TRUE(MySQLTestHelpers::isShowKeysQuery("show keys from users"));
        ASSERT_TRUE(MySQLTestHelpers::isShowKeysQuery("SHOW KEYS FROM `table_name`"));
        ASSERT_TRUE(MySQLTestHelpers::isShowKeysQuery("SHOW KEYS FROM bankAccounts WHERE Key_name = 'PRIMARY'"));
        
        // Test queries that should NOT match
        ASSERT_FALSE(MySQLTestHelpers::isShowKeysQuery("SHOW TABLES"));
        ASSERT_FALSE(MySQLTestHelpers::isShowKeysQuery("SELECT * FROM bankAccounts"));
        ASSERT_FALSE(MySQLTestHelpers::isShowKeysQuery("SHOW COLUMNS FROM bankAccounts"));
    }

    void testShowKeysTableNameExtraction() {
        // Test basic table name extraction
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM bankAccounts") == "bankAccounts");
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM users") == "users");
        
        // Test backtick-quoted table names
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `bankAccounts`") == "bankAccounts");
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `table-name`") == "table-name");
        
        // Test with WHERE clause
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `bankAccounts` WHERE Key_name = 'PRIMARY'") == "bankAccounts");
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM users WHERE Key_name = 'PRIMARY'") == "users");
        
        // Test with extra whitespace
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM   bankAccounts   ") == "bankAccounts");
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM\t`table_name`\t") == "table_name");
        
        // Test edge cases
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS").empty()); // No FROM
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM").empty()); // No table name
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromShowKeysQuery("SHOW KEYS FROM `").empty()); // Malformed backticks
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
        
        ASSERT_TRUE(MySQLTestHelpers::isForeignKeyConstraintQuery(complexQuery));
        
        // Test variations
        ASSERT_TRUE(MySQLTestHelpers::isForeignKeyConstraintQuery("SELECT * FROM information_schema.key_column_usage JOIN information_schema.referential_constraints"));
        ASSERT_TRUE(MySQLTestHelpers::isForeignKeyConstraintQuery("select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"));
        
        // Test queries that should NOT match
        ASSERT_FALSE(MySQLTestHelpers::isForeignKeyConstraintQuery("SELECT * FROM information_schema.key_column_usage"));  // Missing referential_constraints
        ASSERT_FALSE(MySQLTestHelpers::isForeignKeyConstraintQuery("SELECT * FROM information_schema.referential_constraints"));  // Missing key_column_usage
        ASSERT_FALSE(MySQLTestHelpers::isForeignKeyConstraintQuery("SELECT * FROM information_schema.tables"));
        ASSERT_FALSE(MySQLTestHelpers::isForeignKeyConstraintQuery("SELECT * FROM bankAccounts"));
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
        
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery(complexQuery) == "bankAccounts");
        
        // Test with different quote styles and table alias prefixes
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("WHERE cu.table_name = \"users\"") == "users");
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("AND table_name = 'accounts'") == "accounts");
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("WHERE alias.table_name = 'test'") == "test");
        
        // Test with whitespace variations
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("WHERE table_name  =  'testTable'") == "testTable");
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("AND\ttable_name\t=\t\"my_table\"") == "my_table");
        
        // Test case insensitive matching
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("WHERE TABLE_NAME = 'CamelCase'") == "CamelCase");
        
        // Test edge cases
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("SELECT * FROM tables").empty()); // No table_name pattern
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("WHERE other_field = 'value'").empty()); // Wrong field
        ASSERT_TRUE(MySQLTestHelpers::extractTableNameFromForeignKeyQuery("WHERE table_name =").empty()); // No value
    }
} __MySQLTest;
