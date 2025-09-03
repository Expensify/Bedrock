#include "MySQLUtils.h"

namespace MySQLUtils {

    bool parseVersionQuery(const string& query, vector<string>& matches) {
        string upperQuery = SToUpper(query);
        return SREMatch("^SELECT\\s+VERSION\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, &matches);
    }

    bool parseConnectionIdQuery(const string& query, vector<string>& matches) {
        string upperQuery = SToUpper(query);
        return SREMatch("^SELECT\\s+CONNECTION_ID\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, &matches);
    }

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

    bool isInformationSchemaTablesQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.TABLES");
    }

    bool isInformationSchemaViewsQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.VIEWS");
    }

    bool isInformationSchemaColumnsQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.COLUMNS");
    }

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

} // namespace MySQLUtils
