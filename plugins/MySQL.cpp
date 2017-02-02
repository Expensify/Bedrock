#include "MySQL.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

MySQLPacket::MySQLPacket() {
    // Initialize
    sequenceID = 0;
}

string MySQLPacket::serialize() {
    // Wrap in a 3-byte header
    uint32_t payloadLength = payload.size();
    string header;
    header.resize(4);
    memcpy(&header[0], &payloadLength, 3);
    header[3] = sequenceID;
    return header + payload;
}

int MySQLPacket::deserialize(const string& packet) {
    // Does it have a header?
    if (packet.size() < 4) {
        return 0;
    }

    // Has a header, parse it out
    uint32_t payloadLength = (*(uint32_t*)&packet[0]) & 0x00FFFFFF; // 3 bytes
    sequenceID = (uint8_t)packet[3];

    // Do we have enough data for the full payload?
    if (packet.size() < (4 + payloadLength)) {
        return 0;
    }

    // Have the full payload, parse it out
    payload.resize(payloadLength);
    memcpy(&payload[0], &packet[4], payloadLength);

    // Indicate that we've consumed this full packet
    return 4 + payloadLength;
}

string MySQLPacket::lenEncInt(uint64_t val) {
    // Encode based on the length.
    // **NOTE: The below assume this is running on a "little-endian"
    //         machine, which means the least significant byte comes first
    string out;
    void* valPtr = &val;
    if (val < 251) {
        // Take the last byte
        SAppend(out, valPtr, 1);
    } else if (val < 1 << 16) {
        // Take the last 2 bytes
        out += "\xFC";
        SAppend(out, valPtr, 2);
    } else if (val < 1 << 24) {
        // Take the last 3 bytes
        out += "\xFD";
        SAppend(out, valPtr, 3);
    } else {
        // Take all bytes
        out += "\xFE";
        SAppend(out, valPtr, sizeof(val));
    }
    return out;
}

string MySQLPacket::lenEncStr(const string& str) {
    // Add the length, and then the string
    return lenEncInt(str.size()) + str;
}

string MySQLPacket::serializeHandshake() {
    // Protocol described here:
    // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake

    // Just hard code the values for now
    MySQLPacket handshake;
    handshake.payload += lenEncInt(10);      // protocol version
    handshake.payload += (string) "bedrock"; // server version
    handshake.payload += lenEncInt(0);       // NULL
    uint32_t connectionID = 1;
    SAppend(handshake.payload, &connectionID, 4); // connection_id
    handshake.payload += (string) "xxxxxxxx";     // auth_plugin_data_part_1
    handshake.payload += lenEncInt(0);            // filler

    uint32_t CLIENT_LONG_PASSWORD = 0x00000001;
    uint32_t CLIENT_PLUGIN_AUTH   = 0x00080000;
    uint32_t capability_flags = CLIENT_LONG_PASSWORD | CLIENT_PLUGIN_AUTH;

    uint16_t capability_flags_1 = (const unsigned short)(capability_flags);
    uint16_t capability_flags_2 = (const unsigned short)(capability_flags >> 16);
    SAppend(handshake.payload, &capability_flags_1, 2); // capability_flags_1 (low 2 bytes)

    uint8_t latin1_swedish_ci = 0x08;
    SAppend(handshake.payload, &latin1_swedish_ci, 1); // character_set

    uint16_t SERVER_STATUS_AUTOCOMMIT = 0x0002;
    SAppend(handshake.payload, &SERVER_STATUS_AUTOCOMMIT, 2); // status_flags

    SAppend(handshake.payload, &capability_flags_2, 2); // capability_flags_2 (high 2 bytes)

    // Random challenge bytes client expects for mysql_native_password authentication.
    // Hardcoded for now as proper authentication is not yet supported by Bedrock.
    // Specific bytes are taken from example handshake packed provided by Oracle:
    // https://dev.mysql.com/doc/internals/en/client-wants-native-server-wants-old.html
    // (Initial Handshake Packet)
    uint8_t auth_plugin_data[] = {
        0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x40, 0x42, 0x68, 0x66, 0x48,
        0x74, 0x2f, 0x2d, 0x34, 0x5e, 0x5a, 0x2c, 0x00 };

    SAppend(handshake.payload, auth_plugin_data, sizeof(auth_plugin_data));

    handshake.payload += (string) "mysql_native_password"; // auth_plugin_name
    handshake.payload += lenEncInt(0);                     // filler

    return handshake.serialize();
}

string MySQLPacket::serializeQueryResponse(int sequenceID, const SQResult& result) {
    // Add the response
    string sendBuffer;

    // First the column count
    MySQLPacket columnCount;
    columnCount.sequenceID = ++sequenceID;
    columnCount.payload = lenEncInt(result.headers.size());
    sendBuffer += columnCount.serialize();

    // Add all the columns
    for (const auto& header : result.headers) {
        // Now a column description
        MySQLPacket column;
        column.sequenceID = ++sequenceID;
        column.payload += lenEncStr("unknown"); // table name
        column.payload += lenEncStr(header);    // column name
        column.payload += lenEncInt(3);         // length of column length field
        uint32_t colLength = 1024;
        SAppend(column.payload, &colLength, 3); // column length (3 bytes)
        column.payload += lenEncInt(1);         // length of type field
        column.payload += lenEncInt(0);         // type (int or static string)
        column.payload += lenEncInt(2);         // length of flags + decimals
        SAppend(column.payload, "\00\00", 2);   // flags + decimals
        sendBuffer += column.serialize();
    }

    // EOF packet to signal no more columns
    MySQLPacket eofPacket;
    eofPacket.sequenceID = ++sequenceID;
    SAppend(eofPacket.payload, "\xFE", 1); // EOF
    sendBuffer += eofPacket.serialize();

    // Add all the rows
    for (const auto& row : result.rows) {
        // Now the row
        MySQLPacket rowPacket;
        rowPacket.sequenceID = ++sequenceID;
        for (const auto& cell : row) {
            rowPacket.payload += lenEncStr(cell);
        }
        SAppend(rowPacket.payload, "\xFE", 1); // EOF
        sendBuffer += rowPacket.serialize();
    }

    // Finish with another EOF packet
    eofPacket.sequenceID = ++sequenceID;
    sendBuffer += eofPacket.serialize();

    // Done!
    return sendBuffer;
}

string MySQLPacket::serializeOK(int sequenceID) {
    // Just fill out the packet
    MySQLPacket ok;
    ok.sequenceID = sequenceID + 1;
    ok.payload += lenEncInt(0); // OK
    ok.payload += lenEncInt(0); // Affected rows
    ok.payload += lenEncInt(0); // Last insert ID
    ok.payload += "OK";         // Message
    return ok.serialize();
}

string MySQLPacket::serializeERR(int sequenceID, uint16_t code, const string& message) {
    // Fill it with our custom error message
    MySQLPacket err;
    err.sequenceID = sequenceID + 1;
    err.payload += "\xFF";                     // Header of the ERR packet
    SAppend(err.payload, &code, sizeof(code)); // Error code
    err.payload += message;                    // Error message
    return err.serialize();
}

void BedrockPlugin_MySQL::onPortAccept(STCPManager::Socket* s) {
    // Send Protocol::HandshakeV10
    SINFO("Accepted MySQL request from '" << s->addr << "'");
    s->send(MySQLPacket::serializeHandshake());
}

bool BedrockPlugin_MySQL::onPortRecv(STCPManager::Socket* s, SNodeData& request) {
    // Get any new MySQL requests
    int packetSize = 0;
    MySQLPacket packet;
    while ((packetSize = packet.deserialize(s->recvBuffer))) {
        // Got a packet, process it
        SDEBUG("Received command #" << (int)packet.payload[0] << ": '" << SToHex(packet.serialize()) << "'");
        SConsumeFront(s->recvBuffer, packetSize);
        switch (packet.payload[0]) {
        case 3: { // COM_QUERY
            // Decode the query
            string query = packet.payload.substr(1, packet.payload.size() - 1);
            SINFO("Processing query '" << query << "'");

            // See if it's asking for a global variable
            string varName;
            string regExp = "^(?:(?:SELECT\\s+)?@@(?:\\w+\\.)?|SHOW VARIABLES LIKE ')(\\w+).*$";
            if (pcrecpp::RE(regExp, pcrecpp::RE_Options().set_caseless(true)).FullMatch(query, &varName)) {
                // Loop across and look for it
                SQResult result;
                result.headers.push_back(varName);
                for (int c = 0; c < MYSQL_NUM_VARIABLES; ++c) {
                    if (SIEquals(g_MySQLVariables[c][0], varName)) {
                        // Found it!
                        SINFO("Returning variable '" << varName << "'='" << g_MySQLVariables[c][1] << "'");
                        result.rows.resize(1);
                        result.rows[0].push_back(g_MySQLVariables[c][1]);
                        break;
                    }
                }
                if (result.rows.empty()) {
                    SHMMM("Couldn't find variable '" << varName << "', returning empty.");
                }
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SIEquals(query, "SHOW VARIABLES")) {
                // Return the variable list
                SINFO("Responding with fake variable list");
                SQResult result;
                result.headers.push_back("Variable Name");
                result.headers.push_back("Value");
                for (int c = 0; c < MYSQL_NUM_VARIABLES; ++c) {
                    result.rows.resize(result.rows.size() + 1);
                    result.rows.back().resize(2);
                    result.rows.back()[0] = g_MySQLVariables[c][0];
                    result.rows.back()[1] = g_MySQLVariables[c][1];
                }
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SIEquals(query, "SHOW DATABASES")) {
                // Return a fake "main" database
                SINFO("Responding with fake database list");
                SQResult result;
                result.headers.push_back("Database");
                result.rows.resize(1);
                result.rows.back().push_back("main");
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SIEquals(query, "SHOW /*!50002 FULL*/ TABLES")) {
                // Return an empty list of tables
                SINFO("Responding with fake table list");
                SQResult result;
                result.headers.push_back("Tables");
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SContains(query, "information_schema")) {
                // Return an empty set
                SINFO("Responding with empty routine list");
                SQResult result;
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SStartsWith(SToUpper(query), "SET ") || SStartsWith(SToUpper(query), "USE ")) {
                // Ignore
                s->send(MySQLPacket::serializeOK(packet.sequenceID));
            } else {
                // Transform this into an internal request
                request.methodLine = "Query";
                request["format"] = "json";
                request["sequenceID"] = SToStr(packet.sequenceID);
                request["query"] = query;
            }
            break;
        }

        default: { // Say OK to everything else
            // Send OK
            SINFO("Sending OK");
            s->send(MySQLPacket::serializeOK(packet.sequenceID));
            break;
        }
        }
    }

    // Keep the socket alive
    return true;
}

bool BedrockPlugin_MySQL::onPortRequestComplete(const SNodeData& response, STCPManager::Socket* s) {
    // Only one request supported: Query.
    SASSERT(SIEquals(response["request.methodLine"], "Query"));
    SASSERT(response.isSet("request.sequenceID"));
    if (SToInt(response.methodLine) == 200) {
        // Success!  Were there any results?
        if (response.content.empty()) {
            // Just send OK
            s->send(MySQLPacket::serializeOK(response.calc("request.sequenceID")));
        } else {
            // Convert the JSON response from Bedrock::DB into MySQL protocol
            SQResult result;
            SASSERT(response.content.empty() || result.deserialize(response.content));
            s->send(MySQLPacket::serializeQueryResponse(response.calc("request.sequenceID"), result));
        }
    } else {
        // Failure -- pass along the message
        s->send(MySQLPacket::serializeERR(response.calc("request.sequenceID"), SToInt(response.methodLine),
                                          response["error"]));
    }
    return true;
}

// Define the global variable list to pretend to be MySQL
const char* g_MySQLVariables[MYSQL_NUM_VARIABLES][2] = {
    {"auto_increment_increment", "1"},
    {"auto_increment_offset", "1"},
    {"autocommit", "ON"},
    {"automatic_sp_privileges", "ON"},
    {"back_log", "50"},
    {"basedir", "/rdsdbbin/mysql-5.1.73a.R1/"},
    {"big_tables", "OFF"},
    {"binlog_cache_size", "32768"},
    {"binlog_direct_non_transactional_updates", "OFF"},
    {"binlog_format", "MIXED"},
    {"bulk_insert_buffer_size", "8388608"},
    {"character_set_client", "latin1"},
    {"character_set_connection", "latin1"},
    {"character_set_database", "utf8"},
    {"character_set_filesystem", "binary"},
    {"character_set_results", "latin1"},
    {"character_set_server", "latin1"},
    {"character_set_system", "utf8"},
    {"character_sets_dir", "/rdsdbbin/mysql-5.1.73a.R1/share/mysql/charsets/"},
    {"collation_connection", "latin1_swedish_ci"},
    {"collation_database", "utf8_unicode_ci"},
    {"collation_server", "latin1_swedish_ci"},
    {"completion_type", "0"},
    {"concurrent_insert", "1"},
    {"connect_timeout", "15"},
    {"datadir", "/rdsdbdata/db/"},
    {"date_format", "%Y-%m-%d"},
    {"datetime_format", "%Y-%m-%d %H:%i:%s"},
    {"default_week_format", "0"},
    {"delay_key_write", "ON"},
    {"delayed_insert_limit", "100"},
    {"delayed_insert_timeout", "300"},
    {"delayed_queue_size", "1000"},
    {"div_precision_increment", "4"},
    {"engine_condition_pushdown", "ON"},
    {"error_count", "0"},
    {"event_scheduler", "OFF"},
    {"expire_logs_days", "0"},
    {"flush", "OFF"},
    {"flush_time", "0"},
    {"foreign_key_checks", "ON"},
    {"ft_boolean_syntax", "+ -><()~*:"
                          "&|"},
    {"ft_max_word_len", "84"},
    {"ft_min_word_len", "4"},
    {"ft_query_expansion_limit", "20"},
    {"ft_stopword_file", "(built-in)"},
    {"general_log", "OFF"},
    {"general_log_file", "/rdsdbdata/log/general/mysql-general.log"},
    {"group_concat_max_len", "1024"},
    {"have_community_features", "YES"},
    {"have_compress", "YES"},
    {"have_crypt", "YES"},
    {"have_csv", "YES"},
    {"have_dynamic_loading", "YES"},
    {"have_geometry", "YES"},
    {"have_innodb", "YES"},
    {"have_ndbcluster", "NO"},
    {"have_openssl", "YES"},
    {"have_partitioning", "YES"},
    {"have_query_cache", "YES"},
    {"have_rtree_keys", "YES"},
    {"have_ssl", "YES"},
    {"have_symlink", "YES"},
    {"hostname", "ip-10-178-20-210"},
    {"identity", "0"},
    {"ignore_builtin_innodb", "ON"},
    {"init_connect", ""},
    {"init_file", ""},
    {"init_slave", ""},
    {"innodb_adaptive_flushing", "ON"},
    {"innodb_adaptive_hash_index", "ON"},
    {"innodb_additional_mem_pool_size", "2097152"},
    {"innodb_autoextend_increment", "8"},
    {"innodb_autoinc_lock_mode", "1"},
    {"innodb_buffer_pool_size", "11674845184"},
    {"innodb_change_buffering", "inserts"},
    {"innodb_checksums", "ON"},
    {"innodb_commit_concurrency", "0"},
    {"innodb_concurrency_tickets", "500"},
    {"innodb_data_file_path", "ibdata1:10M:autoextend"},
    {"innodb_data_home_dir", "/rdsdbdata/db/innodb"},
    {"innodb_doublewrite", "ON"},
    {"innodb_fast_shutdown", "1"},
    {"innodb_file_format", "Antelope"},
    {"innodb_file_format_check", "Barracuda"},
    {"innodb_file_per_table", "ON"},
    {"innodb_flush_log_at_trx_commit", "0"},
    {"innodb_flush_method", "O_DIRECT"},
    {"innodb_force_recovery", "0"},
    {"innodb_io_capacity", "200"},
    {"innodb_lock_wait_timeout", "50"},
    {"innodb_locks_unsafe_for_binlog", "OFF"},
    {"innodb_log_buffer_size", "8388608"},
    {"innodb_log_file_size", "134217728"},
    {"innodb_log_files_in_group", "2"},
    {"innodb_log_group_home_dir", "/rdsdbdata/log/innodb"},
    {"innodb_max_dirty_pages_pct", "75"},
    {"innodb_max_purge_lag", "0"},
    {"innodb_mirrored_log_groups", "1"},
    {"innodb_old_blocks_pct", "37"},
    {"innodb_old_blocks_time", "0"},
    {"innodb_open_files", "300"},
    {"innodb_random_read_ahead", "OFF"},
    {"innodb_read_ahead_threshold", "56"},
    {"innodb_read_io_threads", "4"},
    {"innodb_replication_delay", "0"},
    {"innodb_rollback_on_timeout", "OFF"},
    {"innodb_spin_wait_delay", "6"},
    {"innodb_stats_method", "nulls_equal"},
    {"innodb_stats_on_metadata", "ON"},
    {"innodb_stats_sample_pages", "8"},
    {"innodb_strict_mode", "OFF"},
    {"innodb_support_xa", "ON"},
    {"innodb_sync_spin_loops", "30"},
    {"innodb_table_locks", "ON"},
    {"innodb_thread_concurrency", "0"},
    {"innodb_thread_sleep_delay", "10000"},
    {"innodb_use_sys_malloc", "ON"},
    {"innodb_version", "5.1.73"},
    {"innodb_write_io_threads", "4"},
    {"insert_id", "0"},
    {"interactive_timeout", "28800"},
    {"join_buffer_size", "131072"},
    {"keep_files_on_create", "OFF"},
    {"key_buffer_size", "16777216"},
    {"key_cache_age_threshold", "300"},
    {"key_cache_block_size", "1024"},
    {"key_cache_division_limit", "100"},
    {"language", "/rdsdbbin/mysql/share/mysql/english/"},
    {"large_files_support", "ON"},
    {"large_page_size", "0"},
    {"large_pages", "OFF"},
    {"last_insert_id", "0"},
    {"lc_time_names", "en_US"},
    {"license", "GPL"},
    {"local_infile", "ON"},
    {"locked_in_memory", "OFF"},
    {"log", "OFF"},
    {"log_bin", "ON"},
    {"log_bin_trust_function_creators", "ON"},
    {"log_bin_trust_routine_creators", "ON"},
    {"log_error", "/rdsdbdata/log/error/mysql-error.log"},
    {"log_output", "TABLE"},
    {"log_queries_not_using_indexes", "OFF"},
    {"log_slave_updates", "OFF"},
    {"log_slow_queries", "ON"},
    {"log_warnings", "1"},
    {"long_query_time", "10.000000"},
    {"low_priority_updates", "OFF"},
    {"lower_case_file_system", "OFF"},
    {"lower_case_table_names", "0"},
    {"max_allowed_packet", "5242880"},
    {"max_binlog_cache_size", "18446744073709547520"},
    {"max_binlog_size", "134217728"},
    {"max_connect_errors", "1000000000"},
    {"max_connections", "1500"},
    {"max_delayed_threads", "20"},
    {"max_error_count", "64"},
    {"max_heap_table_size", "16777216"},
    {"max_insert_delayed_threads", "20"},
    {"max_join_size", "18446744073709551615"},
    {"max_length_for_sort_data", "1024"},
    {"max_long_data_size", "5242880"},
    {"max_prepared_stmt_count", "16382"},
    {"max_relay_log_size", "0"},
    {"max_seeks_for_key", "18446744073709551615"},
    {"max_sort_length", "1024"},
    {"max_sp_recursion_depth", "0"},
    {"max_tmp_tables", "32"},
    {"max_user_connections", "600"},
    {"max_write_lock_count", "18446744073709551615"},
    {"min_examined_row_limit", "0"},
    {"multi_range_count", "256"},
    {"myisam_data_pointer_size", "6"},
    {"myisam_max_sort_file_size", "9223372036853727232"},
    {"myisam_mmap_size", "18446744073709551615"},
    {"myisam_recover_options", "OFF"},
    {"myisam_repair_threads", "1"},
    {"myisam_sort_buffer_size", "8388608"},
    {"myisam_stats_method", "nulls_unequal"},
    {"myisam_use_mmap", "OFF"},
    {"net_buffer_length", "16384"},
    {"net_read_timeout", "30"},
    {"net_retry_count", "10"},
    {"net_write_timeout", "60"},
    {"new", "OFF"},
    {"old", "OFF"},
    {"old_alter_table", "OFF"},
    {"old_passwords", "OFF"},
    {"open_files_limit", "65535"},
    {"optimizer_prune_level", "1"},
    {"optimizer_search_depth", "62"},
    {"optimizer_switch", "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on"},
    {"pid_file", "/rdsdbdata/log/mysql-3306.pid"},
    {"plugin_dir", "/rdsdbbin/mysql/lib/mysql/plugin"},
    {"port", "3306"},
    {"preload_buffer_size", "32768"},
    {"profiling", "OFF"},
    {"profiling_history_size", "15"},
    {"protocol_version", "10"},
    {"pseudo_thread_id", "26810995"},
    {"query_alloc_block_size", "8192"},
    {"query_cache_limit", "1048576"},
    {"query_cache_min_res_unit", "4096"},
    {"query_cache_size", "0"},
    {"query_cache_type", "ON"},
    {"query_cache_wlock_invalidate", "OFF"},
    {"query_prealloc_size", "8192"},
    {"rand_seed1", ""},
    {"rand_seed2", ""},
    {"range_alloc_block_size", "4096"},
    {"read_buffer_size", "262144"},
    {"read_only", "OFF"},
    {"read_rnd_buffer_size", "524288"},
    {"relay_log", "/rdsdbdata/log/relaylog/relaylog"},
    {"relay_log_index", ""},
    {"relay_log_info_file", "relay-log.info"},
    {"relay_log_purge", "ON"},
    {"relay_log_space_limit", "0"},
    {"report_host", ""},
    {"report_password", ""},
    {"report_port", "3306"},
    {"report_user", ""},
    {"rpl_recovery_rank", "0"},
    {"secure_auth", "OFF"},
    {"secure_file_priv", "/tmp/"},
    {"server_id", "973870556"},
    {"skip_external_locking", "ON"},
    {"skip_name_resolve", "OFF"},
    {"skip_networking", "OFF"},
    {"skip_show_database", "OFF"},
    {"slave_compressed_protocol", "OFF"},
    {"slave_exec_mode", "STRICT"},
    {"slave_load_tmpdir", "/rdsdbdata/tmp"},
    {"slave_max_allowed_packet", "1073741824"},
    {"slave_net_timeout", "3600"},
    {"slave_skip_errors", "OFF"},
    {"slave_transaction_retries", "10"},
    {"slow_launch_time", "2"},
    {"slow_query_log", "ON"},
    {"slow_query_log_file", "/rdsdbdata/log/slowquery/mysql-slowquery.log"},
    {"socket", "/tmp/mysql.sock"},
    {"sort_buffer_size", "2097144"},
    {"sql_auto_is_null", "ON"},
    {"sql_big_selects", "ON"},
    {"sql_big_tables", "OFF"},
    {"sql_buffer_result", "OFF"},
    {"sql_log_bin", "ON"},
    {"sql_log_off", "OFF"},
    {"sql_log_update", "ON"},
    {"sql_low_priority_updates", "OFF"},
    {"sql_max_join_size", "18446744073709551615"},
    {"sql_mode", ""},
    {"sql_notes", "ON"},
    {"sql_quote_show_create", "ON"},
    {"sql_safe_updates", "OFF"},
    {"sql_select_limit", "18446744073709551615"},
    {"sql_slave_skip_counter", ""},
    {"sql_warnings", "OFF"},
    {"ssl_ca", "/rdsdbdata/rds-metadata/ca-cert.pem"},
    {"ssl_capath", ""},
    {"ssl_cert", "/rdsdbdata/rds-metadata/server-cert.pem"},
    {"ssl_cipher", "EXP1024-RC4-SHA:EXP1024-DES-CBC-SHA:AES256-SHA:AES128-SHA:DES-CBC3-SHA:DES-CBC-SHA:EXP-DES-CBC-SHA:"
                   "EXP-RC2-CBC-MD5:RC4-SHA:RC4-MD5:EXP-RC4-MD5:NULL-SHA:NULL-MD5:DES-CBC3-MD5:DES-CBC-MD5:EXP-RC2-CBC-"
                   "MD5:RC2-CBC-MD5:EXP-RC4-MD5:RC4-MD5:KRB5-DES-CBC3-MD5:KRB5-DES-CBC3-SHA:ADH-DES-CBC3-SHA:EDH-RSA-"
                   "DES-CBC3-SHA:EDH-DSS-DES-CBC3-SHA:ADH-AES256-SHA:DHE-RSA-AES256-SHA:DHE-DSS-AES256-SHA:ADH-AES128-"
                   "SHA:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA:EXP-KRB5-RC4-MD5:EXP-KRB5-RC2-CBC-MD5:EXP-KRB5-DES-CBC-"
                   "MD5:KRB5-RC4-MD5:KRB5-DES-CBC-MD5:ADH-RC4-MD5:EXP-ADH-RC4-MD5:DHE-DSS-RC4-SHA:EXP1024-DHE-DSS-RC4-"
                   "SHA:EXP1024-DHE-DSS-DES-CBC-SHA:EXP-KRB5-RC4-SHA:EXP-KRB5-RC2-CBC-SHA:EXP-KRB5-DES-CBC-SHA:KRB5-"
                   "RC4-SHA:KRB5-DES-CBC-SHA:ADH-DES-CBC-SHA:EXP-ADH-DES-CBC-SHA:EDH-RSA-DES-CBC-SHA:EXP-EDH-RSA-DES-"
                   "CBC-SHA:EDH-DSS-DES-CBC-SHA:EXP-EDH-DSS-DES-CBC-SHA"},
    {"ssl_key", "/rdsdbdata/rds-metadata/server-key.pem"},
    {"storage_engine", "InnoDB"},
    {"sync_binlog", "0"},
    {"sync_frm", "ON"},
    {"system_time_zone", "UTC"},
    {"table_definition_cache", "256"},
    {"table_lock_wait_timeout", "50"},
    {"table_open_cache", "96"},
    {"table_type", "InnoDB"},
    {"thread_cache_size", "0"},
    {"thread_handling", "one-thread-per-connection"},
    {"thread_stack", "196608"},
    {"time_format", "%H:%i:%s"},
    {"time_zone", "UTC"},
    {"timed_mutexes", "OFF"},
    {"timestamp", "1454813864"},
    {"tmp_table_size", "16777216"},
    {"tmpdir", "/rdsdbdata/tmp"},
    {"transaction_alloc_block_size", "8192"},
    {"transaction_prealloc_size", "4096"},
    {"tx_isolation", "REPEATABLE-READ"},
    {"unique_checks", "ON"},
    {"updatable_views_with_limit", "YES"},
    {"version", "5.1.73-log"},
    {"version_comment", SVERSION},
    {"version_compile_machine", "x86_64"},
    {"version_compile_os", "unknown-linux-gnu"},
    {"wait_timeout", "28800"},
    {"warning_count", "0"},
};
