## Source findings

**1. `libstuff/libstuff`** — priority 43.9, high, blast 68, **7 findings**

   - SQuery/SQVerifyTable/SQVerifyTableExists/SQ/SQList → `A dedicated SQLite-execution unit alongside SQResult/SQValue/SQliteParameter`
     <br/>A full SQLite execution engine (busy-retry-on-SQLITE_BUSY loop, named-parameter binding, corruption detection, slow-query logging) declared and implemented in a generic string-utility header/source, while SQResult, SQValue, and SQliteParameter already live in this same directory as their own dedicated units
   - SParseHTTP/SComposeHTTP/SParseURI/SParseURIPath/SComposePOST/SParseHost family → `SHTTPSManager or a new SHTTPMessage unit`
     <br/>A complete HTTP wire-format grammar (headers, chunked transfer-encoding, cookie folding, URI parsing) sitting in the generic utility file even though this directory already has a dedicated SHTTPSManager for HTTP transactions
   - SAESEncrypt/SAESDecrypt/SHashSHA1/SHashSHA256/SEncodeBase64/SDecodeBase64/SHMACS → `A new SCrypto unit`
     <br/>Self-contained mbedtls wrappers with no dependency on anything else in this file
   - The direct-syslog-socket pool (SLogSocketFD/SLogSocketMutex/SLogSocketAddr/SSysl → `SLog.cpp`
     <br/>A complete alternate logging transport with its own per-socket locking, sitting beside unrelated HTTP/JSON/SQL/crypto code, even though SLog.cpp already exists in this directory as the dedicated logging-support unit
   - S_socket/S_close/S_accept/S_recvfrom/S_recvappend/S_sendconsume/S_poll/SFDset/SF → `STCPManager`
     <br/>Raw socket/poll primitives whose only real caller is STCPManager, the dedicated socket-management class that already lives in this same directory
   - SGZip/SGUnzip
     <br/>Gzip compression, unrelated to the string/HTTP/SQL utilities around it; small and self-contained
   - _SParseHTTP_GetUpToNext/_SParseHTTP_GetUpToEnd/_SDecodeURIChar/_SParseJSONString
     <br/>Leading-underscore naming signals 'file-private' but none are declared static or placed in an anonymous namespace, so they have external linkage and could collide with an identically-named symbol in another translation unit

**2. `sqlitecluster/SQLite`** — priority 8.3, high, blast 13, **5 findings**

   - BedrockPlugin_Compression::compress/decompress/registerSQLite calls in SQLite.cp → `a libstuff/sqlitecluster-level compression helper that plugins/Compression itself builds on, instead of the reverse`
     <br/>core sqlitecluster engine depends on plugins/Compression, a Bedrock command plugin, to compress journal entries and register SQLite UDFs - an inverted layering dependency for a library that otherwise only depends on libstuff
   - SQLite::enableRewrite / setRewriteHandler / _rewriteHandler
     <br/>query-rewriting exists, per its own comment, "to support mocked requests and load testing" - a test/mock-only feature wired directly into the production replicated-DB class
   - SQLite::setUpdateNoopMode / _noopUpdateMode
     <br/>noop-update mode is documented as existing only for `mockRequest`-enabled command testing, but lives as first-class state on the core DB class
   - Commented-out HC-Tree slow-commit diagnostic block inside SQLite::commit()
     <br/>~15 lines of disabled debug-logging code left in place rather than removed or gated behind a runtime flag
   - SQLite::SharedData::writeLock
     <br/>per its own comment this exists specifically to support the BlockWrites command - a single command's requirement implemented as a generic-looking member of the cross-handle SharedData structure

**3. `BedrockServer`** — priority 6.3, high, blast 12, **3 findings**

   - __quiesceLock/__quiesceShouldUnlock/__quiesceThread
     <br/>Plain (non-static) file-scope globals with external linkage rather than BedrockServer members, holding exactly the kind of per-server state (which thread owns the DB quiesce lock) this file otherwise keeps private; their double-leading-underscore names are also identifiers reserved to the implementation by the C++ standard
   - BedrockServer::commandPortSuppressionReasons
     <br/>Public list<string> whose doc comment references an 'instance of commandPortSuppressionCount', a type that does not exist anywhere in the repo; the member itself is never read or written anywhere, a dead vestige of an earlier refactor of suppressCommandPort
   - BedrockServer::_control
     <br/>One ~250-line function directly owns the tuning knobs of several otherwise-separate subsystems (conflict retry counts and page-lock policy, the blocking queue's rate-limit windows, DB quiesce, socket-thread caps, sync-node priority, journal hash lookup) instead of delegating each to its own subsystem

**4. `libstuff/SData`** — priority 4.7, low, blast 67

   - SData::deserialize simdjson padding logic → `wherever the simdjson parsing of these values actually happens`
     <br/>unconditionally reserves 32 extra bytes on any JSON-looking header value to satisfy simdjson's trailing-scratch-space requirement; a generic HTTP message container shaping its storage to one downstream JSON parser's needs
   - SData::create
     <br/>marked **DEPRECATED** in favor of the constructor but still present as public API

**5. `BedrockPlugin`** — priority 2.3, med, blast 11

   - BedrockPlugin::verifyAttributeInt64/verifyAttributeSize/verifyAttributeBool/veri → `libstuff/SData.h or a new shared request-validation unit`
     <br/>Purely static helpers that operate only on an SData request with no dependency on BedrockPlugin state or the plugin lifecycle; read like generic request-validation utilities rather than plugin-system code

**6. `sqlitecluster/SQLiteNode`** — priority 2.1, med, blast 12, **3 findings**

   - #include <plugins/Compression.h> and BedrockPlugin_Compression::decompress calls
     <br/>sqlitecluster/ is a lower-level replication library depending directly on one specific plugin under plugins/, inverting the codebase's normal dependency direction (plugins depend on sqlitecluster, not the reverse); sqlitecluster/SQLite.cpp has the same dependency, so it's an established pattern rather than unique to this file
   - SQLiteNode::KILLABLE_SQLITE_NODE / SQLiteNode::NODE_KILLED
     <br/>a global static pointer/flag letting process-wide signal-handling code reach into one specific node instance to kill its sockets; shutdown coordination bolted onto the node class rather than a dedicated signal-handling module
   - SQLiteNode::_priority / SQLiteNode::_syncPeer
     <br/>both carry inline comments marking them "Remove" with links to open GitHub issues (208449, 208439) -- acknowledged, unresolved design debt in the node's state tracking

**7. `libstuff/JSON/Value`** — priority 1.8, med, blast 9, **4 findings**

   - Value::mergeDeep(useSQLiteMergeBehavior)
     <br/>a generic JSON value type's merge API names a specific downstream consumer (SQLite) in its parameter; the behavior it selects is really just RFC 7386 JSON Merge Patch semantics
   - JSON::logStackTraceOnEnsureTypeFailure
     <br/>mutable global-ish thread_local debug/diagnostics toggle declared inline in the core value-type header; reads like a logging switch, not part of the JSON data model
   - Value::startTime / Value::logSlowConstructor
     <br/>constructor-timing instrumentation adds a chrono::time_point member to every Value instance, even scalars that never touch it, purely to detect occasional slow map/vector-based construction
   - friend class SAXHandler
     <br/>grants SAXHandler direct access to Value's private storage for fast construction; the source comment itself calls this out as 'Coupling++'

**8. `BedrockCommand`** — priority 1.5, med, blast 7, **3 findings**

   - BedrockCommand::getMethodName → `the Auth plugin, or null`
     <br/>special-cases a 'returnValueList' suffix documented in-code as a hack to support one legacy plugin format (Auth's old Get), inside the generic base command class
   - BedrockCommand::GrowOnlyList<T> → `libstuff (as a reusable append-only-list utility)`
     <br/>a generic, command-agnostic container template with no dependency on BedrockCommand, defined inline inside it
   - BedrockCommand.cpp: finalizeTimingInfo
     <br/>a sizeable timing-metrics/logging routine (per-phase totals, upstream-value promotion, a long SINFO composition) bundled into the command class itself rather than a separate metrics helper

**9. `libstuff/SHTTPSManager`** — priority 1.1, med, blast 4, **3 findings**

   - SHTTPSManager (class) → `near BedrockPlugin, or keep in libstuff but move the BedrockPlugin coupling into a thinner adapter`
     <br/>exists only to attach a BedrockPlugin& to the manager, so libstuff depends on BedrockPlugin.h - inverts the expected dependency direction
   - SHTTPSManager.cpp: #include <BedrockServer.h>, #include <sqlitecluster/SQLiteNod
     <br/>neither BedrockServer:: nor SQLiteNode:: is referenced anywhere in the file; unused includes coupling libstuff to the application/clustering layers
   - SStandaloneHTTPSManager::Transaction::Transaction throwing on isBlockingCommitTh
     <br/>encodes a Bedrock-specific execution-model rule inside an otherwise generic transaction constructor

**10. `plugins/Jobs`** — priority 1.0, high, blast 1, **3 findings**

   - BedrockPlugin_Jobs::upgradeDatabase (jobsPriorityNextRunManualSmartScan*, jobsMa
     <br/>Hardcodes Expensify-specific job-name literals (manual/SmartScan*, www-prod/*, www-stag/*) as partial-index WHERE clauses inside an otherwise generic, reusable job-queue plugin
   - scopedDisableNoopMode → `libstuff or SQLite.h`
     <br/>Generic SQLite-noop RAII guard with no Jobs-specific logic, named outside repo type-naming convention, defined locally instead of in a shared header where other plugins needing it could reuse it
   - BedrockPlugin_Jobs crashed-job blacklist (getCrashedBedrockJobPatterns, onNodeLo
     <br/>An operational kill-switch/ops-safety feature layered onto job scheduling; a distinct concern from job CRUD though tightly coupled to GetJob(s) peek/process

**11. `version`** — priority 0.8, med, blast 0

   - version.h (whole file / SVERSION)
     <br/>SVERSION is never included or referenced anywhere else in the repo (only bedrockVersion.h's differently-named VERSION is actually used); appears to be dead code superseded by bedrockVersion.h

**12. `libstuff/SFastBuffer`** — priority 0.7, low, blast 8

   - SFastBuffer::startsWithHTTPRequest and its nextToCheck/headerLength state → `a small HTTP-framing helper layered on top of SFastBuffer, rather than inside it`
     <br/>bakes HTTP-specific framing (\r\n\r\n / \n\n terminator) knowledge into an otherwise protocol-agnostic buffer type
   - SFastBuffer::contentLength (private member)
     <br/>reset to 0 in four places but never read or set to any other value anywhere in the file - dead/vestigial field

**13. `libstuff/AutoScopeOnPrepare`** — priority 0.6, med, blast 1

   - AutoScopeOnPrepare (whole unit) → `sqlitecluster, alongside SQLite`
     <br/>a libstuff file whose sole purpose is scoping a sqlitecluster/SQLite feature (setOnPrepareHandler/enablePrepareNotifications), and it #includes sqlitecluster/SQLite.h directly - libstuff is meant to be generic and dependency-light, this is SQLite-specific

**14. `libstuff/JSON/Utils`** — priority 0.5, med, blast 2

   - Utils::recursiveReplaceJSONKeys
     <br/>doc comment is written entirely in terms of one caller's domain (bankAccounts.additionalData, apiResult, errorAttemptsCount, assetReport) and names an external test (GetWithdrawalAccountsTest) not present in this repo; the algorithm is generic but the documentation ties a package the README describes as application-agnostic to one consumer's onyx-merge use case

**15. `libstuff/AutoTimer`** — priority 0.5, med, blast 1

   - class AutoTimer
     <br/>the header's own comment notes a second, unrelated class also named AutoTimer exists in BedrockCore.h (a per-command timing guard) - same name, different purpose, genuinely confusable

**16. `libstuff/SQResultFormatter`** — priority 0.5, med, blast 2

   - utf8Next/isCombining/isWide/displayWidth (local lambdas in formatColumn) → `libstuff/SString or a new text-width utility unit`
     <br/>a generic UTF-8 display-width utility with no dependency on SQResult, trapped as local lambdas instead of being a reusable libstuff string-width helper
   - SQResultFormatter::FORMAT_OPTIONS::nullvalue / ::separator
     <br/>declared but never read by any formatXXX() implementation - dead configuration surface

**17. `sqlitecluster/SQLitePeer`** — priority 0.3, low, blast 5

   - SQLitePeer.h comment on `socket` member
     <br/>claims 'see friend class declaration above' but no friend is declared anywhere in this file - a stale/incorrect cross-reference left over from an earlier version

**18. `plugins/DB`** — priority 0.3, med, blast 1

   - BedrockPlugin_DB::Sqlite3QRFSpecWrapper / parseSQLite3Args / generateErrorContex → `libstuff/qrf.h or a new libstuff unit alongside it`
     <br/>a generic sqlite3-CLI argument-parsing and error-formatting layer around libstuff/qrf.h's C struct, none of it specific to being a request-command plugin

**19. `sqlitecluster/SQLiteCommand`** — priority 0.3, low, blast 3

   - SQLiteCommand::preprocessRequest's commandExecuteTime deprecation-warning branch
     <br/>deprecation-specific handling of one legacy wire field living inside an otherwise generic request-preprocessing helper

**20. `libstuff/SRingBuffer`** — priority 0.2, low, blast 2

   - State
     <br/>declared at global scope instead of nested inside SRingBuffer, injecting a generic five-letter name into every translation unit that includes this header

**21. `libstuff/SHTTPSProxySocket`** — priority 0.2, low, blast 2

   - private members proxyAddress, hostname, requestID, proxyNegotiationComplete, fil
     <br/>lack the repo's `_` prefix convention for private members, used elsewhere in this same batch (e.g. SStandaloneHTTPSManager's _pem, SSynchronizedQueue's _queue)

**22. `plugins/MySQL`** — priority 0.2, low, blast 3

   - g_MySQLVariables
     <br/>~300-row hardcoded table of fake AWS-RDS MySQL server variables/values; pure static data embedded in the plugin's header/cpp rather than an external data file, dominating the file's line count

**23. `libstuff/SSignal`** — priority 0.2, med, blast 0, **3 findings**

   - _SSignal_StackTrace: SQLiteNode::KILLABLE_SQLITE_NODE->kill()
     <br/>a generic libstuff crash handler directly reaching into the sqlitecluster layer to kill peer connections on crash
   - hardcoded "/tmp/bedrock_crash_{}.log" path
     <br/>bakes a product-specific filename into an otherwise general-purpose signal handler
   - no SSignal.h
     <br/>the whole public API is declared in libstuff.h instead of a dedicated header, unlike most libstuff pairs

**24. `BedrockBlockingCommandQueue`** — priority 0.2, low, blast 2

   - IdentifierState / StateMap / _recordAndCheck / _isBlocked → `libstuff, as a general-purpose rate limiter`
     <br/>a generic string-keyed sliding-window rate limiter with no dependency on BedrockCommand, implemented as private nested machinery of a command-queue subclass

**25. `main`** — priority 0.2, med, blast 0

   - loadPlugins → `BedrockPlugin.h/.cpp`
     <br/>a self-contained dlopen/dlsym plugin-registration subsystem implemented as a free function in main.cpp
   - VacuumDB / BackupDB / BACKUP_DIR
     <br/>shell out to the external `sqlite3` binary and hardcode a `/var/tmp/` backup path inside the process entry point; BACKUP_DIR is the only path in this file not configurable via args like everything else here

**26. `VMTouch`** — priority 0.1, low, blast 1

   - VMTouch (whole unit) → `libstuff/VMTouch.h or libstuff/VMTouch.cpp`
     <br/>A generic, Bedrock-agnostic OS/filesystem utility living at the repo root rather than under libstuff with the rest of the reusable helpers

**27. `plugins/Cache`** — priority 0.1, low, blast 1

   - BedrockPlugin_Cache::LRUMap → `libstuff (e.g. a generic SLRUMap/SLRUCache container)`
     <br/>a fully generic string-keyed LRU tracker with no cache-plugin-specific logic, nested privately inside one plugin instead of being a reusable libstuff container

**28. `BedrockCore`** — priority 0.1, low, blast 1

   - AutoScopeRewrite
     <br/>defined at file scope in BedrockCore.cpp without `static` or an anonymous namespace, so its file-local intent relies on convention rather than being enforced

**29. `libstuff/SLog`** — priority 0.1, low, blast 0

   - SLog.cpp's public API (SLogStackTrace, SWhitelistLogParams, SIsLogParamWhitelist → `libstuff/SLog.h`
     <br/>declared in the libstuff.h catch-all rather than a dedicated SLog.h, unlike other already-separated libstuff units

**30. `libstuff/STime`** — priority 0.1, low, blast 0

   - STime.cpp's public API → `libstuff/STime.h`
     <br/>declared in the libstuff.h catch-all rather than a dedicated STime.h, unlike other already-separated libstuff units


## Test findings

**1. `test/clustertest/BedrockClusterTester`** — priority 7.0, med, blast 36

   - ClusterTester<T>'s sized constructor: testplugin.so auto-detection block → `TestPlugin or a test-fixture setup helper, per the code's own TODO comment`
     <br/>plugin-specific special-casing (checking cwd for testplugin/testplugin.so) inside an otherwise generic cluster-bootstrap helper; the code's own comment says it 'should get moved somewhere else, really. Probably inside TestPlugin.'

**2. `test/clustertest/testplugin/ExternPointer`** — priority 0.5, low, blast 0

   - __pointerToFakeIntArray as its own translation unit → `test/clustertest/testplugin/TestPlugin.cpp`
     <br/>a single global exists as its own .cpp file; its only direct user in this batch is TestPlugin.cpp (extern-declared there), and the file's own comment references BadCommandTest, which only references the associated command names, not the symbol itself

**3. `test/clustertest/tests/FinishJobTest`** — priority 0.2, med, blast 0

   - FinishJobTest::negativeDelay / FinishJobTest::positiveDelay
     <br/>fully implemented test methods that are never registered via TEST(...) in the fixture's constructor, so they silently never run

**4. `test/tests/LibStuffTest`** — priority 0.1, med, blast 0

   - LibStuff::testUpperLower
     <br/>Fully written test method for SToUpper/SToLower is never added to the constructor's TEST(...) list, so it never executes; SToUpper/SToLower have no running coverage

**5. `test/clustertest/main`** — priority 0.1, low, blast 0

   - log()
     <br/>dead code - defined but never invoked from main; also a generic name that collides in intent with the repo's SINFO/SWARN logging macros despite doing something unrelated (execing into tail+grep)

**6. `test/tests/jobs/GetJobsTest`** — priority 0.1, low, blast 0

   - absoluteDiff, getAllJobData
     <br/>declared as plain file-scope free functions rather than static or in an anonymous namespace, unlike sibling test files which keep all helper logic inside the fixture struct; leaks symbol names into the test binary's global namespace

**7. `benchmarks/main`** — priority 0.1, low, blast 0

   - main (baseline-comparison branch) → `a wrapper shell script driving the plain benchmark binary twice`
     <br/>Shells out to git (stash/checkout/rev-parse) and reruns `make bench -j32` from inside the benchmark binary itself; this is build/source-control orchestration that reads more like a CI shell script than a benchmark runner

**8. `test/clustertest/tests/CompressionTest`** — priority 0.1, low, blast 0

   - CompressionTest::readDictionaryFile
     <br/>hardcodes two relative filesystem paths to locate the dictionary fixture depending on working directory, rather than using a shared test-fixture-path helper

**9. `test/clustertest/testplugin/TestPlugin`** — priority 0.1, low, blast 0

   - fileAppend / fileLockAndLoad
     <br/>generic flock-guarded file I/O helpers with no relation to plugin/command logic, defined as plain global functions in a plugin .cpp
   - BedrockPlugin_TestPlugin::arbitraryData / dataLock
     <br/>ad hoc static cross-command scratch storage that bypasses normal command/database plumbing entirely, held as globals for test convenience

**10. `test/tests/jobs/GetJobTest`** — priority 0.1, low, blast 0

   - isBetweenSecondsInclusive → `static/anonymous-namespace within this file, or lifted into a shared test-helper header such as test/tests/jobs/JobTestHelper.h if other job tests need it`
     <br/>plain (non-static, non-anonymous-namespace) global function in a .cpp linked into a large multi-file test binary; generic name risks an ODR clash with an identically-named helper in another test file, even though none exists today

