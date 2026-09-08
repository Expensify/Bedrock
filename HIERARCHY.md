# Bedrock — File Hierarchy
Authored code only. Excluded as third-party: `externalLib/rapidjson`, `libstuff/sqlite3.{c,h}`, `libstuff/qrf.c`, `mbedtls/` (submodule).

**152 units** (56 `.h`/`.cpp` pairs, 96 singles) — 61 source, 91 test — across 50,909 lines.

A *unit* is a class: `Foo.h` + `Foo.cpp` counted once.

```
Bedrock/
├── .github/  (0 units, 0 lines)
│   ├── actions/  (0 units, 0 lines)
│   │   └── composite/  (0 units, 0 lines)
│   │       └── download-binaries/  (0 units, 0 lines)
│   │           └── action.yml
│   ├── workflows/  (0 units, 0 lines)
│   │   └── bedrock.yml
│   └── PULL_REQUEST_TEMPLATE.md
├── benchmarks/  (5 units, 721 lines)
│   ├── BenchmarkBase.h  (154 L)
│   ├── SDeburrBench.cpp  (90 L)
│   ├── SReplaceAllBench.cpp  (100 L)
│   ├── SReplaceBench.cpp  (110 L)
│   ├── main.cpp  (267 L)
│   ├── Makefile
│   └── README.md
├── configs/  (0 units, 0 lines)
│   ├── bedrock.conf
│   ├── bedrock.init
│   ├── bedrock.service
│   └── bedrock_prerequisites.service
├── docker/  (0 units, 0 lines)
│   ├── libstuff/  (0 units, 0 lines)
│   │   └── bedrock.sh
│   ├── Dockerfile
│   ├── README.md
│   ├── build_and_extract.sh
│   └── docker-compose.yml
├── docs/  (0 units, 0 lines)
│   ├── _layouts/  (0 units, 0 lines)
│   │   └── default.html
│   ├── CNAME
│   ├── _config.yml
│   ├── blockchain.md
│   ├── cache.md
│   ├── cli.md
│   ├── db.md
│   ├── index.md
│   ├── jobs.md
│   ├── multizone.md
│   ├── mysql.md
│   ├── status.md
│   ├── synchronization.md
│   └── vs_mysql.md
├── libstuff/  (35 units, 14,492 lines)
│   ├── JSON/  (7 units, 3,820 lines)
│   │   ├── Metrics.{h,cpp}  (48 L)  [.h+.cpp]
│   │   ├── Parser.{h,cpp}  (69 L)  [.h+.cpp]
│   │   ├── SAXHandler.{h,cpp}  (269 L)  [.h+.cpp]
│   │   ├── Serializable.h  (52 L)
│   │   ├── Utils.{h,cpp}  (402 L)  [.h+.cpp]
│   │   ├── Value.{h,cpp}  (2630 L)  [.h+.cpp]
│   │   ├── Writer.{h,cpp}  (350 L)  [.h+.cpp]
│   │   └── README.md
│   ├── AutoScopeOnPrepare.{h,cpp}  (33 L)  [.h+.cpp]
│   ├── AutoTimer.{h,cpp}  (66 L)  [.h+.cpp]
│   ├── SData.{h,cpp}  (283 L)  [.h+.cpp]
│   ├── SDeburr.{h,cpp}  (277 L)  [.h+.cpp]
│   ├── SFastBuffer.{h,cpp}  (180 L)  [.h+.cpp]
│   ├── SFluentdLogger.{h,cpp}  (132 L)  [.h+.cpp]
│   ├── SHTTPSManager.{h,cpp}  (341 L)  [.h+.cpp]
│   ├── SHTTPSProxySocket.{h,cpp}  (176 L)  [.h+.cpp]
│   ├── SLog.cpp  (141 L)
│   ├── SMultiHostSocketPool.{h,cpp}  (48 L)  [.h+.cpp]
│   ├── SPerformanceTimer.{h,cpp}  (77 L)  [.h+.cpp]
│   ├── SQResult.{h,cpp}  (389 L)  [.h+.cpp]
│   ├── SQResultFormatter.{h,cpp}  (588 L)  [.h+.cpp]
│   ├── SQValue.{h,cpp}  (230 L)  [.h+.cpp]
│   ├── SQliteParameter.{h,cpp}  (174 L)  [.h+.cpp]
│   ├── SRandom.{h,cpp}  (57 L)  [.h+.cpp]
│   ├── SResolver.{h,cpp}  (146 L)  [.h+.cpp]
│   ├── SRingBuffer.h  (101 L)
│   ├── SSSLState.{h,cpp}  (269 L)  [.h+.cpp]
│   ├── SSignal.cpp  (307 L)
│   ├── SSocketPool.{h,cpp}  (168 L)  [.h+.cpp]
│   ├── SSynchronizedQueue.h  (175 L)
│   ├── STCPManager.{h,cpp}  (553 L)  [.h+.cpp]
│   ├── SThread.h  (66 L)
│   ├── STime.cpp  (138 L)
│   ├── libstuff.{h,cpp}  (4612 L)  [.h+.cpp]
│   ├── qrf.h  (200 L)
│   ├── sqlite3ext.h  (745 L)
│   └── README.md
├── plugins/  (5 units, 4,166 lines)
│   ├── Cache.{h,cpp}  (374 L)  [.h+.cpp]
│   ├── Compression.{h,cpp}  (414 L)  [.h+.cpp]
│   ├── DB.{h,cpp}  (490 L)  [.h+.cpp]
│   ├── Jobs.{h,cpp}  (1740 L)  [.h+.cpp]
│   ├── MySQL.{h,cpp}  (1148 L)  [.h+.cpp]
│   ├── Cache.md
│   └── Jobs.md
├── sqlitecluster/  (9 units, 6,677 lines)
│   ├── SQLite.{h,cpp}  (2374 L)  [.h+.cpp]
│   ├── SQLiteClusterMessenger.{h,cpp}  (430 L)  [.h+.cpp]
│   ├── SQLiteCommand.{h,cpp}  (154 L)  [.h+.cpp]
│   ├── SQLiteCore.{h,cpp}  (86 L)  [.h+.cpp]
│   ├── SQLiteNode.{h,cpp}  (2964 L)  [.h+.cpp]
│   ├── SQLitePeer.{h,cpp}  (425 L)  [.h+.cpp]
│   ├── SQLitePool.{h,cpp}  (179 L)  [.h+.cpp]
│   ├── SQLiteServer.h  (21 L)
│   ├── SQLiteUtils.{h,cpp}  (44 L)  [.h+.cpp]
│   └── index.markdown
├── test/  (86 units, 18,624 lines)
│   ├── clustertest/  (40 units, 6,054 lines)
│   │   ├── testplugin/  (2 units, 963 lines)
│   │   │   ├── ExternPointer.cpp  (2 L)
│   │   │   └── TestPlugin.{h,cpp}  (961 L)  [.h+.cpp]
│   │   ├── tests/  (36 units, 4,727 lines)
│   │   │   ├── AfterCommitCallbackClusterTest.cpp  (65 L)
│   │   │   ├── BadCommandTest.cpp  (127 L)
│   │   │   ├── BlockingQueueRateLimitTest.cpp  (143 L)
│   │   │   ├── BoundParametersTest.cpp  (141 L)
│   │   │   ├── BroadcastTest.cpp  (87 L)
│   │   │   ├── ClusterUpgradeTest.cpp  (243 L)
│   │   │   ├── CompressionTest.cpp  (279 L)
│   │   │   ├── ConflictSpamTest.cpp  (295 L)
│   │   │   ├── ControlCommandTest.cpp  (53 L)
│   │   │   ├── DoubleDetachTest.cpp  (52 L)
│   │   │   ├── EscalateTest.cpp  (107 L)
│   │   │   ├── FastStandDownTest.cpp  (120 L)
│   │   │   ├── FinishJobTest.cpp  (605 L)
│   │   │   ├── ForkCheckTest.cpp  (139 L)
│   │   │   ├── FutureExecutionTest.cpp  (87 L)
│   │   │   ├── GracefulFailoverTest.cpp  (228 L)
│   │   │   ├── HTTPSBlockingCommitTest.cpp  (99 L)
│   │   │   ├── HTTPSTest.cpp  (123 L)
│   │   │   ├── JobIDTest.cpp  (97 L)
│   │   │   ├── LeadingTest.cpp  (190 L)
│   │   │   ├── MassiveQueryTest.cpp  (53 L)
│   │   │   ├── MultipleLeaderSyncTest.cpp  (187 L)
│   │   │   ├── PermafollowerTest.cpp  (53 L)
│   │   │   ├── PrePeekPostProcessTest.cpp  (91 L)
│   │   │   ├── SetPriorityTest.cpp  (210 L)
│   │   │   ├── StatusHandlingCommandsTest.cpp  (60 L)
│   │   │   ├── StatusTest.cpp  (63 L)
│   │   │   ├── SynchronousCommandsTest.cpp  (66 L)
│   │   │   ├── ThreadExceptionTest.cpp  (23 L)
│   │   │   ├── TimeoutTest.cpp  (249 L)
│   │   │   ├── TimingTest.cpp  (111 L)
│   │   │   ├── UniqueConstraintsTest.cpp  (21 L)
│   │   │   ├── UpgradeDBTest.cpp  (51 L)
│   │   │   ├── UpgradeTest.cpp  (62 L)
│   │   │   ├── VersionMismatchTest.cpp  (70 L)
│   │   │   └── WriteLocalUnreplicatedClusterTest.cpp  (77 L)
│   │   ├── BedrockClusterTester.h  (252 L)
│   │   └── main.cpp  (112 L)
│   ├── lib/  (6 units, 2,300 lines)
│   │   ├── BedrockTester.{h,cpp}  (876 L)  [.h+.cpp]
│   │   ├── PortMap.{h,cpp}  (107 L)  [.h+.cpp]
│   │   ├── PrintEquality.h  (54 L)
│   │   ├── RemoteSQLite.{h,cpp}  (138 L)  [.h+.cpp]
│   │   ├── TestHTTPS.{h,cpp}  (47 L)  [.h+.cpp]
│   │   └── tpunit++.{h,cpp}  (1078 L)  [.h+.cpp]
│   ├── sample_data/  (0 units, 0 lines)
│   │   ├── journal.dict
│   │   └── lottoNumbers.json
│   ├── tests/  (39 units, 10,143 lines)
│   │   ├── jobs/  (14 units, 4,402 lines)
│   │   │   ├── CancelJobTest.cpp  (393 L)
│   │   │   ├── CreateJobTest.cpp  (620 L)
│   │   │   ├── CreateJobsTest.cpp  (266 L)
│   │   │   ├── DeleteJobTest.cpp  (200 L)
│   │   │   ├── FailJobTest.cpp  (126 L)
│   │   │   ├── FailedJobReplyTest.cpp  (173 L)
│   │   │   ├── GetJobTest.cpp  (971 L)
│   │   │   ├── GetJobsTest.cpp  (130 L)
│   │   │   ├── InifiniteRetryAfterTest.cpp  (106 L)
│   │   │   ├── JobTestHelper.{h,cpp}  (17 L)  [.h+.cpp]
│   │   │   ├── QueryJobTest.cpp  (62 L)
│   │   │   ├── RequeueJobsTest.cpp  (282 L)
│   │   │   ├── RetryJobTest.cpp  (903 L)
│   │   │   └── UpdateJobTest.cpp  (153 L)
│   │   ├── AfterCommitCallbackTest.cpp  (123 L)
│   │   ├── AsyncResolveTest.cpp  (206 L)
│   │   ├── BlockingCommandQueueTest.cpp  (313 L)
│   │   ├── ChainedHTTPTest.cpp  (69 L)
│   │   ├── CommandPortTest.cpp  (42 L)
│   │   ├── FastHTTPParsing.cpp  (107 L)
│   │   ├── JSONParserTest.cpp  (189 L)
│   │   ├── JSONTest.cpp  (139 L)
│   │   ├── JSONUtilsTest.{h,cpp}  (453 L)  [.h+.cpp]
│   │   ├── JSONValueTest.cpp  (862 L)
│   │   ├── LibStuffTest.cpp  (1283 L)
│   │   ├── MySQLTest.cpp  (157 L)
│   │   ├── MySQLUtilsTest.cpp  (285 L)
│   │   ├── QueryTest.cpp  (130 L)
│   │   ├── ReadTest.cpp  (52 L)
│   │   ├── SDeburrTest.cpp  (86 L)
│   │   ├── SFluentdLoggerTest.cpp  (136 L)
│   │   ├── SIsValidSQLiteDateModifierTest.cpp  (75 L)
│   │   ├── SQLiteNodeTest.cpp  (170 L)
│   │   ├── SRingBufferTest.cpp  (339 L)
│   │   ├── SSLTest.cpp  (97 L)
│   │   ├── STimeTest.cpp  (42 L)
│   │   ├── StatusTest.cpp  (21 L)
│   │   ├── WriteLocalUnreplicatedTest.cpp  (146 L)
│   │   └── WriteTest.cpp  (219 L)
│   ├── main.cpp  (127 L)
│   └── Makefile
├── BedrockBlockingCommandQueue.{h,cpp}  (354 L)  [.h+.cpp]
├── BedrockCommand.{h,cpp}  (879 L)  [.h+.cpp]
├── BedrockCommandQueue.{h,cpp}  (315 L)  [.h+.cpp]
├── BedrockConflictManager.{h,cpp}  (91 L)  [.h+.cpp]
├── BedrockCore.{h,cpp}  (575 L)  [.h+.cpp]
├── BedrockPlugin.{h,cpp}  (212 L)  [.h+.cpp]
├── BedrockServer.{h,cpp}  (2972 L)  [.h+.cpp]
├── ConflictLockGuard.{h,cpp}  (126 L)  [.h+.cpp]
├── VMTouch.{h,cpp}  (232 L)  [.h+.cpp]
├── bedrockVersion.h  (12 L)
├── main.cpp  (457 L)
├── version.h  (4 L)
├── .clang-format
├── .clangd.example
├── .gitattributes
├── .gitignore
├── .gitmodules
├── .uncrustify.cfg
├── COPYING
├── Makefile
├── README.md
├── ci_build.sh
├── ci_style.sh
├── ci_tests.sh
├── ci_utils.sh
├── class_hierarchy.md
├── expensify.ca.crt.enc
├── format.sh
├── mbedtls
├── style.sh
└── use_clang
```
