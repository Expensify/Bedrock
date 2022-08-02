/// bedrock/main.cpp
/// =================
/// Process entry point for Bedrock server.
///
#include <dlfcn.h>
#include <iostream>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <bedrockVersion.h>
#include <BedrockServer.h>
#include <BedrockPlugin.h>
#include <plugins/Cache.h>
#include <plugins/DB.h>
#include <plugins/Jobs.h>
#include <plugins/MySQL.h>
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
void RetrySystem(const string& command) {
    // We might be waiting for some threads to unlink, so retry a few times
    int numRetries = 3;
    SINFO("Trying to run '" << command << "' up to " << numRetries << " times...");
    while (numRetries--) {
        // Try it and see if it works
        int returnCode = system(command.c_str());
        if (returnCode) {
            // Didn't work
            SWARN("'" << command << "' failed with return code " << returnCode << ", waiting 5s and retrying "
                      << numRetries << " more times");
            this_thread::sleep_for(chrono::seconds(5));
        } else

        {
            // Done!
            SINFO("Successfully ran '" << command << "'");
            return;
        }
    }

    // Didn't work -- fatal error
    SERROR("Failed to run '" << command << "', aborting.");
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

void VacuumDB(const string& db) { RetrySystem("sqlite3 " + db + " 'VACUUM;'"); }

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#define BACKUP_DIR "/var/tmp/"
void BackupDB(const string& dbPath) {
    const string& dbFile = string(basename((char*)dbPath.c_str()));
    SINFO("Starting " << dbFile << " database backup.");
    SASSERT(SFileCopy(dbPath, BACKUP_DIR + dbFile));
    SINFO("Finished " << dbFile << " database backup.");

    const string& dbWalPath = dbPath + "-wal";
    SINFO("Checking for existence of " << dbWalPath);
    if (SFileExists(dbWalPath)) {
        SALERT("WAL file exists for " << dbFile << ". Backing up");
        SASSERT(SFileCopy(dbWalPath, BACKUP_DIR + string(basename((char*)dbWalPath.c_str()))));
        SINFO("Finished " << dbFile << "-wal database backup.");
    }

    const string& dbShmPath = dbPath + "-shm";
    SINFO("Checking for existence of " << dbShmPath);
    if (SFileExists(dbShmPath)) {
        SALERT("SHM file exists for " << dbFile << ". Backing up");
        SASSERT(SFileCopy(dbShmPath, BACKUP_DIR + string(basename((char*)dbShmPath.c_str()))));
        SINFO("Finished " << dbFile << "-shm database backup.");
    }
}


set<string> loadPlugins(SData& args) {
    list<string> plugins = SParseList(args["-plugins"]);

    // We'll return the names of the plugins we've loaded, which don't necessarily match the file names we're passed.
    // Those are stored here.
    set <string> postProcessedNames;

    // Register all of our built-in plugins.
    BedrockPlugin::g_registeredPluginList.emplace(make_pair("DB", [](BedrockServer& s){return new BedrockPlugin_DB(s);}));
    BedrockPlugin::g_registeredPluginList.emplace(make_pair("JOBS", [](BedrockServer& s){return new BedrockPlugin_Jobs(s);}));
    BedrockPlugin::g_registeredPluginList.emplace(make_pair("CACHE", [](BedrockServer& s){return new BedrockPlugin_Cache(s);}));
    BedrockPlugin::g_registeredPluginList.emplace(make_pair("MYSQL", [](BedrockServer& s){return new BedrockPlugin_MySQL(s);}));

    for (string pluginName : plugins) {
        // If it's one of our standard plugins, just move on to the next one.
        if (BedrockPlugin::g_registeredPluginList.find(SToUpper(pluginName)) != BedrockPlugin::g_registeredPluginList.end()) {
            postProcessedNames.emplace(SToUpper(pluginName));
            continue;
        }

        // Any non-standard plugin is loaded from a shared library. If a name is passed without a trailing '.so', we
        // will add it, and look for a file with that name. A file should be passed with either a complete absolute
        // path, or the file should exist in a place that dlopen() can find it (like, /usr/lib).

        // We look for the 'base name' of the plugin. I.e., the filename excluding a path or extension. We'll look for
        // a symbol based on this name to call to instantiate our plugin.
        size_t slash = pluginName.rfind('/');
        size_t dot = pluginName.find('.', slash);
        string name = pluginName.substr(slash + 1, dot - slash - 1);
        string symbolName = "BEDROCK_PLUGIN_REGISTER_" + SToUpper(name);

        // Save the base name of the plugin.
        if(postProcessedNames.find(SToUpper(name)) != postProcessedNames.end()) {
            SWARN("Duplicate entry for plugin " << name << ", skipping.");
            continue;
        }
        postProcessedNames.insert(SToUpper(name));

        // Add the file extension if it's missing.
        if (!SEndsWith(pluginName, ".so")) {
            pluginName += ".so";
        }

        // Open the library.
        void* lib = dlopen(pluginName.c_str(), RTLD_NOW);
        if(!lib) {
            SWARN("Error loading bedrock plugin " << pluginName << ": " << dlerror());
        } else {
            void* sym = dlsym(lib, symbolName.c_str());
            if (!sym) {
                SWARN("Couldn't find symbol " << symbolName);
            } else {
                // Call the plugin registration function with the same name.
                BedrockPlugin::g_registeredPluginList.emplace(make_pair(SToUpper(name), (BedrockPlugin*(*)(BedrockServer&))sym));
            }
        }
    }

    return postProcessedNames;
}

/////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    // Process the command line
    SData args = SParseCommandLine(argc, argv);
    if (args.empty()) {
        // It's valid to run bedrock with no parameters provided, but unusual
        // -- let's provide some help just in case
        cout << "Protip: check syslog for details, or run 'bedrock -?' for help" << endl;
    }

    // Initialize the sqlite library before any other code has a chance to do anything with it.
    // Set the logging callback for sqlite errors.
    SASSERT(sqlite3_config(SQLITE_CONFIG_LOG, SQLite::_sqliteLogCallback, 0) == SQLITE_OK);

    // Enable memory-mapped files.
    int64_t mmapSizeGB = args.isSet("-mmapSizeGB") ? stoll(args["-mmapSizeGB"]) : 0;
    if (mmapSizeGB) {
        SINFO("Enabling Memory-Mapped I/O with " << mmapSizeGB << " GB.");
        const int64_t GB = 1024 * 1024 * 1024;
        SASSERT(sqlite3_config(SQLITE_CONFIG_MMAP_SIZE, mmapSizeGB * GB, 16 * 1024 * GB) == SQLITE_OK); // Max is 16TB
    }

    // Disable a mutex around `malloc`, which is *EXTREMELY IMPORTANT* for multi-threaded performance. Without this
    // setting, all reads are essentially single-threaded as they'll all fight with each other for this mutex.
    SASSERT(sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0) == SQLITE_OK);
    sqlite3_initialize();
    SASSERT(sqlite3_threadsafe());

    // Disabled by default, but lets really beat it in. This way checkpointing does not need to wait on locks
    // created in this thread.
    SASSERT(sqlite3_enable_shared_cache(0) == SQLITE_OK);

    // Fork if requested
    if (args.isSet("-fork")) {
        // Do the fork
        int pid = fork();
        SASSERT(pid >= 0);
        if (pid > 0) {
            // Successful fork -- write the pidfile (if requested) and exit
            if (args.isSet("-pidfile"))
                SASSERT(SFileSave(args["-pidfile"], SToStr(pid)));
            return 0;
        }

        // Daemonize
        // **NOTE: See http://www-theorie.physik.unizh.ch/~dpotter/howto/daemonize
        umask(0);
        SASSERT(setsid() >= 0);
        SASSERT(chdir("/") >= 0);
        if (!freopen("/dev/null", "r", stdin) ||
            !freopen("/dev/null", "w", stdout) ||
            !freopen("/dev/null", "w", stderr)
        ) {
            cout << "Couldn't daemonize." << endl;
            return -1;
        }
    }

    if (args.isSet("-version")) {
        // Just output the version
        cout << VERSION << endl;
        return 0;
    }
    if (args.isSet("-h") || args.isSet("-?") || args.isSet("-help")) {
        // Ouput very basic documentation
        cout << "Usage:" << endl;
        cout << "------" << endl;
        cout << "bedrock [-? | -h | -help]" << endl;
        cout << "bedrock -version" << endl;
        cout << "bedrock [-clean] [-v] [-db <filename>] [-serverHost <host:port>] [-nodeHost <host:port>] [-nodeName "
                "<name>] [-peerList <list>] [-priority <value>] [-plugins <list>] [-cacheSize <kb>] [-workerThreads <#>] "
                "[-versionOverride <version>]"
             << endl;
        cout << endl;
        cout << "Common Commands:" << endl;
        cout << "----------------" << endl;
        cout << "-?, -h, -help               Outputs instructions and exits" << endl;
        cout << "-version                    Outputs version and exits" << endl;
        cout << "-v                          Enables verbose logging" << endl;
        cout << "-q                          Enables quiet logging" << endl;
        cout << "-clean                      Recreate a new database from scratch" << endl;
        cout << "-enableMultiWrite           Enable multi-write mode (default: true)" << endl;
        cout << "-versionOverride <version>  Pretends to be a different version when talking to peers" << endl;
        cout << "-db             <filename>  Use a database with the given name (default 'bedrock.db')" << endl;
        cout
            << "-serverHost     <host:port> Listen on this host:port for cluster connections (default 'localhost:8888')"
            << endl;
        cout << "-nodeName       <name>      Name this specfic node in the cluster as indicated (defaults to '"
             << SGetHostName() << "')" << endl;
        cout << "-nodeHost       <host:port> Listen on this host:port for connections from other nodes" << endl;
        cout << "-peerList       <list>      See below" << endl;
        cout << "-priority       <value>     See '-peerList Details' below (defaults to 100)" << endl;
        cout << "-plugins        <list>      Enable these plugins (defaults to 'db,jobs,cache,mysql')" << endl;
        cout << "-cacheSize      <kb>        number of KB to allocate for a page cache (defaults to 1GB)" << endl;
        cout << "-workerThreads  <#>         Number of worker threads to start (min 1, defaults to # of cores)" << endl;
        cout << "-queryLog       <filename>  Set the query log filename (default 'queryLog.csv', SIGUSR2/SIGQUIT to "
                "enable/disable)"
             << endl;
        cout << "-maxJournalSize <#commits>  Number of commits to retain in the historical journal (default 1000000)"
             << endl;
        cout << "-synchronous    <value>     Set the PRAGMA schema.synchronous "
                "(defaults see https://sqlite.org/pragma.html#pragma_synchronous)"
             << endl;
        cout << endl;
        cout << "Quick Start Tips:" << endl;
        cout << "-----------------" << endl;
        cout << "In a hurry?  Just run 'bedrock -clean' the first time, and it'll create a new database called "
                "'bedrock.db', then use all the defaults listed above.  (After the first time, leave out the '-clean' "
                "to reuse the same database.)  Once running, you can verify it's working using NetCat to manualy send "
                "a Ping request as follows:"
             << endl;
        cout << endl;
        cout << "$ bedrock -clean &" << endl;
        cout << "$ nc local 8888" << endl;
        cout << "Ping" << endl;
        cout << endl;
        cout << "200 OK" << endl;
        cout << endl;
        cout << "-peerList Details:" << endl;
        cout << "------------------" << endl;
        cout << "The -peerList parameter enables you to configure multiple Bedrock nodes into a redundant cluster.  "
                "Bedrock supports any number of nodes: simply start each node with a comma-separated list of the "
                "'-nodeHost' of all other nodes.  You can safely send any command to any node.  Some best practices:"
             << endl;
        cout << endl;
        cout << "- Put each Bedrock node on a different server." << endl;
        cout << endl;
        cout << "- Assign each node a different priority (greater than 0).  The highest priority node will be the "
                "'leader', which will coordinate distributed transactions."
             << endl;
        cout << endl;
        return 1;
    }

    // Start libstuff. Generally, we want to initialize libstuff immediately on any new thread, but we wait until after
    // the `fork` above has completed, as we can get strange behaviors from signal handlers across forked processes.
    SInitialize("main", (args.isSet("-overrideProcessName") ? args["-overrideProcessName"].c_str() : 0));
    SLogLevel(LOG_INFO);

    if (args.isSet("-v")) {
        // Verbose logging
        SINFO("Enabling verbose logging");
        SLogLevel(LOG_DEBUG);
    } else if (args.isSet("-q")) {
        // Quiet logging
        SLogLevel(LOG_WARNING);
    }

// Set the defaults
#define SETDEFAULT(_NAME_, _VAL_)                                                                                      \
    do {                                                                                                               \
        if (!args.isSet(_NAME_))                                                                                       \
            args[_NAME_] = _VAL_;                                                                                      \
    } while (false)
    SETDEFAULT("-db", "bedrock.db");
    SETDEFAULT("-serverHost", "localhost:8888");
    SETDEFAULT("-nodeHost", "localhost:8889");
    SETDEFAULT("-commandPortPrivate", "localhost:8890");
    SETDEFAULT("-controlPort", "localhost:9999");
    SETDEFAULT("-nodeName", SGetHostName());
    SETDEFAULT("-cacheSize", SToStr(1024 * 1024)); // 1024 * 1024KB = 1GB.
    SETDEFAULT("-plugins", "db,jobs,cache,mysql");
    SETDEFAULT("-priority", "100");
    SETDEFAULT("-maxJournalSize", "1000000");
    SETDEFAULT("-queryLog", "queryLog.csv");
    SETDEFAULT("-enableMultiWrite", "true");

    args["-plugins"] = SComposeList(loadPlugins(args));

    // Reset the database if requested
    if (args.isSet("-clean")) {
        // Remove it
        SDEBUG("Resetting database");
        string db = args["-db"];
        unlink(db.c_str());
    } else if (args.isSet("-bootstrap")) {
        // Allow for bootstraping a node with no database file in place.
        SINFO("Loading in bootstrap mode, skipping check for database existance.");
    } else {
        // Otherwise verify the database exists
        SDEBUG("Verifying database exists");
        SASSERT(SFileExists(args["-db"]));
    }

    // Set our soft limit to the same as our hard limit to allow for more file handles.
    struct rlimit limits;
    if (!getrlimit(RLIMIT_NOFILE, &limits)) {
        limits.rlim_cur = limits.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &limits)) {
            SERROR("Couldn't set FD limit");
        }
    } else {
        SERROR("Couldn't get FD limit");
    }

    // Log stack traces if we have unhandled exceptions.
    set_terminate(STerminateHandler);

    // Create our BedrockServer object so we can keep it for the life of the
    // program.
    SINFO("Starting bedrock server");
    BedrockServer* _server = new BedrockServer(args);
    BedrockServer& server = *_server;

    // Keep going until someone kills it (either via TERM or Control^C)
    while (!(SGetSignal(SIGTERM) || SGetSignal(SIGINT))) {
        if (SGetSignals()) {
            // Log and clear any outstanding signals.
            SALERT("Uncaught signals (" << SGetSignalDescription() << "), ignoring.");
            SClearSignals();
        }

        // Counters for seeing how long we spend in postPoll.
        chrono::steady_clock::duration pollCounter(0);
        chrono::steady_clock::duration postPollCounter(0);
        chrono::steady_clock::time_point start = chrono::steady_clock::now();

        uint64_t nextActivity = STimeNow();
        while (!server.shutdownComplete()) {
            if (server.shouldBackup() && server.isDetached()) {
                BackupDB(args["-db"]);
                server.setDetach(false);
            }
            // Wait and process
            fd_map fdm;
            server.prePoll(fdm);
            const uint64_t now = STimeNow();
            auto timeBeforePoll = chrono::steady_clock::now();
            S_poll(fdm, max(nextActivity, now) - now);
            nextActivity = STimeNow() + STIME_US_PER_S; // 1s max period
            auto timeAfterPoll = chrono::steady_clock::now();
            server.postPoll(fdm, nextActivity);
            auto timeAfterPostPoll = chrono::steady_clock::now();

            pollCounter += timeAfterPoll - timeBeforePoll;
            postPollCounter += timeAfterPostPoll - timeAfterPoll;

            // Every 10s, log and reset.
            if (timeAfterPostPoll > (start + 10s)) {
                SINFO("[performance] main poll loop timing: "
                      << chrono::duration_cast<chrono::milliseconds>(timeAfterPostPoll - start).count() << " ms elapsed. "
                      << chrono::duration_cast<chrono::milliseconds>(pollCounter).count() << " ms in poll. "
                      << chrono::duration_cast<chrono::milliseconds>(postPollCounter).count() << " ms in postPoll.");
                pollCounter = chrono::microseconds::zero();
                postPollCounter = chrono::microseconds::zero();
                start = timeAfterPostPoll;
            }
        }
        if (server.shutdownWhileDetached) {
            // We need to actually shut down here.
            break;
        }
    }

    SINFO("Deleting BedrockServer");
    delete _server;
    SINFO("BedrockServer deleted");

    // Finished with our signal handler.
    SStopSignalThread();

    // All done
    SINFO("Graceful process shutdown complete");
    return 0;
}
