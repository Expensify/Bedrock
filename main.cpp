/// bedrock/main.cpp
/// =================
/// Process entry point for Bedrock server.
///
#include <libstuff/libstuff.h>
#include <libstuff/version.h>
#include "BedrockServer.h"
#include "BedrockPlugin.h"
#include "plugins/Cache.h"
#include "plugins/DB.h"
#include "plugins/Jobs.h"
#include "plugins/MySQL.h"
#include <sys/stat.h> // for umask()
#include <dlfcn.h>

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

    // Instantiate all of our built-in plugins.
    map<string, BedrockPlugin*> standardPluginMap = {
        {"DB",     new BedrockPlugin_DB()},
        {"JOBS",   new BedrockPlugin_Jobs()},
        {"CACHE",  new BedrockPlugin_Cache()},
        {"MYSQL",  new BedrockPlugin_MySQL()}
    };

    for (string pluginName : plugins) {
        // If it's one of our standard plugins, pass it's name through to postProcessedNames and move on.
        if (standardPluginMap.find(SToUpper(pluginName)) != standardPluginMap.end()) {
            postProcessedNames.insert(SToUpper(pluginName));
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
            cout << dlerror() << endl;
        } else {
            void* sym = dlsym(lib, symbolName.c_str());
            if (!sym) {
                cout << "Couldn't find symbol " << symbolName << endl;
            } else {
                // Call the plugin registration function with the same name.
                ((void(*)()) sym)();
            }
        }
    }

    return postProcessedNames;
}

/////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    // Start libstuff
    SInitialize("main");
    SLogLevel(LOG_INFO);

    // Process the command line
    SData args = SParseCommandLine(argc, argv);
    if (args.empty()) {
        // It's valid to run bedrock with no parameters provided, but unusual
        // -- let's provide some help just in case
        cout << "Protip: check syslog for details, or run 'bedrock -?' for help" << endl;
    }
    if (args.isSet("-version")) {
        // Just output the version
        cout << SVERSION << endl;
        return 1;
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
        cout << "-plugins        <list>      Enable these plugins (defaults to 'status,db,jobs,cache')" << endl;
        cout << "-cacheSize      <kb>        number of KB to allocate for a page cache (defaults to 1GB)" << endl;
        cout << "-workerThreads  <#>         Number of worker threads to start (min 1, defaults to # of cores)" << endl;
        cout << "-queryLog       <filename>  Set the query log filename (default 'queryLog.csv', SIGUSR2/SIGQUIT to "
                "enable/disable)"
             << endl;
        cout << "-maxJournalSize <#commits>  Number of commits to retain in the historical journal (default 1000000)"
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
                "'master', which will coordinate distributed transactions."
             << endl;
        cout << endl;
        return 1;
    }
    if (args.isSet("-v")) {
        // Verbose logging
        SINFO("Enabling verbose logging");
        SLogLevel(LOG_DEBUG);
    } else if (args.isSet("-q")) {
        // Quiet logging
        SLogLevel(LOG_WARNING);
    }

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
        freopen("/dev/null", "r", stdin);
        freopen("/dev/null", "w", stdout);
        freopen("/dev/null", "w", stderr);
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
    SETDEFAULT("-nodeName", SGetHostName());
    SETDEFAULT("-cacheSize", SToStr(1024 * 1024)); // 1024 * 1024KB = 1GB.
    SETDEFAULT("-plugins", "status,db,jobs,cache");
    SETDEFAULT("-priority", "100");
    SETDEFAULT("-maxJournalSize", "1000000");
    SETDEFAULT("-queryLog", "queryLog.csv");

    args["-plugins"] = SComposeList(loadPlugins(args));

    // Reset the database if requested
    if (args.isSet("-clean")) {
        // Remove it
        SDEBUG("Resetting database");
        string db = args["-db"];
        unlink(db.c_str());
    } else {
        // Otherwise verify the database exists
        SDEBUG("Verifying database exists");
        SASSERT(SFileExists(args["-db"]));
    }

    // Keep going until someone kills it (either via TERM or Control^C)
    while (!(SCatchSignal(SIGTERM) || SCatchSignal(SIGINT))) {
        // Log any uncaught signals
        if (SGetSignals()) {
            // Log and clear
            SALERT("Uncaught exceptions (" << SGetSignalNames(SGetSignals()) << "), ignoring.");
            SClearSignals();
        }

        // Make sure the BedrockServer is destroyed before VACUUM so it lets go of the db files.
        {
            // Run the server
            SINFO("Starting bedrock server");
            BedrockServer server(args);
            uint64_t nextActivity = STimeNow();
            while (!server.shutdownComplete()) {
                // Wait and process
                fd_map fdm;
                server.prePoll(fdm);
                const uint64_t now = STimeNow();
                S_poll(fdm, max(nextActivity, now) - now);
                nextActivity = STimeNow() + STIME_US_PER_S; // 1s max period
                server.postPoll(fdm, nextActivity);
            }
            SINFO("Graceful bedrock shutdown complete");
        }

        // Vacuum on USR1 signal.
        if (SCatchSignal(SIGUSR1)) {
            // Vacuum and analyze the database
            VacuumDB(args["-db"]);
            SINFO("Starting main analyze.");
            RetrySystem("sqlite3 " + args["-db"] + " 'ANALYZE;'");
            SINFO("Finished main analyze.");
        }

        // Checkpoint databases on USR2 signal.
        if (SCatchSignal(SIGUSR2)) {
            // Cleanup the wal file.
            // Note, we get out of control growth in wal files sometimes.
            // The sqlite3 tool cleans it up if you simply run a query.
            const string& cmdCheckpointMainWal = "sqlite3 " + args["-db"] + " 'SELECT * FROM accounts LIMIT 1;'";
            SINFO("Starting main wal checkpoint. WAL filesize="
                  << SToStr(SFileSize(args["-db"] + "-wal") / (float)(1024 * 1024)) << " MB");
            RetrySystem(cmdCheckpointMainWal);
            SINFO("Done with checkpointing.");
        }

        // Database backup on HUP signal.
        if (SCatchSignal(SIGHUP)) {
            // Backup the main database
            const string& mainDB = args["-db"];
            BackupDB(mainDB);
        }

        // Analyze indicies on TSTP signal
        if (SCatchSignal(SIGTSTP)) {
            SINFO("Starting main analyze.");
            RetrySystem("sqlite3 " + args["-db"] + " 'ANALYZE;'");
            SINFO("Finished main analyze.");
        }
    }

    // Log how much time we spent in our main mutex.
    SQLite::g_commitLock.log();
    // All done
    SINFO("Graceful process shutdown complete");
    return 0;
}
