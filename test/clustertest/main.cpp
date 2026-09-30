#include <iostream>
#include <memory>
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>
#include <test/lib/BedrockTestPath.h>
#include <test/lib/ConsoleOutputWriter.h>
#include <test/output/NcursesOutputWriter.h>

/*
 * This is based on the 'test' application in the parent directory to this one, but specifically aims to test the
 * redundancy and replication features of bedrock. As such, the intention is to not shut down servers and clean the
 * database between tests, and so in all likelihood, an early failure will carry over into later failures in
 * subsequent tests. This is expected as we want to verify overall database integrity rather than unit test individual
 * bits of functionality.
 */

static tpunit::NcursesOutputWriter* activeOutput = nullptr;

void sigclean(int sig)
{
    if (activeOutput) {
        activeOutput->finish();
    }
    cout << "Got SIGINT, cleaning up." << endl;
    BedrockTester::stopAll();
    cout << "Done." << endl;
    exit(1);
}

// This is a bit of a hack and assumes various things about syslogd
void log()
{
    int pid = getpid();
    cout << "Starting log recording with pid: " << pid << endl;
    unlink("log.txt");
    execl("/bin/bash", "/bin/bash", "-c", "tail -f /var/log/syslog | grep --line-buffered bedrock > log.txt", (char*) NULL);
}

int main(int argc, char* argv[])
{
    configureBedrockTestPath();
    SData args = SParseCommandLine(argc, argv);

    // Catch sigint.
    signal(SIGINT, sigclean);

    set<string> include;
    set<string> exclude;
    list<string> before;
    list<string> after;
    int threads = 1;
    int repeatCount = 1;

    if (args.isSet("-repeatCount")) {
        repeatCount = max(1, SToInt(args["-repeatCount"]));
    }

    if (args.isSet("-only")) {
        list<string> includeList = SParseList(args["-only"]);
        for (string name : includeList) {
            include.insert(name);
        }
    }
    if (args.isSet("-except")) {
        list<string> excludeList = SParseList(args["-except"]);
        for (string name : excludeList) {
            exclude.insert(name);
        }
    }
    if (args.isSet("-before")) {
        list<string> beforeList = SParseList(args["-before"]);
        for (string name : beforeList) {
            before.push_back(name);
        }
    }
    if (args.isSet("-after")) {
        list<string> afterList = SParseList(args["-after"]);
        for (string name : afterList) {
            after.push_back(name);
        }
    }
    if (args.isSet("-threads")) {
        threads = SToInt(args["-threads"]);
    }

    // Enable HCTree for the tests
    if (args.isSet("-enableHctree")) {
        BedrockTester::ENABLE_HCTREE = true;
        cout << "HCTree enabled" << endl;
    }

    SLogLevel(LOG_INFO);
    if (args.isSet("-v")) {
        BedrockTester::VERBOSE_LOGGING = true;
        SLogLevel(LOG_DEBUG);
    }
    if (args.isSet("-q")) {
        BedrockTester::QUIET_LOGGING = true;
        SLogLevel(LOG_WARNING);
    }

    int retval = 0;
    tpunit::ConsoleOutputWriter outputWriter(args.isSet("-v"));
    unique_ptr<tpunit::NcursesOutputWriter> newOutput;
    if (args.isSet("-useNewOutput") && tpunit::NcursesOutputWriter::available()) {
        try {
            newOutput = make_unique<tpunit::NcursesOutputWriter>();
            activeOutput = newOutput.get();
        } catch (const runtime_error& error) {
            cerr << "New test output unavailable: " << error.what() << ". Using console output." << endl;
        }
    }
    auto initThread = []() {
    };
    {
        for (int i = 0; i < repeatCount; i++) {
            try {
                retval = tpunit::Tests::run(include, exclude, before, after, threads, initThread, &tpunit::_TestFixture::sorter,
                                            newOutput ? static_cast<tpunit::OutputWriter*>(newOutput.get()) : &outputWriter);
            } catch (...) {
                cout << "Unhandled exception running tests!" << endl;
                retval = 1;
            }
        }
    }

    SStopSignalThread();

    if (newOutput) {
        newOutput->finish();
        activeOutput = nullptr;
    }

    // Tester gets destroyed here. Everything's done.
    return retval;
}
