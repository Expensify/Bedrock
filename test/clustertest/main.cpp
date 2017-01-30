#include <libstuff/libstuff.h>
#include <test/clustertest/BedrockClusterTester.h>

/*
 * This is based on the 'test' application in the parent directory to this one, but specifically aims to test the
 * redundancy and replication features of bedrock. As such, the intention is to not shut down servers and clean the
 * database between tests, and so in all likelihood, an early failure will carry over into later failures in
 * subsequent tests. This is expected as we want to verify overall database integrity rather than unit test individual
 * bits of functionality.
 */

void cleanup() {
    if (BedrockTester::serverPIDs.size()) {
        while (BedrockTester::serverPIDs.size()) {
            BedrockTester::stopServer(*(BedrockTester::serverPIDs.begin()));
        }
    }
}

void sigclean(int sig) {
    cout << "Got SIGINT, cleaning up." << endl;
    cleanup();
    cout << "Done." << endl;
    exit(1);
}


int main(int argc, char* argv[]) {
    SData args = SParseCommandLine(argc, argv);

    // Catch sigint.
    signal(SIGINT, sigclean);

    // Create our cluster.
    BedrockClusterTester tester(BedrockClusterTester::THREE_NODE_CLUSTER);

    // Our cluster is up and has responded to status, we can run our tests!
    int retval = 0;
    try {
        retval = tpunit::Tests::run();
    } catch (...) {
        cout << "Unhandled exception running tests!" << endl;
        retval = 1;
    }

    // all done.
    cleanup();

    return retval;
}

