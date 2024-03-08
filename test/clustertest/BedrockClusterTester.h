#pragma once
#include <iostream>
#include <unistd.h>

#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

enum class ClusterSize {
    ONE_NODE_CLUSTER = 1,
    THREE_NODE_CLUSTER = 3,
    FIVE_NODE_CLUSTER = 5,
    SIX_NODE_CLUSTER = 6,
};

template <typename T>
class ClusterTester {
  public:

    // Creates a cluster of the given size and brings up all the nodes. The nodes will have priority in the order of
    // their creation (i.e., node 0 is highest priority and will become leader.
    // You can also specify plugins to load if for some reason you need to override the default configuration.
    ClusterTester(ClusterSize size, list<string> queries = {}, map<string, string> _args = {}, list<string> uniquePorts = {}, string pluginsToLoad = "db,cache,jobs", const string& processPath = "");
    ClusterTester(const string& pluginString = "db,cache,jobs", const string& processPath = "");
    ~ClusterTester();

    // Returns the tester at the given index in the cluster.
    T& getTester(size_t index);

    // Starts a given node, given the same arguments given by the constructor.
    string startNode(size_t index);

    // Same as above but don't wait for the command port to be ready.
    string startNodeDontWait(size_t index);

    // Stops a given node.
    void stopNode(size_t index);

    atomic<uint64_t> groupCommitCount{0};

  private:

    // The number of nodes in the cluster.
    int _size;

    // A list of all our testers that make up our cluster.
    list<T> _cluster;
};

typedef ClusterTester<BedrockTester> BedrockClusterTester;

template <typename T>
ClusterTester<T>::ClusterTester(const string& pluginString, const string& processPath) : ClusterTester<T>(
    ClusterSize::THREE_NODE_CLUSTER,
    {},
    {},
    {},
    pluginString,
    processPath
    )
{}

template <typename T>
ClusterTester<T>::ClusterTester(ClusterSize size,
                                list<string> queries,
                                map<string, string> _args,
                                list<string> uniquePorts,
                                string pluginsToLoad,
                                const string& processPath)
: _size((int)size)
{
    // Generate three ports for each node.
    vector<vector<uint16_t>> ports((int)size);
    for (size_t i = 0; i < (size_t)size; i++) {
        const uint16_t serverPort = BedrockTester::ports.getPort();
        const uint16_t nodePort = BedrockTester::ports.getPort();
        const uint16_t controlPort = BedrockTester::ports.getPort();
        ports[i].resize(3);
        ports[i][0] = serverPort;
        ports[i][1] = nodePort;
        ports[i][2] = controlPort;
    }

    // If the test plugin exists, and isn't already specified, load it. Otherwise, skip it. This keeps this from failing for derived tests that
    // don't use this plugin. This should get moved somewhere else, really. Probably inside TestPlugin.
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        STHROW("Couldn't get CWD");
    }
    if (SFileExists(string(cwd) + "/testplugin/testplugin.so") && !SContains(pluginsToLoad, "testplugin")) {
        pluginsToLoad += pluginsToLoad.size() ? "," : "";
        pluginsToLoad += string(cwd) + "/testplugin/testplugin.so";
    }

    const string nodeNamePrefix = "cluster_node_";
    for (size_t i = 0; i < (size_t)size; i++) {

        // We already allocated our own ports earlier.
        uint16_t serverPort = ports[i][0];
        uint16_t nodePort = ports[i][1];
        uint16_t controlPort = ports[i][2];

        // Construct all the arguments for each server.
        string serverHost  = "127.0.0.1:" + to_string(serverPort);
        string nodeHost = "127.0.0.1:" + to_string(nodePort);
        string controlHost = "127.0.0.1:" + to_string(controlPort);
        string nodeName = nodeNamePrefix + to_string(i);

        // If we're building a 6 node cluster, make the last node a permafollower
        string priority = i < 5 ? to_string(100 - (i * 10)) : to_string(0);

        // Construct our list of peers.
        size_t j = 0;
        list<string> peerList;
        for (auto p : ports) {
            if (j != i) {
                peerList.push_back("127.0.0.1:"s + to_string(p[1]) + "?nodeName=" + nodeNamePrefix + to_string(j) + (j == 5 ? "&Permafollower=true" : ""));
            }
            j++;
        }
        string peerString = SComposeList(peerList, ",");

        // Ok, build a legit map out of these.
        map <string, string> args = {
            {"-serverHost", serverHost},
            {"-nodeHost", nodeHost},
            {"-controlPort", controlHost},
            {"-priority", priority},
            {"-nodeName", nodeName},
            {"-peerList", peerString},
            {"-plugins", pluginsToLoad},
            {"-overrideProcessName", "bedrock" + to_string(nodePort)},
        };

        // Now, if any args were supplied, we'll use those instead of these ones (note that this overwrites our
        // existing values).
        for (auto& a : _args) {
            if (a.first == "-serverHost" || a.first == "-nodeHost" || a.first == "-controlPort") {
                cout << "Skipping port overwriting, " << a.first << ":" << a.second << endl;
            } else {
                args[a.first] = a.second;
            }
        }

        // And add the new entry in the map.
        _cluster.emplace_back(args, queries, serverPort, nodePort, controlPort, false, processPath, &groupCommitCount);
    }

    auto start = STimeNow();

    // Now start them all.
    list<thread> threads;
    for (auto it = _cluster.begin(); it != _cluster.end(); it++) {
        threads.emplace_back([it](){
            it->startServer();
        });
    }
    for (auto& i : threads) {
        i.join();
    }

    // Ok, now we should be able to wait for the cluster to come up. Let's wait until each server responds to 'status'.
    for (auto& node : _cluster) {
        uint64_t startTime = STimeNow();
        while (1) {
            // Give up after a minute. This will fail the remainder of the test, but won't hang indefinitely.
            if (startTime + 60'000'000 < STimeNow()) {
                cout << "ClusterTester constructor timed out starting cluster" << endl;
                break;
            }
            try {
                SData status("Status");
                string response = node.executeWaitVerifyContent(status);
                STable json = SParseJSONObject(response);
                break;
            } catch (...) {
                // This will happen if the server's not up yet. We'll just try again.
            }
            usleep(100000); // 0.1 seconds.
        }
    }
    auto end = STimeNow();

    cout << "Took " << ((end - start) / 1000) << "ms to start cluster." << endl;
}

template <typename T>
ClusterTester<T>::~ClusterTester()
{
    auto start = STimeNow();

    // Shut down everything but the leader first.
    list<thread> threads;
    cout << "Starting shutdown at " << SCURRENT_TIMESTAMP() << endl;
    for (int i = _size - 1; i > 0; i--) {
        threads.emplace_back([&, i](){
            cout << "Stopping node " << i << " at " << SCURRENT_TIMESTAMP() << endl;
            stopNode(i);
        });
    }
    for (auto& t: threads) {
        t.join();
    }

    // Then do leader last. This is to avoid getting in a state where nodes try to stand up as leader shuts down.
    cout << "Stopping node " << 0 << " at " << SCURRENT_TIMESTAMP() << endl;
    stopNode(0);

    auto end = STimeNow();

    cout << "Took " << ((end - start) / 1000) << "ms to stop cluster." << endl;
    _cluster.clear();
}

template <typename T>
T& ClusterTester<T>::getTester(size_t index)
{
    return *next(_cluster.begin(), index);
}

template <typename T>
void ClusterTester<T>::stopNode(size_t index)
{
    next(_cluster.begin(), index)->stopServer();
}

template <typename T>
string ClusterTester<T>::startNode(size_t index)
{
    return next(_cluster.begin(), index)->startServer();
}

template <typename T>
string ClusterTester<T>::startNodeDontWait(size_t index)
{
    return next(_cluster.begin(), index)->startServer(false);
}
