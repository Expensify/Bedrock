#include <test/lib/BedrockTester.h>

template <typename T>
class ClusterTester {
  public:
    enum ClusterSize {
        ONE_NODE_CLUSTER = 1,
        THREE_NODE_CLUSTER = 3,
        FIVE_NODE_CLUSTER = 5,
        SIX_NODE_CLUSTER = 6,
    };

    // Creates a cluster of the given size and brings up all the nodes. The nodes will have priority in the order of
    // their creation (i.e., node 0 is highest priority and will become leader.
    // You can also specify plugins to load if for some reason you need to override the default configuration.
    ClusterTester(ClusterSize size, list<string> queries = {}, int threadID = 0, map<string, string> _args = {}, list<string> uniquePorts = {}, string pluginsToLoad = "db,cache,jobs");
    ClusterTester(int threadID, string pluginsToLoad = "db,cache,jobs");
    ClusterTester(const string& pluginString = "db,cache,jobs");
    ~ClusterTester();

    // Returns the tester at the given index in the cluster.
    T& getTester(size_t index);

    // Legacy name for the above. TODO: Remove when not used by auth.
    T* getBedrockTester(size_t index);

    // Starts a given node, given the same arguments given by the constructor.
    string startNode(size_t nodeIndex);

    // Same as above but don't wait for the command port to be ready.
    string startNodeDontWait(size_t nodeIndex);

    // Stops a given node.
    void stopNode(size_t nodeIndex);

  private:

    // The number of nodes in the cluster.
    int _size;

    // A list of all our testers that make up our cluster.
    vector<T> _cluster;
};

typedef ClusterTester<BedrockTester> BedrockClusterTester;

template <typename T>
ClusterTester<T>::ClusterTester(const string& pluginString) : ClusterTester<T>(
    THREE_NODE_CLUSTER,
    {},
    0,
    {},
    {},
    pluginString
    )
{}

template <typename T>
ClusterTester<T>::ClusterTester(int threadID, string pluginsToLoad)
  : ClusterTester<T>(THREE_NODE_CLUSTER, {},
                            threadID, {}, {}, pluginsToLoad, nullptr) {}

template <typename T>
ClusterTester<T>::ClusterTester(ClusterTester::ClusterSize size,
                                list<string> queries,
                                int threadID,
                                map<string, string> _args,
                                list<string> uniquePorts,
                                string pluginsToLoad)
: _size(size)
{
    // Make sure we won't re-allocate.
    _cluster.reserve(size);

    // We need three ports for each node.
    vector<vector<uint16_t>> ports(size);
    for (size_t i = 0; i < size; i++) {
        const uint16_t serverPort = BedrockTester::ports.getPort();
        const uint16_t nodePort = BedrockTester::ports.getPort();
        const uint16_t controlPort = BedrockTester::ports.getPort();
        ports[i].resize(3);
        ports[i][0] = serverPort;
        ports[i][1] = nodePort;
        ports[i][2] = controlPort;
    }

    // If the test plugin exists, load it. Otherwise, skip it. This keeps this from failing for derived tests that
    // don't use this plugin. This should get moved somewhere else, really. Probably inside TestPlugin.
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        STHROW("Couldn't get CWD");
    }
    if (SFileExists(string(cwd) + "/testplugin/testplugin.so")) {
        pluginsToLoad += pluginsToLoad.size() ? pluginsToLoad += "," : "";
        pluginsToLoad += string(cwd) + "/testplugin/testplugin.so";

        // And we'll also create the test table in this case.
        if (queries.empty()) {
            queries.push_back("CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)");
        }
    }

    const string nodeNamePrefix = "brcluster_node_";
    for (size_t i = 0; i < size; i++) {

        // We already allocated our own ports earlier.
        uint16_t serverPort = ports[i][0];
        uint16_t nodePort = ports[i][1];
        uint16_t controlPort = ports[i][2];

        // Construct all the arguments for each server.
        string serverHost  = "127.0.0.1:" + to_string(serverPort);
        string nodeHost = "127.0.0.1:" + to_string(nodePort);
        string controlHost = "127.0.0.1:" + to_string(controlPort);
        string db = BedrockTester::getTempFileName("cluster_node_" + to_string(nodePort));
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
            {"-db", db},
            {"-priority", priority},
            {"-nodeName", nodeName},
            {"-peerList", peerString},
            {"-plugins", pluginsToLoad},
            {"-overrideProcessName", "bedrock" + to_string(nodePort)},
        };

        // Now, if any args were supplied, we'll use those instead of these ones (note that this overwrites our
        // existing values).
        for (auto& a : _args) {
            if (a.first == "-serverHost" || "-nodeHost" || "-controlPort") {
                cout << "Skipping port overwriting" << endl;
            } else {
                args[a.first] = a.second;
            }
        }

        // And add the new entry in the map.
        _cluster.emplace_back(args, queries, false, false, serverPort, nodePort, controlPort, false);
    }

    // Now start them all.
    list<thread> threads;
    for (size_t i = 0; i < _cluster.size(); i++) {
        threads.emplace_back([i, this](){
            _cluster[i].startServer();
        });
    }
    for (auto& i : threads) {
        i.join();
    }

    // Ok, now we should be able to wait for the cluster to come up. Let's wait until each server responds to 'status'.
    vector<string> states(size);
    int count = 0;
    for (size_t i = 0; i < size; i++) {
        while (1) {
            count++;
            // Give up after a minute. This will fail the remainder of the test, but won't hang indefinitely.
            if (count > 60 * 10) {
                break;
            }
            try {
                SData status("Status");
                string response = _cluster[i].executeWaitVerifyContent(status);
                STable json = SParseJSONObject(response);
                states[i] = json["state"];
                break;
            } catch (...) {
                // This will happen if the server's not up yet. We'll just try again.
            }
            usleep(500000); // 0.5 seconds.
        }
    }
}

template <typename T>
ClusterTester<T>::~ClusterTester()
{
    // Shut them down in reverse order so they don't try and stand up as leader in the middle of everything.
    for (int i = _size - 1; i >= 0; i--) {
        stopNode(i);
    }

    _cluster.clear();
}

template <typename T>
T* ClusterTester<T>::getBedrockTester(size_t index)
{
    return &_cluster[index];
}

template <typename T>
T& ClusterTester<T>::getTester(size_t index)
{
    return _cluster[index];
}

template <typename T>
void ClusterTester<T>::stopNode(size_t nodeIndex)
{
    _cluster[nodeIndex].stopServer();
}

template <typename T>
string ClusterTester<T>::startNode(size_t nodeIndex)
{
    return _cluster[nodeIndex].startServer();
}

template <typename T>
string ClusterTester<T>::startNodeDontWait(size_t nodeIndex)
{
    return _cluster[nodeIndex].startServer(true);
}
