#include <test/lib/BedrockTester.h>

template <typename T>
class ClusterTester {
  public:
    enum ClusterSize {
        ONE_NODE_CLUSTER   = 1,
        THREE_NODE_CLUSTER = 3,
        FIVE_NODE_CLUSTER  = 5,
        SIX_NODE_CLUSTER  = 6,
    };

    // Creates a cluster of the given size and brings up all the nodes. The nodes will have priority in the order of
    // their creation (i.e., node 0 is highest priority and will become leader.
    // You can also specify plugins to load if for some reason you need to override the default configuration.
    ClusterTester(ClusterSize size, list<string> queries = {}, int threadID = 0, map<string, string> _args = {}, list<string> uniquePorts = {}, string pluginsToLoad = "db,cache,jobs");
    ClusterTester(int threadID, string pluginsToLoad = "db,cache,jobs");
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
ClusterTester<T>::ClusterTester(int threadID, string pluginsToLoad)
  : ClusterTester<T>(THREE_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"},
                            threadID, {}, {}, pluginsToLoad) {}

template <typename T>
ClusterTester<T>::ClusterTester(ClusterTester::ClusterSize size, list<string> queries, int threadID,
                                            map<string, string> _args, list<string> uniquePorts, string pluginsToLoad)
: _size(size)
{
    // Make sure we won't re-allocate.
    _cluster.reserve(size);

    // Each node gets three + uniquePorts ports. The lowest port we'll use is 11111. To make sure each thread gets it's
    // own port space, we'll add enough to this base port.
    int portCount = 3 + (int)uniquePorts.size();
    int nodePortBase = 11111 + (threadID * portCount * size);

    // Each node gets three ports.
    vector<vector<int>> ports;
    for (int i = 0; i < size; i++) {
        // This node gets three consecutive ports.
        vector<int> nodePorts = {nodePortBase + (i * portCount), nodePortBase + (i * portCount) + 1, nodePortBase + (i * portCount) + 2};
        ports.push_back(nodePorts);
    }

    // We'll need a list of each node's addresses, and each will need to know the addresses of the others.
    // We'll use this to create the 'peerList' argument for each node.
    vector<string> peers;
    for (size_t i = 0; i < size; i++) {
        int nodePort = nodePortBase + 1;
        peers.push_back("127.0.0.1:" + to_string(nodePort));
    }

    string nodeNamePrefix = "brcluster_node_";

    for (size_t i = 0; i < size; i++) {

        int portOffset = 3;
        for (auto& up : uniquePorts) {
            _args[up] = "127.0.0.1:" + to_string(nodePortBase + (i * portCount) + portOffset);
            portOffset++;
        }

        // We need each node to listen on a different port which is sort of inconvenient, but since they're all running
        // on the same machine, they can't share a port.
        int nodePort = ports[i][0];
        int serverPort = ports[i][1];
        int controlPort = ports[i][2];

        // Construct all the arguments for each server.
        string serverHost  = "127.0.0.1:" + to_string(serverPort);
        string nodeHost    = "127.0.0.1:" + to_string(nodePort);
        string controlHost = "127.0.0.1:" + to_string(controlPort);
        string db          = BedrockTester::getTempFileName("cluster_node_" + to_string(nodePort));
        string nodeName    = nodeNamePrefix + to_string(i);

        // If we're building a 6 node cluster, make the last node a permafollower
        string priority    = i < 5 ? to_string(100 - (i * 10)) : to_string(0);


        // Construct our list of peers.
        int j = 0;
        list<string> peerList;
        for (auto p : ports) {
            if (p[0] != nodePort) {
                peerList.push_back("127.0.0.1:"s + to_string(p[0]) + "?nodeName=" + nodeNamePrefix + to_string(j) + (j == 5 ? "&Permafollower=true" : ""));
            }
            j++;
        }
        string peerString = SComposeList(peerList, ",");

        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }

        // Ok, build a legit map out of these.
        map <string, string> args = {
            {"-serverHost",  serverHost},
            {"-nodeHost",    nodeHost},
            {"-controlPort", controlHost},
            {"-db",          db},
            {"-priority",    priority},
            {"-nodeName",    nodeName},
            {"-peerList",    peerString},
            {"-plugins",     pluginsToLoad + "," + string(cwd) + "/testplugin/testplugin.so"},
            {"-overrideProcessName", "bedrock" + to_string(nodePort)},
        };

        for (auto& a : _args) {
            args[a.first] = a.second;
        }
        _cluster.emplace_back(threadID, args, queries, false);
    }
    list<thread> threads;
    for (size_t i = 0; i < _cluster.size(); i++) {
        threads.emplace_back([i, this](){
            _cluster[i].startServer();
        });
    }
    for (auto& i : threads) {
        i.join();
    }

    // Ok, now we should be able to wait for the cluster to come up. Let's wait until each server responds to 'status',
    // leader first.
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
