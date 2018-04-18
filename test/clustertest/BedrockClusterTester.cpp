#include "BedrockClusterTester.h"

BedrockClusterTester::BedrockClusterTester(int threadID)
  : BedrockClusterTester(THREE_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"}, threadID, {}, {}) {}

BedrockClusterTester::BedrockClusterTester(BedrockClusterTester::ClusterSize size, list<string> queries, int threadID, map<string, string> _args, list<string> uniquePorts)
: _size(size)
{
    cout << "Starting " << size << " node bedrock cluster." << endl;
    // Make sure we won't re-allocate.
    _cluster.reserve(size);

    // Each node gets three + uniquePorts ports. The lowest port we'll use is 11111. To make sure each thread gets it's
    // own port space, we'll add enough to this base port.
    int portCount = 3 + uniquePorts.size();
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
        string priority    = to_string(100 - (i * 10));
        string nodeName    = nodeNamePrefix + to_string(i);

        // Construct our list of peers.
        int j = 0;
        list<string> peerList;
        for (auto p : ports) {
            if (p[0] != nodePort) {
                peerList.push_back("127.0.0.1:"s + to_string(p[0]) + "?nodeName=" + nodeNamePrefix + to_string(j));
            }
            j++;
        }
        string peerString = SComposeList(peerList, ",");

        char cwd[1024];
        getcwd(cwd, sizeof(cwd));

        // Ok, build a legit map out of these.
        map <string, string> args = {
            {"-serverHost",  serverHost},
            {"-nodeHost",    nodeHost},
            {"-controlPort", controlHost},
            {"-db",          db},
            {"-priority",    priority},
            {"-nodeName",    nodeName},
            {"-peerList",    peerString},
            {"-plugins",     "db,cache,jobs," + string(cwd) + "/testplugin/testplugin.so"},
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
    // master first.
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

BedrockClusterTester::~BedrockClusterTester()
{
    // Shut them down in reverse order so they don't try and stand up as master in the middle of everything.
    for (int i = _size - 1; i >= 0; i--) {
        stopNode(i);
    }

    _cluster.clear();
}

BedrockTester* BedrockClusterTester::getBedrockTester(size_t index)
{
    return &_cluster[index];
}

void BedrockClusterTester::stopNode(size_t nodeIndex)
{
    _cluster[nodeIndex].stopServer();
}

string BedrockClusterTester::startNode(size_t nodeIndex)
{
    return _cluster[nodeIndex].startServer();
}

string BedrockClusterTester::startNodeDontWait(size_t nodeIndex)
{
    return _cluster[nodeIndex].startServer(true);
}
