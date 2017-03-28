#include "BedrockClusterTester.h"

list<BedrockClusterTester*> BedrockClusterTester::testers;

BedrockClusterTester::BedrockClusterTester(BedrockClusterTester::ClusterSize size, list<string> queries)
: _size(size)
{
    cout << "Starting " << size << " node bedrock cluster." << endl;
    // Make sure we won't re-allocate.
    _cluster.reserve(size);
    _args.reserve(size);

    int nodePortBase = 9500;
    // We'll need a list of each node's addresses, and each will need to know the addresses of the others.

    // We'll use this to create the 'peerList' argument for each node.
    vector<string> peers;
    for (size_t i = 0; i < size; i++) {
        int nodePort = nodePortBase + i;
        peers.push_back("127.0.0.1:" + to_string(nodePort));
    }

    string nodeNamePrefix = "brcluster_node_";

    for (size_t i = 0; i < size; i++) {

        // We need each node to listen on a different port which is sort of inconvenient, but since they're all running
        // on the same machine, they can't share a port.
        int serverPort = 9000 + i;
        int nodePort = nodePortBase + i;

        // Construct all the arguments for each server.
        string serverHost = "127.0.0.1:" + to_string(serverPort);
        string nodeHost   = "127.0.0.1:" + to_string(nodePort);
        string db         = BedrockTester::getTempFileName("cluster_node_" + to_string(i) + "_");
        string priority   = to_string(100 - (i * 10));
        string nodeName   = nodeNamePrefix + to_string(i);

        // Construct our list of peers.
        list<string> peerList;
        for (size_t j = 0; j < peers.size(); j++) {
            if (j != i) {
                peerList.push_back(peers[j] + "?nodeName=" + nodeNamePrefix + to_string(j));
            }
        }
        string peerString = SComposeList(peerList, ",");

        char cwd[1024];
        getcwd(cwd, sizeof(cwd));

        // Ok, build a legit map out of these.
        map <string, string> args = {
            {"-serverHost", serverHost},
            {"-nodeHost",   nodeHost},
            {"-db",         db},
            {"-priority",   priority},
            {"-nodeName",   nodeName},
            {"-peerList",   peerString},
            {"-plugins",    "status,db,cache," + string(cwd) + "/testplugin/testplugin.so"},
        };

        // save this map for later.
        _args.push_back(args);

        _cluster.emplace_back(db, serverHost, queries, args, false);
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
                string response = _cluster[i].executeWait(status, "200");
                STable json = SParseJSONObject(response);
                states[i] = json["state"];
                break;
            } catch (...) {
                // This will happen if the server's not up yet. We'll just try again.
            }
            usleep(500000); // 0.5 seconds.
        }
    }

    testers.push_back(this);
}

BedrockClusterTester::~BedrockClusterTester()
{
    // Shut them down in reverse order so they don't try and stand up as master in the middle of everything.
    for (int i = _size - 1; i >= 0; i--) {
        stopNode(i);
    }

    _cluster.clear();
    // Remove ourselves from our global list.
    testers.remove_if([this](BedrockClusterTester* bct){
        return (bct == this);
    });
}

BedrockTester* BedrockClusterTester::getBedrockTester(size_t index)
{
    return &_cluster[index];
}

void BedrockClusterTester::stopNode(size_t nodeIndex)
{
    _cluster[nodeIndex].stopServer();
}

void BedrockClusterTester::startNode(size_t nodeIndex)
{
    _cluster[nodeIndex].startServer(_args[nodeIndex], true);
}
