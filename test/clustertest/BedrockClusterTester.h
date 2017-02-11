#include <test/lib/BedrockTester.h>

class BedrockClusterTester {
  public:

    // This is really only set up to ever have a single entry.
    static list<BedrockClusterTester*> testers;

    enum ClusterSize {
        ONE_NODE_CLUSTER   = 1,
        THREE_NODE_CLUSTER = 3,
        FIVE_NODE_CLUSTER  = 5,
    };

    // Creates a cluster of the given size and brings up all the nodes. The nodes will have priority in the order of
    // their creation (i.e., node 0 is highest priority and will become master.
    BedrockClusterTester(ClusterSize size, list<string> queries = {});
    ~BedrockClusterTester();

    // Returns the index of the node that's mastering. Returns a negative number on error:
    // -1: no master
    // -2: multiple masters
    int getMasterNodeIndex();

    // Runs the given query on all nodes and verifies the output is the same. Make sure you include "ORDER BY" if you
    // want to verify this across multiple rows, as they're not guaranteed to be returned in the same order for all
    // nodes.
    bool VerifyQuery(string query);

    // Wait until all nodes have the same highest commit count. Optional timeout, set to 0 for no timeout.
    // Returns true on sync, false on timeout.
    bool waitForSync(int timeout_usec = 0);

    // Returns the bedrock tester at the given index in the cluster.
    BedrockTester* getBedrockTester(size_t index);

    // Starts a given node, given the same arguments given by the constructor.
    void startNode(size_t nodeIndex);

    // Stops a given node.
    void stopNode(size_t nodeIndex);

  private:

    // The number of nodes in the cluster.
    int _size;

    // A list of all our testers that make up our cluster.
    vector<BedrockTester> _cluster;

    // The arguments we start each server with. We store them so we can bring them down and back up with the same
    // settings.
    vector<map<string, string>> _args;
};
