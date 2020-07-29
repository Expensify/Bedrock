#pragma once
#include "SQLiteNode.h"

class SQLiteCommand {
  public:

    // This allows for modifying a request passed into the constructor such that we can store it as `const`.
    static SData preprocessRequest(SData&& request);

    // If this command was created via an escalation from a peer, this value will point to that peer object. As such,
    // this should only ever be set on leader nodes, though it does not *need* to be set on leader nodes, as they can
    // also accept connections directly from clients.
    // A value of zero is an invalid ID, and is interpreted to mean "not set".
    // A negative value indicates a valid ID of an invalid peer (a psuedo-peer, or a disconnected peer), that we can't
    // respond to.
    int64_t initiatingPeerID;

    // If this command was created via a direct client connection, this value should be set. This can be set on both
    // leader and followers, but should not be set simultaneously with `initiatingPeerID`. A command was initiated either
    // by a client, or by a peer.
    // A value of zero is an invalid ID, and is interpreted to mean "not set".
    // A negative value indicates a valid ID of an invalid client (a psuedo-client, or a disconnected client), that we
    // can't respond to.
    int64_t initiatingClientID;

    // Each command is given a unique id that can be serialized and passed back and forth across nodes. Its id must be
    // uniquely identifiable for cases where, for instance, two peers escalate commands to the leader, and leader will
    // need to  respond to them.
    string id;

    // Original request, immutable.
    const SData request;

    // Accumulated response content
    STable jsonContent;

    // Final response
    SData response;

    // Write consistency required when committing this command.
    SQLiteNode::ConsistencyLevel writeConsistency;

    // Whether this command has been completed.
    bool complete;

    // Performance metrics.
    // This is the amount of time a command spent in escalation. A leader node will record this from the time it first
    // dequeues and parses the command, until the time it sends the response back to a follower. A follower node will record
    // this from just before it sends the command to leader until it gets the response back.
    uint64_t escalationTimeUS;

    // Timestamp at which this command was created. This is specific to the current server - it's not persisted from
    // follower to leader.
    uint64_t creationTime;

    // Whether or not the command has been escalated.
    bool escalated;

    // Construct that takes a request object.
    SQLiteCommand(SData&& _request);

    // Default Constructor.
    SQLiteCommand();

    // Move constructor.
    SQLiteCommand(SQLiteCommand&& from) = default;

    // Default move assignment operator.
    SQLiteCommand& operator=(SQLiteCommand&& from) = default;

    // Destructor.
    virtual ~SQLiteCommand() {}
};
