#pragma once
#include "SQLiteNode.h"

class SQLiteCommand {
  public:
    // If this command was created via an escalation from a peer, this value will point to that peer object. As such,
    // this should only ever be set on master nodes, though it does not *need* to be set on master nodes, as they can
    // also accept connections directly from clients.
    // A value of zero is an invalid ID, and is interpreted to mean `not set`.
    uint64_t initiatingPeerID;

    // If this command was created via a direct client connection, this value should be set. This can be set on both
    // master and slaves, but should not be set simultaneously with `initiatingPeer`. A command was initiated either by
    // a client, or by a peer.
    // A value of zero is an invalid ID, and is interpreted to mean `not set`.
    uint64_t initiatingClientID;

    // Each command is given a unique id that can be serialized and passed back and forth across nodes. It's id must be
    // uniquely identifiable for cases where, for instance, two peers escalate commands to the master, and master will
    // need to  respond to them.
    string id;
    SData transaction;  // Used inside SQLiteNode
    SData request;      // Original request
    STable jsonContent; // Accumulated response content
    SData response;     // Final response
    SQLiteNode::ConsistencyLevel writeConsistency;
    bool complete;

    // Track timing information. TODO: Remove? Refactor?
    uint64_t creationTimestamp;
    uint64_t replicationStartTimestamp;
    uint64_t processingTime;

    // Construct that takes a request object.
    SQLiteCommand(SData&& _request);

    // Default Constructor.
    SQLiteCommand();

    // Move constructor.
    SQLiteCommand(SQLiteCommand&& from) = default;

    // Default move assignment operator.
    SQLiteCommand& operator=(SQLiteCommand&& from) = default;

    // Destructor.
    ~SQLiteCommand() {}
};
