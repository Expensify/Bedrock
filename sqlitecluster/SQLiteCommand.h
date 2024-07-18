#pragma once

#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>

class SQLiteCommand {
  // This is a little bit weird so let me explain. We have, and long have had, a publicly visible member
  // `const SData request`. This is const because it prevents all sort of weird reply bugs that happen when commands
  // get re-run due to database conflicts. This works great *except* that it was discovered that this breaks move
  // semantics. Because the request object was const, it could not be moved-from. Instead, in places where the move
  // constructor was called, it would be copied, which could be quite expensive for large requests.
  // The solution was to add a private, non-const request variable that can be moved-from, and to make the existing
  // `request` object a const reference to this internal object. This allows the move constructor to move-from the
  // existing object's privateRequest, and then initialize `request` to refer to it.
  private:
    SData privateRequest;

  public:
    // Immutable reference handle to the original request.
    const SData& request;

    // This allows for modifying a request passed into the constructor such that we can store it as `const`.
    static SData preprocessRequest(SData&& request);

    // A value of zero is an invalid ID, and is interpreted to mean "not set".
    // A negative value indicates a valid ID of an invalid client (a psuedo-client, or a disconnected client), that we
    // can't respond to.
    int64_t initiatingClientID;

    // Each command is given a unique id that can be serialized and passed back and forth across nodes. Its id must be
    // uniquely identifiable for cases where, for instance, two peers escalate commands to the leader, and leader will
    // need to  respond to them.
    string id;

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
    SQLiteCommand(SQLiteCommand&& from);

    // Default move assignment operator.
    // SQLiteCommand& operator=(SQLiteCommand&& from) = default;

    // Destructor.
    virtual ~SQLiteCommand() {}
};
