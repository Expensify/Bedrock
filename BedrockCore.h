#pragma once
#include <sqlitecluster/SQLiteCore.h>
#include <BedrockCommand.h>
class BedrockServer;

class BedrockCore : public SQLiteCore {
  public:
    BedrockCore(SQLite& db, const BedrockServer& server);

    // Possible result values for `peekCommand` and `processCommand`
    enum class RESULT {
        INVALID = 0,
        COMPLETE = 1,
        SHOULD_PROCESS = 2,
        NEEDS_COMMIT = 3,
        NO_COMMIT_REQUIRED = 4,
        SERVER_NOT_LEADING = 5
    };

    // Automatic timing class that records an entry corresponding to its lifespan.
    class AutoTimer {
      public:
        AutoTimer(unique_ptr<BedrockCommand>& command, BedrockCommand::TIMING_INFO type) :
        _command(command), _type(type), _start(STimeNow()) { }
        ~AutoTimer() {
          _command->timingInfo.emplace_back(make_tuple(_type, _start, STimeNow()));
        }
      private:
        unique_ptr<BedrockCommand>& _command;
        BedrockCommand::TIMING_INFO _type;
        uint64_t _start;
    };

    // Checks if a command has already timed out. Like `peekCommand` without doing any work. Returns `true` and sets
    // the same command state as `peekCommand` would if the command has timed out. Returns `false` and does nothing if
    // the command hasn't timed out.
    bool isTimedOut(unique_ptr<BedrockCommand>& command);

    void prePeekCommand(unique_ptr<BedrockCommand>& command, bool isBlockingCommitThread);

    // Peek lets you pre-process a command. It will be called on each command before `process` is called on the same
    // command, and it *may be called multiple times*. Preventing duplicate actions on calling peek multiple times is
    // up to the implementer, and may happen *across multiple servers*. I.e., a follower server may call `peek`, and on
    // its returning false, the command will be escalated to the leader server, where `peek` will be called again. It
    // should be considered an error to modify the DB from inside `peek`.
    // Returns a boolean value of `true` if the command is complete and its `response` field can be returned to the
    // caller. Returns `false` if the command will need to be passed to `process` to complete handling the command.
    RESULT peekCommand(unique_ptr<BedrockCommand>& command, bool exclusive = false);

    // Process is the follow-up to `peek` if `peek` was insufficient to handle the command. It will only ever be called
    // on the leader node, and should always be able to resolve the command completely. When a command is passed to
    // `process`, the caller will *usually* have already begun a database transaction with either `BEGIN TRANSACTION`
    // or `BEGIN CONCURRENT`, and it's up to `process` to add the rest of the transaction, without performing a
    // `ROLLBACK` or `COMMIT`, which will be handled by the caller. It returns `true` if it has modified the database
    // and requires the caller to perform a `commit`, and `false` if no changes requiring a `commit` have been made.
    // Upon being returned `false`, the caller will perform a `ROLLBACK` of the empty transaction, and will not
    // replicate the transaction to follower nodes. Upon being returned `true`, the caller will attempt to perform a
    // `COMMIT` and replicate the transaction to follower nodes. It's allowable for this `COMMIT` to fail, in which case
    // this command *will be passed to process again in the future to retry*.
    RESULT processCommand(unique_ptr<BedrockCommand>& command, bool exclusive = false);

    void postProcessCommand(unique_ptr<BedrockCommand>& command, bool isBlockingCommitThread);

    // If the remaining time until timeout is greater than timeoutMS, then the command timeout will be decreased to timeoutMS,
    // otherwise, nothing happens
    void decreaseCommandTimeout(unique_ptr<BedrockCommand>& command, uint64_t timeoutMS);
  private:
    // When called in the context of handling an exception, returns the demangled (if possible) name of the exception.
    string _getExceptionName();

    // Gets the amount of time remaining until this command times out. This is the difference between the command's
    // 'timeout' value (or the default timeout, if not set) and the time the command was initially scheduled to run. If
    // this time is already expired, this throws `555 Timeout`
    uint64_t _getRemainingTime(const unique_ptr<BedrockCommand>& command, bool isProcessing);

    void _handleCommandException(unique_ptr<BedrockCommand>& command, const SException& e);
    const BedrockServer& _server;
};
