/// bedrock/BedrockServer_MessageQueue.cpp
/// ===================
/// Synchronized message queue used between Bedrock threads.
///
#include <libstuff/libstuff.h>
#include "BedrockServer.h"

// --------------------------------------------------------------------------
BedrockServer::MessageQueue::MessageQueue() {
  // Initialize
  _queueMutex = SMutexOpen();

  // Open up a pipe for communication and set the nonblocking reads.
  SASSERT(0 == pipe(_pipeFD));
  int flags = fcntl(_pipeFD[0], F_GETFL, 0);
  fcntl(_pipeFD[0], F_SETFL, flags | O_NONBLOCK);
}

// --------------------------------------------------------------------------
BedrockServer::MessageQueue::~MessageQueue() {
  // Clean up
  SMutexClose(_queueMutex);
}

// --------------------------------------------------------------------------
int BedrockServer::MessageQueue::preSelect(fd_map &fdm) {
  // Put the read-side of the pipe into the fd set.
  // **NOTE: This is *not* synchronized.  All threads use the same pipes.
  //         All threads use *different* fd_maps, though so we don't have
  //         to worry about contention inside FDSet.
  SFDset(fdm, _pipeFD[0], SREADEVTS);
  return _pipeFD[0];
}

// --------------------------------------------------------------------------
void BedrockServer::MessageQueue::push(const SData &rhs) {
  SAUTOLOCK(_queueMutex);
  // Just add to the queue
  _queue.push_back(rhs);

  // Write arbitrary buffer to the pipe so any subscribers will
  // be awoken.
  // **NOTE: 1 byte so write is atomic.
  SASSERT(write(_pipeFD[1], "A", 1));
}

// --------------------------------------------------------------------------
SData BedrockServer::MessageQueue::pop() {
  SAUTOLOCK(_queueMutex);
  // Return the first if any, otherwise an empty object
  SData item;
  if (!_queue.empty()) {
    // Take the first
    item = _queue.front();
    _queue.pop_front();
  }
  return item;
}

// --------------------------------------------------------------------------
bool BedrockServer::MessageQueue::empty() {
  SAUTOLOCK(_queueMutex);
  // Just return the state of the queue
  return _queue.empty();
}

// --------------------------------------------------------------------------
bool BedrockServer::MessageQueue::cancel(const string &name,
                                         const string &value) {
  SAUTOLOCK(_queueMutex);
  // Loop across and see if we can find it; if so, cancel
  SFOREACH(list<SData>, _queue, queueIt) {
    if ((*queueIt)[name] == value) {
      // Found it
      _queue.erase(queueIt);
      return true;
    }
  }

  // Didn't find it
  return false;
}

// --------------------------------------------------------------------------
void BedrockServer::MessageQueue::postSelect(fd_map &fdm, int bytesToRead) {
  // Caller determines the bytes to read.  If a consumer can
  // only process one item then it will only read 1 byte.  If
  // the pipe has more data to read it will continue to "fire"
  // so other threads also subscribing will pick up work.
  if (SFDAnySet(fdm, _pipeFD[0], SREADEVTS)) {
    char readbuffer[bytesToRead];
    read(_pipeFD[0], readbuffer, sizeof(readbuffer));
  }
}
