// BedrockNode.h
#ifndef _BEDROCKNODE_H
#define _BEDROCKNODE_H
#include <sqlitecluster/SQLiteNode.h>

class BedrockServer;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
struct BedrockTester; // Defined in BedrockTester.h, but can't include else
                      // circular
class BedrockNode : public SQLiteNode {
public:
  // Construct the base class
  BedrockNode(const SData &args, BedrockServer *server_);
  virtual ~BedrockNode();

  // Simple plugin system to add functionality to a node at runtime
  class Plugin {
  public:
    // --- Parent interface ---
    Plugin();

    // --- Child interface ---
    // Returns a short, descriptive name of this plugin
    virtual string getName() {
      SERROR("No name defined by this plugin, aborting.");
    }

    // Initializes it with command-line arguments
    virtual void initialize(const SData &args) {}

    // Optionally updates the schema as necessary to support this plugin
    virtual void upgradeDatabase(BedrockNode *node, SQLite &db) {}

    // Optionally "peeks" at a command to process it in a read-only manner
    virtual bool peekCommand(BedrockNode *node, SQLite &db,
                             BedrockNode::Command *command) {
      return false;
    }

    // Optionally "processes" a command in a read/write manner, triggering
    // a distributed transaction if necessary
    virtual bool processCommand(BedrockNode *node, SQLite &db,
                                BedrockNode::Command *command) {
      return false;
    }

    // Enable this plugin for active operation
    virtual void enable(bool enabled) { _enabled = enabled; }
    virtual bool enabled() { return _enabled; }

    // Allow this node to do a self-test
    virtual void test(BedrockTester *tester) {}

    // The plugin can add as many of these as it needs in it's initialize
    // function. The server will then add those
    // to it's own global list. Note that this is the *only* time these can be
    // added, once `initialize` completes,
    // the server maintains a copy of this list. This also means that these
    // managers need to live for the life of
    // the application.
    list<SHTTPSManager *> httpsManagers;

    // The plugin can register any number of timers it wants. When any of them
    // `ding`, then the `timerFired`
    // function will be called, and passed the timer that is dinging.
    set<SStopwatch *> timers;
    virtual void timerFired(SStopwatch *timer) {}

  protected:
    // Attributes
    bool _enabled;

  public:
    // Global static attributes
    static list<Plugin *> *g_registeredPluginList;
  };

  BedrockServer *server;

  bool isReadOnly();

  // STCPManager API: Socket management
  void postSelect(fd_map &fdm, uint64_t &nextActivity);

  // SQLiteNode API: Command management
  virtual bool _peekCommand(SQLite &db, Command *command);
  virtual void _processCommand(SQLite &db, Command *command);
  virtual void _abortCommand(SQLite &db, Command *command);
  virtual void _cleanCommand(Command *command);
};

// BedrockNode.h
#endif
