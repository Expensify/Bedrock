#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_Test : public BedrockPlugin {
  public:
    // Implement base class interface
    virtual string getName() { return "Test"; }
    virtual void upgradeDatabase(BedrockNode* node, SQLite& db);
};
