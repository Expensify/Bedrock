
#include <BedrockPlugin.h>

/*
 * We don't really need the Bedrock plugin APIs, just the loading mechanism.
 *
 * The SQLite stored functions we register get done in udf_initialize()
 *
 */ 

class BedrockPlugin_UDF : public BedrockPlugin
{
  public:
    virtual string getName() { return "UDF"; }
};

extern "C" void udf_initialize();

extern "C" void BEDROCK_PLUGIN_REGISTER_UDF() {
    new BedrockPlugin_UDF();

	udf_initialize();
}
