#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>
STable getJsonResult(BedrockTester* tester, SData command) {
    string resultJson = tester->executeWait(command);
    return SParseJSONObject(resultJson);
}
