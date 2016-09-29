#include <libstuff/libstuff.h>

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
extern void SQLiteTest(const SData& args);
int main(int argc, char* argv[])
{
    // Clean up the results of any old test
    SInitialize();
    SLogLevel(LOG_INFO);
    SData args = SParseCommandLine(argc, argv);
    if (args.isSet("-v"))
        SLogLevel(LOG_DEBUG);
    if (!args.isSet("-1") && !args.isSet("-2") && !args.isSet("-3") && !args.isSet("-5"))
        args["-all"] = "true";

    // Run the test
    SQLiteTest(args);
    return 0;
}
