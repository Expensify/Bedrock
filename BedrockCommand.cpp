#include <libstuff/libstuff.h>
#include "BedrockCommand.h"

// Constructor by moving from a SQLiteCommand.
BedrockCommand::BedrockCommand(SQLiteCommand&& from) :
    SQLiteCommand(std::move(from)),
    httpsRequest(nullptr)
{ }

BedrockCommand::BedrockCommand() :
    SQLiteCommand(),
    httpsRequest(nullptr)
{ }

BedrockCommand& BedrockCommand::operator=(BedrockCommand&& from)
{
    if (this != &from) {
        httpsRequest = from.httpsRequest;
        SQLiteCommand::operator=(move(from));
    }

    return *this;
}
