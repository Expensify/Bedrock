#include "../libstuff/libstuff.h"

class SimpleHTTPSManager : public SHTTPSManager {
  public:
    SimpleHTTPSManager();
    virtual bool _onRecv(Transaction* transaction);
    Transaction* send(const string& url, const SData& request);

    virtual ~SimpleHTTPSManager();
};

// Wrapper function to loop over our wrapper functions in a thread.
static void _poll(SimpleHTTPSManager& httpsManager, SHTTPSManager::Transaction* request);

// Wrappers for this plugin that just call the base class of the HTTPSManager.
static void _prePoll(fd_map& fdm, SimpleHTTPSManager& httpsManager);
static void _postPoll(fd_map& fdm, uint64_t nextActivity, SimpleHTTPSManager& httpsManager);
static void _sendQueryRequest(string host, SimpleHTTPSManager& httpsManager);
