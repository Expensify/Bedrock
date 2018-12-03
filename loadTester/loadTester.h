#include "../libstuff/libstuff.h"

class TestHTTPSMananager : public SHTTPSManager {
  public:
    TestHTTPSMananager();
    virtual bool _onRecv(Transaction* transaction);
    Transaction* send(const string& url, const SData& request);

    virtual ~TestHTTPSMananager();
};

// Wrapper function to loop over our wrapper functions in a thread.
static void _poll(TestHTTPSMananager& httpsManager, SHTTPSManager::Transaction* request);

// Wrappers for this plugin that just call the base class of the HTTPSManager.
static void _prePoll(fd_map& fdm, TestHTTPSMananager& httpsManager);
static void _postPoll(fd_map& fdm, uint64_t nextActivity, TestHTTPSMananager& httpsManager);
static void _sendQueryRequest(string host);
