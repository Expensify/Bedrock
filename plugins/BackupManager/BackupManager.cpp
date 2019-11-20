#include "BackupManager.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{BackupManager} "

extern "C" BedrockPlugin_BackupManager* BEDROCK_PLUGIN_REGISTER_BACKUPMANAGER(BedrockServer& s)
{
    return new BedrockPlugin_BackupManager(s);
}

BedrockPlugin_BackupManager::BedrockPlugin_BackupManager(BedrockServer& s) :
    BedrockPlugin(s), operationInProgress(false), shouldExit(false)
{
    localArgs = server.args;
    string awsAccessKey, awsSecretKey, awsBucketName, manifestKey;
    string keyFile = server.args["-backupKeyFile"];

    // Load our backup keys from a file on disk.
    if (SFileExists(keyFile)) {
        // Read the whole file into our key
        if(SParseConfigFile(keyFile, keys)) {
            SINFO("Loaded key file " << keyFile);
        } else {
            SERROR("Failed to load secure keys.");
        }
    } else {
        SERROR("No secure data file " << keyFile << " found for BackupManager.");
    }
    manifestKey = keys["manifestKey"];

    // If no db arg is set we won't know where to get a db from or where to put one.
    SASSERT(server.args.isSet("-db"));
    details["databasePath"] = server.args["-db"];
    details["manifestFileName"] = server.args["-bootstrap"];
    details["hostname"] = server.args["-nodeName"];

    // Make sure our manifest key is the correct size and add it to the details
    manifestKey = SStrFromHex(manifestKey);
    SASSERT(manifestKey.size() == SAES_KEY_SIZE);
    details["manifestKey"] = manifestKey;
    SDEBUG("keys: " << keys["manifestKey"]);
    SDEBUG("keys: " << keys["awsAccessKey"]);
    SDEBUG("keys: " << keys["awsSecretKey"]);
    SDEBUG("keys: " << keys["awsBucketName"]);

    // Check if we are loading into bootstrap mode.
    if (server.args.isSet("-bootstrap")) {
        SINFO("Loading in bootstrap mode.");
        if (details["manifestFileName"].empty()) {
            // It's an error to bootstrap with no given manifest. With no
            // manifest, we have no way of knowing what to download.
            SERROR("Loading into bootstrap mode with no manifest, exiting.");
        }

        if (SFileExists(details["databasePath"])) {
            SALERT("Bootstrapping is going to overwrite the database file: " << details["databasePath"]);
        }

        // Kick off our restore thread.
        {
            lock_guard<mutex> lock(operationMutex);
            operationInProgress = true;
        }
        thread(_beginRestore, ref(*this), server.args.test("-exitAfterRestore")).detach();
    }
}

BedrockPlugin_BackupManager::~BedrockPlugin_BackupManager()
{
}

STable BedrockPlugin_BackupManager::getInfo()
{
    STable info;
    info["version"] = VERSION;
    return info;
}

bool BedrockPlugin_BackupManager::preventAttach()
{
    lock_guard<mutex> lock(operationMutex);
    return operationInProgress;
}

bool BedrockPlugin_BackupManager::serverDetached()
{
    return server.isDetached();
}

// If we're a member of a cluster, make sure we are not going to lose quorum
// as a result of the backup.
bool BedrockPlugin_BackupManager::canBackup()
{
    list<STable> peerData;
    while (!server.getPeerInfo(peerData)) {
        SHMMM("Got empty peerInfo, probably a result of a mutex timeout, trying again.");
    }

    // If we're a single node cluster or if we're a permafollower, go ahead and back up.
    if (!localArgs.isSet("-peerList") && peerData.empty()) {
        SINFO("We're the only member of this cluster, backing up!");
        return true;
    } else if (stoi(localArgs["-priority"]) == 0) {
        SINFO("We're a permafollower, backing up!");
        return true;
    }

    // Calculate if it's safe or not to go offline for a backup. If we would be
    // the last quorum member online, we can't back up yet.
    int fullPeers = 0;
    int onlineFullPeers = 0;
    for (auto& peer: peerData) {
        // Count all of the online full peers, which is a peer that's logged in and either following or leading.
        if (peer["LoggedIn"] == "true" && (peer["State"] == SQLiteNode::stateName(SQLiteNode::FOLLOWING) ||
                                           peer["State"] == SQLiteNode::stateName(SQLiteNode::LEADING)) &&
            peer["Permafollower"] != "true") {
            onlineFullPeers += 1;
        }

        // Count all of the peers that aren't marked as a permafollower
        if (peer["Permafollower"] != "true") {
            fullPeers += 1;
        }
    }

    // If the number of online peers is higher than half + 1 full peers, then we
    // can safely go offline to do a backup.
    SINFO("Trying to run a backup, onlineFullPeers: " << onlineFullPeers << " fullPeers: " << fullPeers << " quroum: " << (fullPeers / 2 + 1));
    return onlineFullPeers >= (fullPeers / 2 + 1);
}

bool BedrockPlugin_BackupManager::peekCommand(SQLite& db, BedrockCommand& command)
{
    SData& request = command.request;
    SData& response = command.response;
    SDEBUG("Peeking at '" << request.methodLine << "'");

    if (SIEquals(request.getVerb(), "BeginBackup")) {
        // We only allow this command to be called from localhost.
        if (!request["_source"].empty()) {
            SWARN("Got command " << request.getVerb() << " from non-localhost source: " << request["_source"]);
            STHROW("401 Unauthorized");
        }

        // We require a 64 char length encryption key
        verifyAttributeSize(request, "key", 64, 64);

        if (!canBackup()) {
            STHROW("501 Unable to backup, not enough peers");
        }

        // Tell bedrock to detach from the database so we can copy it without changes.
        server.setDetach(true);

        // Allow for specifying an amount of threads different from the command line.
        if (request.isSet("threads")) {
            localArgs["-workerThreads"] = request["threads"];
            SINFO("Using worker threads " << localArgs["-workerThreads"]);
        }

        // We use 10MB chunks or the size given in the backup command.
        if (request.isSet("chunkSize")) {
            details["chunkSize"] = request["chunkSize"];
        } else {
            details["chunkSize"] = to_string(10485760);
        }
        SINFO("Using chunk size " << details["chunkSize"]);

        details["encryptionKey"] = SStrFromHex(request["Key"]);
        details["encryptionKeyString"] = request["Key"];
        SASSERT(details["encryptionKey"].size() == SAES_KEY_SIZE);
        details["randomNumber"] = SToStr(SRandom::rand64());
        details["date"] = SComposeTime("%Y%m%d/%H", STimeNow());

        // Generate the IV string for this backup.
        uint ivSize = 16;
        details["ivString"] = SRandom::randStr(ivSize);
        SASSERT(details["ivString"].size() == SAES_BLOCK_SIZE);
        SDEBUG("Encryption key value is: " << details["encryptionKeyString"]);
        SDEBUG("Encryption iv value is: " << details["ivString"]);

        // Generate the IV string for this manifest.
        details["manifestIV"] = SRandom::randStr(ivSize);
        SASSERT(details["manifestIV"].size() == SAES_BLOCK_SIZE);

        // Piece together our file name.
        details["manifestFileName"] = details["date"] + "/" +
            "manifest-" +
            details["hostname"] + "." +
            details["randomNumber"] + ".IV-" +
            details["manifestIV"] + ".json-enc";

        // Start the backup in a thread so that we don't have to block on returning
        // the results of this command to bedrock. This allows us to avoid any
        // timeouts in bedrock around expectations of how long a command will run.
        // Only run a backup if one is not currently in progress.
        {
            lock_guard<mutex> lock(operationMutex);
            if (operationInProgress) {
                STHROW("500 Already backing up.");
            }
            operationInProgress = true;
        }

        SDEBUG("keys: " << keys["manifestKey"]);
        SDEBUG("keys: " << keys["awsAccessKey"]);
        SDEBUG("keys: " << keys["awsSecretKey"]);
        SDEBUG("keys: " << keys["awsBucketName"]);
        thread(_beginBackup, ref(*this), request.test("ExitWhenComplete")).detach();

        // We're done here
        response["manifestFileName"] = details["manifestFileName"];
        return true;
    }
    return false;
}

void BedrockPlugin_BackupManager::_beginBackup(BedrockPlugin_BackupManager& plugin, bool exitWhenComplete)
{
    SInitialize("backup");

    uint64_t startTime = STimeNow();
    const string& fileName = plugin.details["databasePath"];

    // Wait until the server is done detaching
    SINFO("Waiting for server to detach.");
    while (!plugin.serverDetached()) {
        usleep(50000);
    }

    // Figure out the size of the file we're uploading
    uint64_t fromSize = SFileSize(fileName);
    if (!fromSize) {
        SWARN("File " << fileName << " is empty! Returning.");
        return;
    } else {
        SDEBUG("Filesize is: " << fromSize);
    }

    // If there's a wal or shm file present we need to wait until they are gone, or timeout is hit.
    uint64_t retries = 100;
    string walFile = fileName + "-wal";
    string shmFile = fileName + "-shm";

    // Try until there are no retries left or until the wal and shm files are gone or 0 bytes.
    while (retries && ((SFileExists(walFile) && SFileSize(walFile) > 0) || (SFileExists(shmFile) && SFileSize(walFile) > 0))) {
        // Open the DB and run a single query on it to cause a check point.
        sqlite3* _db;
        int walSizeFrames = 0;
        int framesCheckpointed = 0;
        SINFO((SFileExists(fileName) ? "Opening" : "Creating") << " database '" << fileName << "'.");
        const int DB_WRITE_OPEN_FLAGS = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
        SASSERT(!sqlite3_open_v2(fileName.c_str(), &_db, DB_WRITE_OPEN_FLAGS, NULL));

        // The second parameter of this API is the "name" of the database connection inside of sqlite, which
        // is explicitly different than the file name. Unless you have a sqlite3 db connection with multiple databases
        // attached, this is always "main".
        int result = sqlite3_wal_checkpoint_v2(_db, "main", SQLITE_CHECKPOINT_TRUNCATE, &walSizeFrames, &framesCheckpointed);
        if (result == SQLITE_OK) {
            SINFO("Truncate checkpoint successful, checkpointed " << framesCheckpointed << " of " << walSizeFrames);
        } else {
            SWARN("Unable to complete truncate checkpoint, result: " << result << ", extended error code: " << sqlite3_extended_errcode(_db) << " error message: " << sqlite3_errmsg(_db) << " will try " << retries << " more times");
        }

        // Then close it back up.
        SINFO("Closing database '" << fileName << ".");
        SASSERT(!sqlite3_close(_db));
        SINFO("Database closed.");
        retries--;
    }

    if (SFileExists(walFile) || SFileExists(shmFile)) {
        SWARN("We still have a wal or shm file after checkpointing. wal size: " << SFileSize(walFile) << " shm size: " << SFileSize(shmFile));
    }

    size_t chunkSize = stoull(plugin.details["chunkSize"]);
    SINFO(fileName << " is " << fromSize << " bytes, which means we should upload " << fromSize / chunkSize << " chunks");

    // Thread safe variables to be shared across our threads.
    atomic<size_t> chunkNumber(0);
    atomic<int> completePercent(0);
    atomic<bool> readAny(false);
    atomic<uint64_t> completeBytes(0);
    list<thread> uploadThreadList;

    // Detect the amount of threads this server can handle, at a minimum 1 thread.
    uint threadsToUse = plugin.localArgs.isSet("-workerThreads") ? stoi(plugin.localArgs["-workerThreads"]) : thread::hardware_concurrency();
    int workerThreads = max(1u, threadsToUse);
    for (int threadId = 0; threadId < workerThreads; threadId++) {
        uploadThreadList.emplace_back([&, threadId]() {
            SInitialize("uploadWorker" + to_string(threadId));

            // Open the file up, every thread has it's own handle file operations
            // on Ubuntu have internal locks, so anything that requires a lock
            // will happen inside of the file functions.
            FILE* from = fopen(fileName.c_str(), "rb");
            if (!from) {
                SWARN("Couldn't open file " << fileName << " for reading. Error: " << errno << ", " << strerror(errno) << ".");
                return;
            }
            SINFO("Successfully opened " << fileName << " for reading.");

            // Create an S3 connection to poll for data.
            S3 s3(plugin.keys["awsAccessKey"], plugin.keys["awsSecretKey"], plugin.keys["awsBucketName"]);

            char* buf = new char[chunkSize];
            while (!plugin.shouldExit) {
                // Read chunk off of the database file
                size_t myChunkNumber = chunkNumber.fetch_add(1);
                size_t myOffset = myChunkNumber * chunkSize;

                if (myOffset > fromSize) {
                    SINFO("Offset " << myOffset << " for chunkNumber " << myChunkNumber << " is higher than the file size, we're done! Closing the file.");
                    break;
                }

                // Seek to our offset
                uint64_t seekStart = STimeNow();
                fseek(from, myOffset, SEEK_SET);
                SINFO("[performance] Seeking to offset " << myOffset << " took " << (STimeNow() - seekStart) << "us");

                // Record how much we read so we can store it with this file
                size_t numRead = fread(buf, 1, chunkSize, from);

                // Upload the chunk we read
                plugin._processFileChunkUpload(buf, numRead, myOffset, to_string(myChunkNumber), s3);

                completeBytes.fetch_add(numRead);
                int percent = fromSize ? ((completeBytes * 100) / fromSize) : 0;
                if (percent > completePercent) {
                    SINFO("Uploading " << fileName << " to S3 is " << percent << "% complete.");
                    completePercent = percent;
                }

                // See if we failed, or if we hit EOF.
                if (ferror(from)) {
                    SWARN("Failure reading from " << fileName << " Error: " << errno << ", " << strerror(errno) << ".");
                    // No longer in `while`.
                    break;
                }
            }
            delete buf;
            // Close the file back up, we're done here.
            SINFO("Closing the file.");
            fclose(from);
        });
    }

    int threadId = 0;
    SINFO("Done reading from " << fileName << " joining our uploadThreads.");
    for (auto& uploadThread : uploadThreadList) {
        SINFO("Joining upload thread '" << "uploadThread" << threadId << "'");
        threadId++;
        uploadThread.join();
    }

    // Generate and upload the manifest for this backup.
    plugin._saveManifest();

    // We're done!
    {
        // Need to unset operationInProgress before we join or we'll never join.
        lock_guard<mutex> lock(plugin.operationMutex);
        plugin.operationInProgress = false;
        plugin.shouldExit = false;
    }

    // Reattach the db if we're not shutting down.
    if (exitWhenComplete) {
        plugin.server.shutdownWhileDetached = true;
    } else {
        plugin.server.setDetach(false);
    }

    SINFO("Backup is complete, took: " << (STimeNow() - startTime) / 1'000'000 << " seconds.");
}

void BedrockPlugin_BackupManager::_beginRestore(BedrockPlugin_BackupManager& plugin, bool exitWhenComplete)
{
    SInitialize("restore");

    uint64_t startTime = STimeNow();
    const string& fileName = plugin.details["databasePath"];

    // Create the database file. If we have an old (probably corrupted) database file,
    // this will delete it. Otherwise, it creats it so we can write to it below.
    FILE* create = fopen(fileName.c_str(), "wb");
    if (!create) {
        SWARN("Couldn't open file " << fileName << " for writing. Error: " << errno << ", " << strerror(errno) << ".");
        return;
    }
    // We've created it, now we'll close it up and reopen in each thread for update.
    fclose(create);
    string walFile = fileName + "-wal";
    string shmFile = fileName + "-shm";

    // If there's a wal or shm file present we need to delete those
    if (SFileExists(walFile)) {
        unlink(walFile.c_str());
    }
    if (SFileExists(shmFile)) {
        unlink(shmFile.c_str());
    }

    // Start by downloading the given manifest.
    plugin._downloadManifest();

    list<thread> downloadThreadList;

    // Detect the amount of threads this server can handle, at a minimum 1 thread.
    uint threadsToUse = plugin.localArgs.isSet("-workerThreads") ? stoi(plugin.localArgs["-workerThreads"]) : thread::hardware_concurrency();
    int workerThreads = max(1u, threadsToUse);
    for (int threadId = 0; threadId < workerThreads; threadId++) {
        downloadThreadList.emplace_back([&, threadId]() {
            SInitialize("downloadWorker" + to_string(threadId));

            // Create an S3 connection to poll for data.
            S3 s3(plugin.keys["awsAccessKey"], plugin.keys["awsSecretKey"], plugin.keys["awsBucketName"]);

            // Each thread needs it's own file handle, or else another thread could call seek
            // before we call fwrite, causing this thread to write to a location other than where it seeked.
            FILE* to = fopen(fileName.c_str(), "rb+");
            if (!to) {
                SWARN("Couldn't open file " << fileName << " for writing. Error: " << errno << ", " << strerror(errno) << ".");
                return;
            }
            SINFO("Successfully opened " << fileName << " for writing.");

            while (!plugin.shouldExit) {
                // Get a fileName and the associated JSON details.
                string fileName, fileDetailsJSON;

                // Lock here to prevent multiple threads from getting the
                // same file. Erase the file as soon as we have the details
                // pulled out.
                {
                    lock_guard<mutex> lock(plugin.fileManifestMutex);
                    auto it = plugin.fileManifest.begin();
                    if (it != plugin.fileManifest.end()) {
                        fileName = it->first;
                        fileDetailsJSON = it->second;
                        plugin.fileManifest.erase(it);
                    } else {
                        SINFO("No more files to download, we're done!");
                        return;
                    }
                }

                STable fileDetails = SParseJSONObject(fileDetailsJSON);
                size_t offset = stoull(fileDetails["offset"]);
                size_t size = stoull(fileDetails["decryptedFileSize"]);
                size_t gzippedFileSize = stoull(fileDetails["gzippedFileSize"]);
                string fileHash = fileDetails["chunkHash"];

                SINFO("File " << fileName << " details, offset: " << offset << ", size: "
                      << size << ", gzippedFileSize: " << gzippedFileSize << " fileHash: "
                      << fileHash);

                string buffer = plugin._processFileChunkDownload(fileName, size, gzippedFileSize, s3, fileHash);

                // Check that the buffer returned is not completely NULL.
                // This is to help us debug how we're possibly writing a full NULL
                // chunk, even though the hashes matched inside of _processFileChunkDownload.
                if (_isZero(buffer.c_str(), size)) {
                    SERROR("Chunk " << fileName << " is completely NULL before writing.");
                }

                if (!fseek(to, offset, SEEK_SET)) {
                    SDEBUG("Seek successful.");
                } else {
                    if (ferror(to)) {
                        SWARN("Error " << strerror(errno) << " seeking to offset " << offset);
                        fclose(to);
                    }
                    SERROR("Seeking to " << offset << " failed! Aborting.");
                }
                size_t written = fwrite(buffer.c_str(), sizeof(char), buffer.size(), to);

                // Check that the amount written is what it should be.
                // This is to help us debug how we're possibly writing a full NULL
                // chunk, even though the hashes matched inside of _processFileChunkDownload.
                SASSERT(written == size);

                if (ferror(to)) {
                    SWARN("Error " << strerror(errno) << " writing to file!");
                    fclose(to);
                }
            }
            // Done with the file, close it up.
            fclose(to);
            SINFO("Done restoring! Closing file " << fileName);
        });
    }

    int threadId = 0;
    SINFO("Done writing to " << fileName << ", joining our downloadThreads.");
    for (auto& downloadThread : downloadThreadList) {
        SINFO("Joining download thread '" << "downloadThread" << threadId << "'");
        threadId++;
        downloadThread.join();
    }

    SINFO("Bootstrap is complete, took: " << (STimeNow() - startTime) / 1'000'000 << " seconds.");

    {
        lock_guard<mutex> lock(plugin.operationMutex);
        plugin.operationInProgress = false;
        plugin.shouldExit = false;
    }

    // Tell BedrockServer we're ready to go, or shut down.
    if (exitWhenComplete) {
        plugin.server.shutdownWhileDetached = true;
    } else {
        plugin.server.setDetach(false);
    }
}

void BedrockPlugin_BackupManager::_downloadManifest()
{
    const string& fileName = details["manifestFileName"];

    // Get the IV from the file name
    string manifestIV = SAfterUpTo(fileName, "IV-", ".");

    // Create an S3 connection to poll for data.
    S3 s3(keys["awsAccessKey"], keys["awsSecretKey"], keys["awsBucketName"]);

    // Download our manifest
    SHTTPSManager::Transaction* downloadRequest = s3.download(fileName);

    // Wait for a response.
    SASSERT(downloadRequest);

    // Poll until this request has a response.
    _poll(s3, downloadRequest);

    // See if we exited early.
    if (!downloadRequest->response) {
        SINFO("Notified we should exit, returning early.");
        s3.closeTransaction(downloadRequest);
        return;
    }

    // We've got a response, check the status code. Throw if not 200.
    int httpsResponseCode = downloadRequest->response;
    const string responseContent = move(downloadRequest->fullResponse.content);
    SINFO("Received " << httpsResponseCode << " for fileName " << fileName);

    s3.closeTransaction(downloadRequest);

    if (httpsResponseCode != 200) {
        SALERT("507 S3 Request returned: " << httpsResponseCode << " when downloading " << fileName);
        SALERT("S3 full error was: " << responseContent);
        shouldExit = true;
        STHROW("500 Unable to download file.");
    }

    string decryptedManifest = SAESDecrypt(responseContent, manifestIV, details["manifestKey"]);

    // Parse out the file we get back.
    STable backupManifest = SParseJSONObject(decryptedManifest);
    details["date"] = backupManifest["date"];
    details["encryptionKey"] = SStrFromHex(backupManifest["encryptionKeyString"]);
    details["ivString"] = backupManifest["ivString"];
    details["randomNumber"] = backupManifest["randomNumber"];

    // Turn our manifest files list into JSON.
    lock_guard<mutex> lock(fileManifestMutex);
    fileManifest = SParseJSONObject(backupManifest["files"]);
}

void BedrockPlugin_BackupManager::_saveManifest()
{
    // Take the manifest we have and add some information to it.
    STable finalManifest;
    finalManifest["date"] = details["date"];
    finalManifest["encryptionKeyString"] = details["encryptionKeyString"];
    finalManifest["ivString"] = details["ivString"];
    finalManifest["randomNumber"] = details["randomNumber"];

    // Add our files to the manifest.
    {
        lock_guard<mutex> lock(fileManifestMutex);
        finalManifest["files"] = SComposeJSONObject(fileManifest);

        // Delete the file manifest object so that if we re-run a new backup
        // this will be empty.
        fileManifest.clear();
    }

    // Compose a JSON out of all of our other JSON/info, ship it to S3.
    string finalManifestJSON = SComposeJSONObject(finalManifest);

    // Create an S3 connection to poll for data.
    S3 s3(keys["awsAccessKey"], keys["awsSecretKey"], keys["awsBucketName"]);

    SHTTPSManager::Transaction* uploadRequest = s3.upload(details["manifestFileName"], SAESEncrypt(finalManifestJSON, details["manifestIV"], details["manifestKey"]));

    // Wait for a response.
    SASSERT(uploadRequest);

    // Poll until this request has a response.
    _poll(s3, uploadRequest);

    // See if we exited early.
    if (!uploadRequest->response) {
        SINFO("Notified we should exit, returning early.");
        s3.closeTransaction(uploadRequest);
        return;
    }

    // We've got a response, check the status code. Throw if not 200.
    int httpsResponseCode = uploadRequest->response;
    const string responseContent = move(uploadRequest->fullResponse.content);
    SINFO("Received " << httpsResponseCode << " for fileName " << details["manifestFileName"]);

    // Close and free the transaction.
    s3.closeTransaction(uploadRequest);

    if (httpsResponseCode != 200) {
        SALERT("507 S3 Request returned: " << httpsResponseCode << " when uploading " << details["manifestFileName"]);
        SALERT("S3 full error was: " << responseContent);
        shouldExit = true;
    }
}

void BedrockPlugin_BackupManager::_poll(S3& s3, SHTTPSManager::Transaction* request)
{
    while (!request->response && !shouldExit) {
        // Our fdm holds a list of all sockets we could need to read or write to
        fd_map fdm;
        const uint64_t& nextActivity = STimeNow();
        _prePoll(fdm, s3);
        S_poll(fdm, 1'000);
        _postPoll(fdm, nextActivity, s3);
    }
}

void BedrockPlugin_BackupManager::_postPoll(fd_map& fdm, uint64_t nextActivity, S3& s3)
{
    list<SHTTPSManager::Transaction*> completedHTTPSRequests;
    s3.postPoll(fdm, nextActivity, completedHTTPSRequests);
}

void BedrockPlugin_BackupManager::_prePoll(fd_map& fdm, S3& s3)
{
    s3.prePoll(fdm);
}

string BedrockPlugin_BackupManager::_processFileChunkDownload(const string& fileName, size_t& fileSize, size_t& gzippedFileSize, S3& s3, string fileHash)
{
    // Create a buffer for us to store the processed chunk in.
    string buffer;

    // Add a log prefix so we can track this chunk easily through the logs.
    SAUTOPREFIX("chunkNumber" + SAfterUpTo(fileName, details["randomNumber"] + "-", ".enc.gz"));

    // Retry until we get a fatal error or we finish.
    while (true) {
        SINFO("Processing fileName " << fileName);

        // Create a download request for this file
        SHTTPSManager::Transaction* downloadRequest = s3.download(fileName);

        // Make sure we have a request, then add it to our list of outstanding requests.
        SASSERT(downloadRequest);

        // Poll until this request has a response.
        _poll(s3, downloadRequest);

        // See if we exited early.
        if (!downloadRequest->response) {
            SINFO("Notified we should exit, returning early.");
            s3.closeTransaction(downloadRequest);
            return "";
        }

        // We've got a response, check the status code. Throw if not 200.
        int httpsResponseCode = downloadRequest->response;
        const string responseContent = move(downloadRequest->fullResponse.content);
        SINFO("Received " << httpsResponseCode << " for fileName " << fileName);

        // Close and free the transaction.
        s3.closeTransaction(downloadRequest);

        if (httpsResponseCode != 200) {
            SHMMM("S3 Request returned: " << httpsResponseCode << " when downloading " << fileName << ", going to try again.");
            SHMMM("S3 full error was: " << responseContent);
            if (httpsResponseCode == 500 || httpsResponseCode == 501 || httpsResponseCode == 503) {
                SHMMM("Expensify timeout (Error " << httpsResponseCode << "), retrying chunk number: " << fileName);
                continue;
            } else if (httpsResponseCode == 400) {
                SHMMM("Amazon timeout, retrying chunk number: " << fileName);
                continue;
            } else if (httpsResponseCode == 404) {
                SALERT("S3 Returned 404, we can't find this file: " << fileName);
                shouldExit = true;
                break;
            } else {
                SWARN("Unhandled response (" << httpsResponseCode << ") for chunk " << fileName << ", exiting.");
                shouldExit = true;
                break;
            }
        }

        // Decrypt the file chunk.
        uint64_t startTime = STimeNow();
        string decryptedChunk = SAESDecryptNoStrip(responseContent, responseContent.size(), details["ivString"], details["encryptionKey"]);
        uint64_t decryptTime = STimeNow();

        // If the size of the decrypted chunk is longer than the size of the gzipped
        // chunk before encryption, then we need to strip off padding.
        if (decryptedChunk.size() > gzippedFileSize) {
            if ((decryptedChunk.size() - gzippedFileSize) < 16) {
                SINFO("We're stripping " << (decryptedChunk.size() - gzippedFileSize) << " bytes off of the chunk.");
            } else {
                SALERT("Whoa we stripped " << (decryptedChunk.size() - gzippedFileSize) << "  off of this chunk: "
                       << fileName << ", which is more than the max padding amount, something is very wrong.");
                shouldExit = true;
            }
            decryptedChunk.resize(gzippedFileSize);
        }

        buffer = SGUnzip(decryptedChunk);
        uint64_t gzipTime = STimeNow();
        SINFO("[performance] Chunk " << fileName << " took "
              << (gzipTime - startTime) / 1'000 << "ms to process, "
              << (decryptTime - startTime) / 1'000 << "ms was spent decrypting "
              << (gzipTime - decryptTime) / 1'000 << "ms was spent gzipping.");

        string bufferHash = SToHex(SHashSHA256(buffer));

        if (bufferHash == fileHash) {
            SINFO("Hashes match! Got hash: " << bufferHash << " for file " << fileName << ", manifest listed " << fileHash);
        } else if (_isZero(buffer.c_str(), fileSize)) {
            SALERT("We have a full NULL chunk, something must be very wrong.");
        } else if (bufferHash != fileHash) {
            SALERT("Got hash: " << bufferHash << " for file " << fileName << " but manifest listed " << fileHash);
            shouldExit = true;
            STHROW("500 Hash mismatch.");
        }

        // Done, don't need to retry again.
        break;
    }
    return buffer;
}

void BedrockPlugin_BackupManager::_processFileChunkUpload(char* fileChunk, size_t& fromSize,
                                                                   size_t& chunkOffset, const string& chunkNumber, S3& s3)
{
    // Makes for easy retries.
    string encryptedFileChunk;

    // Add a log prefix so we can track this chunk easily through the logs.
    SAUTOPREFIX("chunkNumber" + chunkNumber);

    while (1) {
        SINFO("Processing chunkNumber " << chunkNumber);
        const string& fileName = details["date"] + "/" +
            details["hostname"] + "." +
            details["randomNumber"] + "-" +
            chunkNumber + ".gz.enc";

        // We'll assign this later, we declare it here to keep it in scope.
        SHTTPSManager::Transaction* chunkRequest = nullptr;
        size_t gzippedFileSize = 0;
        string chunkHash = SToHex(SHashSHA256(string(fileChunk, fromSize)));
        SINFO("Hashed chunk " << chunkNumber << " to " << chunkHash);
        if (chunkHash == SToHex(SHashSHA256(""))) {
            SALERT("We hashed a NULL chunk, something must be very wrong.");
        }

        // Scoped to release all this memory as soon as possible.
        // Also, don't bother if we've already done this (i.e., we're retrying).
        uint64_t startTime = 0, gzipTime = 0, encryptTime = 0;
        if (encryptedFileChunk.empty()) {
            // This constructor specifies the amount of bytes to convert from c string to string
            // because otherwise it will stop at the first null byte it encounters. Since we aren't copying
            // an actual string, we will almost always encounter one before the end of the c string.
            // Gzip then encrypt the file chunk.
            startTime = STimeNow();
            string gzippedFileChunk = SGZip(string(fileChunk, fromSize));
            gzipTime = STimeNow();
            gzippedFileSize = gzippedFileChunk.size();
            string encryptedFileChunk = SAESEncrypt(gzippedFileChunk, details["ivString"], details["encryptionKey"]);
            encryptTime = STimeNow();

            chunkRequest = s3.upload(fileName, encryptedFileChunk);
        }
        SINFO("[performance] Chunk " << chunkNumber << " took "
              << (encryptTime - startTime) / 1'000 << "ms to process, "
              << (gzipTime - startTime) / 1'000 << "ms was spent gzipping "
              << (encryptTime - gzipTime) / 1'000 << "ms was spent encrypting");

        // Ensure we have a request object then add it to our list.
        SASSERT(chunkRequest);

        // Poll until this request has a response.
        _poll(s3, chunkRequest);

        // See if we exited early.
        if (!chunkRequest->response) {
            SINFO("Notified we should exit, returning early.");
            s3.closeTransaction(chunkRequest);
            return;
        }

        // Check what response we got, throw if we didn't get a 200 OK
        int httpsResponseCode = chunkRequest->response;
        SINFO("Received " << httpsResponseCode << " for chunkNumber " << chunkNumber << ".");

        if (httpsResponseCode != 200) {
            SHMMM("S3 Request returned: " << httpsResponseCode);
            SHMMM("S3 full error was: " << chunkRequest->fullResponse.content);
            if (httpsResponseCode == 400) {
                SHMMM("Amazon timeout, retrying chunk number: " << chunkNumber);
                s3.closeTransaction(chunkRequest);
                continue;
            } else if (httpsResponseCode == 500 || httpsResponseCode == 501 || httpsResponseCode == 503) {
                SHMMM("Expensify timeout (Error " << httpsResponseCode << "), retrying chunk number: " << chunkNumber);
                s3.closeTransaction(chunkRequest);
                continue;
            } else {
                SWARN("Unhandled response (" << httpsResponseCode << ") for chunk " << chunkNumber << ", exiting.");
                shouldExit = true;
                s3.closeTransaction(chunkRequest);
                break;
            }
            server.setDetach(false);
        }

        // Store some details about this chunk.
        STable fileDetails;
        fileDetails["decryptedFileSize"] = fromSize;
        fileDetails["offset"] = chunkOffset;
        fileDetails["gzippedFileSize"] = gzippedFileSize;
        fileDetails["chunkHash"] = chunkHash;

        // Lock to make sure we're the only ones modifying the file list right now.
        // Turn our details into JSON so we can easily parse it when we download the
        // manifest.
        {
            lock_guard<mutex> lock(fileManifestMutex);
            fileManifest[fileName] = SComposeJSONObject(fileDetails);
        }

        // Close and free the transaction.
        s3.closeTransaction(chunkRequest);

        // Done, don't need to retry.
        break;
    }
}

bool BedrockPlugin_BackupManager::_isZero(const char* c, uint64_t length)
{
    if (length) {
        for (uint64_t i = 0; i < length; i++) {
            if (c[i] != 0) {
                return false;
            }
        }
    }
    return true;
}
