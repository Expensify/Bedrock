#pragma once
#include <BedrockPlugin.h>
#include <BedrockServer.h>
#include <bedrockVersion.h>
#include <libstuff/libstuff.h>
#include <libstuff/sqlite3.h>
#include "S3.h"

class BedrockPlugin_BackupManager : public BedrockPlugin {
public:
    // Constructor
    BedrockPlugin_BackupManager(BedrockServer& s);

    // Destructor
    ~BedrockPlugin_BackupManager();

    bool peekCommand(SQLite& db, BedrockCommand& command);

    virtual string getName()
    {
        return "BackupManager";
    }

    bool preventAttach();

    // Returns info back to BedrockServer
    STable getInfo();

    bool serverDetached();

    bool canBackup();

private:
    SData localArgs;
    STable keys;

    // Used to store details for backup/restores.
    STable details;

    // Used to prevent two backups from running at the same time.
    bool operationInProgress;
    mutex operationMutex;

    // Mutex to wrap our fileMainfest. Necessary because any thread could be
    // attempting to modify the manifest at any given time.
    mutex fileManifestMutex;

    // An STable containing all of our file piece and details (size and offset)
    // for each piece. For uploads we append to this manifest in each thread
    // then turn it into JSON when we call _saveManifest. For downloads
    // we read this STable out of the downloaded manifest and use it to
    // download the correct files and know the given details for each file.
    STable fileManifest;

    // Wrapper function that spawns the upload worker threads. Detaches the
    // database by sending a `Detach` command to bedrock. Once we're done it
    // sends an `Attach` command, to let bedrock know we're done.
    static void _beginBackup(BedrockPlugin_BackupManager& plugin, bool exitWhenComplete = false);

    // Wrapper function that spawns our restore worker threads. Owns the single
    // file pointer for writing out out database.
    static void _beginRestore(BedrockPlugin_BackupManager& plugin, bool exitWhenComplete = false);

    // Internal function for downloading the JSON manifest from S3 and
    // starting a bootstrap.
    void _downloadManifest();

    // Internal function for generating the JSON manifest file for the backup.
    void _saveManifest();

    // Wrapper function to loop over our wrapper functions in a thread.
    void _poll(S3& s3, SHTTPSManager::Transaction* request);

    // Wrappers for this plugin that just call the base class of the HTTPSManager.
    void _prePoll(fd_map& fdm, S3& s3);
    void _postPoll(fd_map& fdm, uint64_t nextActivity, S3& s3);

    // Internal function to download, gunzip, and decrypt a given file from
    // the manifest. This function is called in worker threads so all operations
    // need to be thread safe.
    string _processFileChunkDownload(const string& fileName, size_t& fileSize, size_t& gzippedFileSize, S3& s3, string fileHash);

    // Internal function to encrypt, gzip, and upload a given file chunk from
    // the database. Starts by creating a multipart upload in S3, then chunking
    // the file into 5MB pieces and uploading each one as a "part" of the database
    // chunk. Once it's finished it "finishes" the multipart upload which causes
    // S3 to put all of the pieces together on their end. Finally, it adds the
    // file details to the fileManifest. This function is called in worker threads
    // so all operations need to be thread safe.
    void _processFileChunkUpload(char* fileChunk, size_t& fromSize,
                                 size_t& chunkOffset, const string& chunkNumber, S3& s3);

    // Lets a thread tell all the others that it's broken and everyone should exit.
    atomic<bool> shouldExit;

    static bool _isZero(const char* c, uint64_t size);
};
