#include <libstuff/libstuff.h>
#include "S3.h"

S3::S3(const string& awsAccessKey, const string& awsSecretKey, const string& awsBucketName) :
    SStandaloneHTTPSManager()
{
    _keys["awsAccessKey"] = awsAccessKey;
    _keys["awsSecretKey"] = awsSecretKey;
    _keys["awsBucketName"] = awsBucketName;
    SASSERT(!_keys["awsAccessKey"].empty());
    SASSERT(!_keys["awsSecretKey"].empty());
    SASSERT(!_keys["awsBucketName"].empty());
}

S3::~S3()
{
    // Do nothing.
}

SHTTPSManager::Transaction* S3::download(const string& fileName)
{
    return _sendRequest("GET", fileName);
}

SHTTPSManager::Transaction* S3::upload(const string& fileName, const string& file)
{
    return _sendRequest("PUT", fileName, file);
}

SHTTPSManager::Transaction* S3::_sendRequest(const string& method, const string& fileName, const string& file)
{
    SData request = _buildRequest(method, fileName, file);
    const string& awsBucketName = _keys.find("awsBucketName")->second;
    SASSERT(!awsBucketName.empty());
    string BUCKET_URL = "https://" + awsBucketName + "." + S3_URL;
    return _httpsSendRawData(BUCKET_URL, request);
}

SData S3::_buildRequest(const string& method, const string& fileName, const string& file)
{
    const string& awsAccessKey = _keys.find("awsAccessKey") != _keys.end() ? _keys.find("awsAccessKey")->second : "";
    const string& awsSecretKey = _keys.find("awsSecretKey") != _keys.end() ? _keys.find("awsSecretKey")->second : "";
    const string& awsBucketName = _keys.find("awsBucketName") != _keys.end() ? _keys.find("awsBucketName")->second : "";

    SASSERT(!awsAccessKey.empty());
    SASSERT(!awsSecretKey.empty());
    SASSERT(!awsBucketName.empty());

    uint64_t currentTime = STimeNow();
    string scope = SComposeTime("%Y%m%d", currentTime) + "/us-east-1/s3/aws4_request";

    // Set up all our standard headers, including our authentication.
    SData request;
    request.methodLine = method + " /" + awsBucketName + "/" + fileName + " HTTP/1.1";
    request["Host"] = S3_URL;
    request["X-Amz-Date"] = SComposeTime("%Y%m%dT%H%M%SZ", currentTime);

    // We have to provide this even if there is no file, in the case of no file (i.e. GETs)
    // We hash an empty string.
    request["X-Amz-Content-Sha256"] = SToHex(SHashSHA256(file));

    // If we have a file, we need to add it and our content length to the request.
    if (file.size()) {
        request["Content-Length"] = to_string(file.size());
        request.content = file;
    }

    // Process all of our headers for signing.
    STable signatures = _signRequest(request, awsSecretKey, scope);

    // Put all of the pieces together for the Authorization header
    request["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + awsAccessKey + "/" + scope + ",SignedHeaders="
        + signatures["signedHeaders"] + ",signature=" + SToLower(signatures["requestSignature"]);
    return request;
}

STable S3::_signRequest(const SData& request, const string& awsSecretKey, const string& scope)
{
    // Turn our canonicalQueryString into a single decoded string and a URI
    string canonicalURI;
    STable canonicalQueries;
    string canonicalQueryString;
    SParseURIPath(STrim(SBefore(request.methodLine, "HTTP/1.1")), canonicalURI, canonicalQueries);
    for (auto& canonicalQuery : canonicalQueries) {
        canonicalQueryString += canonicalQuery.first + "=" + canonicalQuery.second + "&";
    }

    // Remove the trailing & and the method from our URI
    if (!canonicalQueryString.empty()) {
        canonicalQueryString.pop_back();
    }
    canonicalURI = STrim(SAfter(canonicalURI, request.getVerb()));

    // We need to keep a list of the headers we are signing. This will vary depending on the type of
    // request i.e. GET vs PUT. We also need to concatenate all of the headers and their values on a new
    // line. Our request headers MUST be sorted before they are signed or our signature will not match the AWS
    // signature. The `map` that they are stored in has already sorted them for us.
    string signedHeaders;
    string canonicalHeaders;
    for (auto& nameValuePair : request.nameValueMap) {
        string name = SToLower(nameValuePair.first);
        signedHeaders += name + ";";
        canonicalHeaders += name + ":" + nameValuePair.second + "\n";
    }

    // The table we'll return when all is said and done.
    STable signatures;

    // Remove the trailing ';', add our signed headers to the table
    signedHeaders.pop_back();
    signatures["signedHeaders"] = signedHeaders;

    // compute the signing key
    string dateKey = SHMACSHA256(string("AWS4" + awsSecretKey), SBefore(request["X-Amz-Date"], "T"));
    string dateRegionKey = SHMACSHA256(dateKey, "us-east-1");
    string dateRegionServiceKey = SHMACSHA256(dateRegionKey, "s3");
    string signingKey = SHMACSHA256(dateRegionServiceKey, "aws4_request");

    // compute our canonical request to sign
    string canonicalRequest = request.getVerb() + "\n" + canonicalURI + "\n" +
        (!canonicalQueryString.empty() ? canonicalQueryString : "") +
        "\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + request["X-Amz-Content-Sha256"];

    // turn our canonical request into a string that needs signing
    string seedStringToSign = "AWS4-HMAC-SHA256\n" + request["X-Amz-Date"] + "\n" + scope + "\n" +
        SToLower(SToHex(SHashSHA256(canonicalRequest)));

    // use our computed signing key to sign the string.
    string seedSignature = SToHex(SHMACSHA256(signingKey, seedStringToSign));

    // Add our request signature to the table.
    signatures["requestSignature"] = seedSignature;
    return signatures;
}

string S3::_composeS3HTTP(const SData& request)
{
    // S3 Requests require signed headers, so we can't modify any of them here.
    // If the values are changed after the signature is computed, then the signature
    // won't match when S3 tries to calculate it.

    // Just walk across and compose a valid HTTP-like message
    string buffer;
    buffer += request.methodLine + "\r\n";
    for (auto& item : request.nameValueMap) {
        buffer += item.first + ": " + SEscape(item.second, "\r\n\t") + "\r\n";
    }

    // Finish the message and add the content, if any
    buffer += "\r\n";
    buffer += request.content;

    return buffer;
}

SHTTPSManager::Transaction* S3::_httpsSendRawData(const string& url, const SData& request)
{
    // Open a connection, optionally using SSL (if the URL is HTTPS). If that doesn't work, then just return a
    // completed transaction with an error response.
    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    SX509* _x509 = SStartsWith(url, "https://") ? SX509Open(_pem, _srvCrt, _caCrt) : nullptr;
    Socket* s = openSocket(host, SStartsWith(url, "https://") ? _x509 : 0);
    if (!s) {
        return _createErrorTransaction();
    }

    // Wrap in a transaction
    Transaction* transaction = new Transaction(*this);
    transaction->s = s;

    // We set these for logging purposes, but leave the body blank to reduce memory usage.
    transaction->fullRequest.methodLine = request.methodLine;
    transaction->fullRequest.nameValueMap = request.nameValueMap;

    // Ship it.
    transaction->s->send(_composeS3HTTP(request));

    // Keep track of the transaction.
    lock_guard<recursive_mutex> lock(_listMutex);
    _activeTransactionList.push_front(transaction);
    return transaction;
}
