#include "AMILicenseCheck.h"
#include "LicenseUtil.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int LICENSE_ERROR;
}
}

namespace MyscaleLicense
{

void AmazonInstanceMetadata::init()
{
    auto token = getAmazonHttpToken();
    instance_id = getAmazonInstanceId();
    dynamic_document = getAmazonDynamicDocument(token);
    signature = getAmazonSignature(token);
}

String AmazonInstanceMetadata::getAmazonHttpToken() const
{
    try
    {
        Poco::URI uri("http://" + AMAZON_EC2_METADATA_SERVER_IP + AMAZON_EC2_TOKEN_URL);
        Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());

        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
        request.set("X-aws-ec2-metadata-token-ttl-seconds", "120");

        session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        std::istream & resStream = session.receiveResponse(response);

        std::stringstream ss;
        Poco::StreamCopier::copyStream(resStream, ss);
        return ss.str();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::LICENSE_ERROR, "Get Amazon http token failed: {}", e.displayText());
    }
}

String AmazonInstanceMetadata::getAmazonDynamicDocument(const String & token) const
{
    try
    {
        Poco::URI uri("http://" + AMAZON_EC2_METADATA_SERVER_IP + AMAZON_EC2_DYNAMIC_DATA_URL);
        Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());

        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
        request.set("X-aws-ec2-metadata-token", token);

        session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        std::istream & resStream = session.receiveResponse(response);

        std::stringstream ss;
        Poco::StreamCopier::copyStream(resStream, ss);
        return ss.str();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::LICENSE_ERROR, "Get Amazon dynamic document failed: {}", e.displayText());
    }
}

String AmazonInstanceMetadata::getAmazonSignature(const String & token) const
{
    try
    {
        Poco::URI uri("http://" + AMAZON_EC2_METADATA_SERVER_IP + AMAZON_EC2_SIGNATURE_URL);
        Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());

        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
        request.set("X-aws-ec2-metadata-token", token);

        session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        std::istream & resStream = session.receiveResponse(response);

        std::stringstream ss;
        Poco::StreamCopier::copyStream(resStream, ss);
        return ss.str();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::LICENSE_ERROR, "Get Amazon signature failed: {}", e.displayText());
    }
}

String AmazonInstanceMetadata::getAmazonInstanceId() const
{
    String cur_instance_id;
    if (fs::exists(AMAZON_EC2_INSTANCE_ID_FILE) && !fs::is_directory(AMAZON_EC2_INSTANCE_ID_FILE))
    {
        auto command = DB::ShellCommand::execute("cat " + AMAZON_EC2_INSTANCE_ID_FILE);
        readStringUntilEOF(cur_instance_id, command->out);
        trim(cur_instance_id, '\n');
        trim(cur_instance_id);
    }
    return cur_instance_id;
}

void AmazonInstanceMetadata::checkAmazonDocumentSignature() const
{
#ifdef USE_SSL
    static const std::unordered_map<String, String> AMAZON_REGION_CERTIFICATE_BASE64 = {
        {"us-east-1",
         "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURJVENDQW9xZ0F3SUJBZ0lVRTF5Mk5JS0NVK1JnNHV1NHUzMmtvRzlRRVlJd0RRWUpLb1pJaHZjTkFRRUwKQlFBd"
         "1hERUxNQWtHQTFVRUJoTUNWVk14R1RBWEJnTlZCQWdURUZkaGMyaHBibWQwYjI0Z1UzUmhkR1V4RURBTwpCZ05WQkFjVEIxTmxZWFIwYkdVeElEQWVCZ05WQkFvVEYwRn"
         "RZWHB2YmlCWFpXSWdVMlZ5ZG1salpYTWdURXhECk1CNFhEVEkwTURReU9URTNNelF3TVZvWERUSTVNRFF5T0RFM016UXdNVm93WERFTE1Ba0dBMVVFQmhNQ1ZWTXgKR1R"
         "BWEJnTlZCQWdURUZkaGMyaHBibWQwYjI0Z1UzUmhkR1V4RURBT0JnTlZCQWNUQjFObFlYUjBiR1V4SURBZQpCZ05WQkFvVEYwRnRZWHB2YmlCWFpXSWdVMlZ5ZG1salpY"
         "TWdURXhETUlHZk1BMEdDU3FHU0liM0RRRUJBUVVBCkE0R05BRENCaVFLQmdRQ0h2UmpmLzBrU3RwSjI0OGtodElhTjhxa0ROM3RrdzRWanZBOW52UGwyYW5KTytlSUIKV"
         "XFQZlFHMDlrWmx3cFdwbXlPOGJHQjJSV3FXeEN3dUIvZGNuSW9iNnc0MjBrOVdZNUMwSUlHdERSTmF1TjNrdQp2R1hrdzNIRW5GMEVqWXIwcGN5V1V2QnlXWTRLc3daVj"
         "QyWDdZN1hTUzEzaE9JY0w2TkxBK0g5NC9RSURBUUFCCm80SGZNSUhjTUFzR0ExVWREd1FFQXdJSGdEQWRCZ05WSFE0RUZnUVVKZGJNQ0JYS3R2Q2NXZHdVVWl6dnRVRjI"
         "KVVRnd2daa0dBMVVkSXdTQmtUQ0Jqb0FVSmRiTUNCWEt0dkNjV2R3VVVpenZ0VUYyVVRpaFlLUmVNRnd4Q3pBSgpCZ05WQkFZVEFsVlRNUmt3RndZRFZRUUlFeEJYWVhO"
         "b2FXNW5kRzl1SUZOMFlYUmxNUkF3RGdZRFZRUUhFd2RUClpXRjBkR3hsTVNBd0hnWURWUVFLRXhkQmJXRjZiMjRnVjJWaUlGTmxjblpwWTJWeklFeE1RNElVRTF5Mk5JS"
         "0MKVStSZzR1dTR1MzJrb0c5UUVZSXdFZ1lEVlIwVEFRSC9CQWd3QmdFQi93SUJBREFOQmdrcWhraUc5dzBCQVFzRgpBQU9CZ1FBbHhTbXdjV25oVDR1QWVTaW5KdXorMU"
         "JUY0toVlNXYjVqVDhwWWpRYjhab1prWFhSR2IwOW12WWVVCk5lcU9CcjI3cnZSQW5hUS85TFVRZjcyK1NhaERGdVM0Q01JOG53b3d5dHFibXdxdXFGcjRkeEEvU0RBRHl"
         "SaUYKZWExVW9NdU5IVFk0OUovMXZQb21xc1ZuN211Z1RwK1RianFDZk9KVHB1MHRlbUhjRkE9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg=="},
        {"cn-north-1",
         "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURDekNDQW5TZ0F3SUJBZ0lKQUxTT01iT29VMnN2TUEwR0NTcUdTSWIzRFFFQkN3VUFNRnd4Q3pBSkJnTlYKQkFZV"
         "EFsVlRNUmt3RndZRFZRUUlFeEJYWVhOb2FXNW5kRzl1SUZOMFlYUmxNUkF3RGdZRFZRUUhFd2RUWldGMApkR3hsTVNBd0hnWURWUVFLRXhkQmJXRjZiMjRnVjJWaUlGTm"
         "xjblpwWTJWeklFeE1RekFlRncweU16QTNNRFF3Ck9ETTFNemxhRncweU9EQTNNREl3T0RNMU16bGFNRnd4Q3pBSkJnTlZCQVlUQWxWVE1Sa3dGd1lEVlFRSUV4QlgKWVh"
         "Ob2FXNW5kRzl1SUZOMFlYUmxNUkF3RGdZRFZRUUhFd2RUWldGMGRHeGxNU0F3SGdZRFZRUUtFeGRCYldGNgpiMjRnVjJWaUlGTmxjblpwWTJWeklFeE1RekNCbnpBTkJn"
         "a3Foa2lHOXcwQkFRRUZBQU9CalFBd2dZa0NnWUVBCnVoaFVObHFBWmRjV1dCL09TRFZER2szT0E5OUVGek9uL21KbG1jaVEvWHd1MmRGSldtU0NxRUFFNmdqdWZDalEKc"
         "TN2b3hBaEMyQ0YrZWxLdEpXL0MwU3ovTFlvNjBQVXFkNmlYRjRoK3VwQjlIa09PR3VXSFhzSEJUc3Zna2dHQQoxQ0dnZWw0VTBDZHErMjNlQU5yOE44bTI4VXpsampTbl"
         "RscllDSHR6TjRzQ0F3RUFBYU9CMURDQjBUQUxCZ05WCkhROEVCQU1DQjRBd0hRWURWUjBPQkJZRUZCa1p1M3dUMjdObllncmZIK3hKejRISmFOSm9NSUdPQmdOVkhTTUU"
         "KZ1lZd2dZT0FGQmtadTN3VDI3Tm5ZZ3JmSCt4Sno0SEphTkpvb1dDa1hqQmNNUXN3Q1FZRFZRUUdFd0pWVXpFWgpNQmNHQTFVRUNCTVFWMkZ6YUdsdVozUnZiaUJUZEdG"
         "MFpURVFNQTRHQTFVRUJ4TUhVMlZoZEhSc1pURWdNQjRHCkExVUVDaE1YUVcxaGVtOXVJRmRsWWlCVFpYSjJhV05sY3lCTVRFT0NDUUMwampHenFGTnJMekFTQmdOVkhST"
         "UIKQWY4RUNEQUdBUUgvQWdFQU1BMEdDU3FHU0liM0RRRUJDd1VBQTRHQkFFQ2ppNDNwK29Qa1lxbXpsbDdlOEhnYgpvQURTMHBoK1lVejVQL2JVQ202MXdGamx4YVRmd0"
         "tjdVRSM3l0ajdiRkxvVzVCbTdTYStUQ2wzbE9HYjJ0YW9uCjJoKzlOaXJSSzZKWWs4N0xNTnZiUzQwSEdQRnVtSkwyTnpFc0dVZUsrTVJpV3UrT2g1L2xKR2lpM3F3NFl"
         "CeXgKU1VEbFJ5TnkxakpGc3RFWmpPaHMKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo="},
        {"cn-northwest-1",
         "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURDekNDQW5TZ0F3SUJBZ0lKQUxTT01iT29VMnN2TUEwR0NTcUdTSWIzRFFFQkN3VUFNRnd4Q3pBSkJnTlYKQkFZV"
         "EFsVlRNUmt3RndZRFZRUUlFeEJYWVhOb2FXNW5kRzl1SUZOMFlYUmxNUkF3RGdZRFZRUUhFd2RUWldGMApkR3hsTVNBd0hnWURWUVFLRXhkQmJXRjZiMjRnVjJWaUlGTm"
         "xjblpwWTJWeklFeE1RekFlRncweU16QTNNRFF3Ck9ETTFNemxhRncweU9EQTNNREl3T0RNMU16bGFNRnd4Q3pBSkJnTlZCQVlUQWxWVE1Sa3dGd1lEVlFRSUV4QlgKWVh"
         "Ob2FXNW5kRzl1SUZOMFlYUmxNUkF3RGdZRFZRUUhFd2RUWldGMGRHeGxNU0F3SGdZRFZRUUtFeGRCYldGNgpiMjRnVjJWaUlGTmxjblpwWTJWeklFeE1RekNCbnpBTkJn"
         "a3Foa2lHOXcwQkFRRUZBQU9CalFBd2dZa0NnWUVBCnVoaFVObHFBWmRjV1dCL09TRFZER2szT0E5OUVGek9uL21KbG1jaVEvWHd1MmRGSldtU0NxRUFFNmdqdWZDalEKc"
         "TN2b3hBaEMyQ0YrZWxLdEpXL0MwU3ovTFlvNjBQVXFkNmlYRjRoK3VwQjlIa09PR3VXSFhzSEJUc3Zna2dHQQoxQ0dnZWw0VTBDZHErMjNlQU5yOE44bTI4VXpsampTbl"
         "RscllDSHR6TjRzQ0F3RUFBYU9CMURDQjBUQUxCZ05WCkhROEVCQU1DQjRBd0hRWURWUjBPQkJZRUZCa1p1M3dUMjdObllncmZIK3hKejRISmFOSm9NSUdPQmdOVkhTTUU"
         "KZ1lZd2dZT0FGQmtadTN3VDI3Tm5ZZ3JmSCt4Sno0SEphTkpvb1dDa1hqQmNNUXN3Q1FZRFZRUUdFd0pWVXpFWgpNQmNHQTFVRUNCTVFWMkZ6YUdsdVozUnZiaUJUZEdG"
         "MFpURVFNQTRHQTFVRUJ4TUhVMlZoZEhSc1pURWdNQjRHCkExVUVDaE1YUVcxaGVtOXVJRmRsWWlCVFpYSjJhV05sY3lCTVRFT0NDUUMwampHenFGTnJMekFTQmdOVkhST"
         "UIKQWY4RUNEQUdBUUgvQWdFQU1BMEdDU3FHU0liM0RRRUJDd1VBQTRHQkFFQ2ppNDNwK29Qa1lxbXpsbDdlOEhnYgpvQURTMHBoK1lVejVQL2JVQ202MXdGamx4YVRmd0"
         "tjdVRSM3l0ajdiRkxvVzVCbTdTYStUQ2wzbE9HYjJ0YW9uCjJoKzlOaXJSSzZKWWs4N0xNTnZiUzQwSEdQRnVtSkwyTnpFc0dVZUsrTVJpV3UrT2g1L2xKR2lpM3F3NFl"
         "CeXgKU1VEbFJ5TnkxakpGc3RFWmpPaHMKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo="}};

    auto dynamic_document_json = convertStringToJson(dynamic_document);
    String region = dynamic_document_json->optValue("region", String(""));
    if (region.empty() || AMAZON_REGION_CERTIFICATE_BASE64.find(region) == AMAZON_REGION_CERTIFICATE_BASE64.end())
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, does not support the region {}.", region);

    String decoded_public_key = base64Decode(AMAZON_REGION_CERTIFICATE_BASE64.find(region)->second);
    String decoded_signature = base64Decode(signature);
    BIO * cert_bio = nullptr;
    X509 * cert = nullptr;
    EVP_PKEY * pubkey = nullptr;
    EVP_MD_CTX * mdctx = nullptr;

    SCOPE_EXIT({
        EVP_MD_CTX_free(mdctx);
        EVP_PKEY_free(pubkey);
        X509_free(cert);
        BIO_free(cert_bio);
    });

    cert_bio = BIO_new_mem_buf(decoded_public_key.c_str(), -1);
    if (!cert_bio)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, create bio failed.");

    PEM_read_bio_X509(cert_bio, &cert, nullptr, nullptr);
    if (!cert)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, get certificate failed.");
    pubkey = X509_get_pubkey(cert);
    if (!pubkey)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, get public key failed.");

    mdctx = EVP_MD_CTX_new();
    if (!mdctx)
    {
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, init mdctx failed.");
    }

    if (EVP_DigestVerifyInit(mdctx, nullptr, EVP_sha256(), nullptr, pubkey) != 1)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, init verify failed.");

    if (EVP_DigestVerifyUpdate(mdctx, dynamic_document.data(), dynamic_document.size()) != 1)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, update verify failed.");

    if (EVP_DigestVerifyFinal(mdctx, reinterpret_cast<const unsigned char *>(decoded_signature.data()), decoded_signature.size()) != 1)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, verify failed.");

#else
    LOG_ERROR(getLogger("AmazonInstanceMetadata"), "Check Amazon document signature failed, OpenSSL is not enabled.");
    throw Exception(ErrorCodes::LICENSE_ERROR, "Check Amazon document signature failed, OpenSSL is not enabled.");
#endif
}


void AMILicenseChecker::checkAmazonDocumentSignature(const AmazonInstanceMetadata & metadata)
{
    metadata.checkAmazonDocumentSignature();
}

void AMILicenseChecker::checkInstanceID(const AmazonInstanceMetadata & metadata)
{
    auto amazon_dynamic_json_data = convertStringToJson(metadata.dynamic_document);
    auto instance_id = amazon_dynamic_json_data->optValue("instanceId", String(""));
    if (instance_id != metadata.instance_id)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, instance is not matched.");
}

void AMILicenseChecker::checkMarketplaceProductCode(const AmazonInstanceMetadata & metadata)
{
    auto amazon_dynamic_json_data = convertStringToJson(metadata.dynamic_document);
    Poco::JSON::Array::Ptr marketplace_product_code_list = amazon_dynamic_json_data->getArray("marketplaceProductCodes");
    if (!marketplace_product_code_list || marketplace_product_code_list->size() == 0)
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, marketplace product code is empty.");
    for (UInt32 i = 0; i < marketplace_product_code_list->size(); ++i) {
        LOG_INFO(log, "Marketplace product code: {}", marketplace_product_code_list->get(i).convert<String>());
        String marketplace_product_code = marketplace_product_code_list->get(i).convert<String>();
        trim(marketplace_product_code);
        std::transform(marketplace_product_code.begin(), marketplace_product_code.end(), marketplace_product_code.begin(),
            [](unsigned char c) { return std::tolower(c); });
        if (marketplace_product_code == AMAZON_MYSCALE_MARKETPLACE_PRODUCT_CODE)
            return;
        else
            LOG_ERROR(log, "Check license failed, marketplace product code: {} is not matched {}.", marketplace_product_code, AMAZON_MYSCALE_MARKETPLACE_PRODUCT_CODE);
    }
    throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, marketplace product code is not found.");
}


}
