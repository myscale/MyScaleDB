
#include "config.h"

#if USE_AWS_S3

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeObjectToFetch.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnObjectToFetch.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Access/AccessControl.h>
#include <Access/AWSConnection.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/S3/AWSLogger.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/STSEndpointProvider.h>
#include <aws/sts/STSEndpointRules.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <aws/sts/model/AssumeRoleResult.h>
#include <aws/sts/model/Credentials.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <Core/Field.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_FILE_NAME;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int FILE_DOESNT_EXIST;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

enum FlagType : int8_t
{
    AWSS3 = 1,
    TencentCOS = 2
};

static std::vector<std::pair<String, Int8>> getProvidersEnumsAndValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"AwsS3FetchObject", static_cast<Int8>(FlagType::AWSS3)},
        {"TencentCOSFetchObject", static_cast<Int8>(FlagType::TencentCOS)},
    };
}

/// Get temporary credentals from AWS IAM using the given connection info for objectURL.
/// The sts credential is assumed to be get from specific directory.
class FunctionGetObject : public IFunction, WithContext
{
public:
    static constexpr auto name = "getObject";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetObject>(context_);
    }

    explicit FunctionGetObject(ContextPtr context_) : WithContext(context_)
    {
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2", getName(), toString(number_of_arguments));

        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}", arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument of function {}", arguments[1]->getName(), getName());

        DataTypePtr flag_element = std::make_shared<DataTypeEnum8>(getProvidersEnumsAndValues());
 
        DataTypePtr element = std::make_shared<DataTypeString>();
        return std::make_shared<DataTypeObjectToFetch>(DataTypes{flag_element, element, element});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr objecturl_column = arguments[0].column;
        const IColumn * conn_column = arguments[1].column.get();

        const ColumnString * conn_string = checkAndGetColumnConstData<ColumnString>(conn_column);
        if (!conn_string)
        {
            if (!(conn_string = checkAndGetColumn<ColumnString>(conn_column)))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} must be constant String", getName());
        }

        /// connection is const
        String conn_name = conn_string->getDataAt(0).toString();

        /// Get provider, role_arn, external_id, duration from system.connections table
        auto & access_control = getContext()->getAccessControl();
        auto conn = access_control.tryRead<AWSConnection>(access_control.getID<AWSConnection>(conn_name));

        /// Get temporary credentials for the role via sts assumeRole api
        String temp_cred;

        if (!saved_access_key_id.empty())
        {
            Aws::Auth::AWSCredentials credentials;
            credentials.SetExpiration(saved_expiration);

            if (!credentials.IsExpired())
            {
                temp_cred = "AccessKeyId=" + saved_access_key_id + "&SecretAccessKey=" + saved_secret_key + "&SessionToken=" + saved_session_token;
            }
        }

        if (temp_cred.empty())
            getRoleTempCredentials(conn, temp_cred);

        /// Get flag value for provider
        String provider = conn->provider_name;
        Int8 flag_type;

        if (provider == "AWS")
            flag_type = FlagType::AWSS3;
        else if (provider == "Tencent")
            flag_type = FlagType::TencentCOS;
        else
            /// Using default provider
            flag_type = FlagType::AWSS3;

        std::shared_ptr<DataTypeEnum8> flag_element = std::make_shared<DataTypeEnum8>(getProvidersEnumsAndValues());
        auto col_flag = flag_element->createColumnConst(
            input_rows_count, static_cast<Field>(flag_type))->convertToFullColumnIfConst();

        DataTypePtr element = std::make_shared<DataTypeString>();
        auto col_cred = element->createColumnConst(
            input_rows_count, static_cast<Field>(temp_cred))->convertToFullColumnIfConst();

        return ColumnObjectToFetch::create(Columns{col_flag, objecturl_column, col_cred});
    }

private:

    /// Get sts credentials from file system
    void GetStsCredentials(String & access_key, String & secret_key) const
    {
        /// Currentlly, we assume the path is '/config/s3/'
        fs::path config_file_absolute_path(config_path);
        fs::path access_file_path = config_file_absolute_path / access_key_file_name;
        fs::path secret_file_path = config_file_absolute_path / secret_key_file_name;

        if (!fs::exists(access_file_path))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist.", access_file_path.string());
         if (!fs::exists(secret_file_path))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist.", secret_file_path.string());

        if (fs::is_directory(access_file_path))
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "File {} can't be a directory.", access_file_path.string());
        if (fs::is_directory(secret_file_path))
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "File {} can't be a directory.", secret_file_path.string());

        ReadBufferFromFile access_in(access_file_path);
        readString(access_key, access_in);

        ReadBufferFromFile security_in(secret_file_path);
        readString(secret_key, security_in);
    }
 
    void getRoleTempCredentials(AWSConnectionPtr & conn, String & temp_cred) const
    {
        /// Get sts credentials from file system
        String access_key;
        String secret_key;
        GetStsCredentials(access_key, secret_key);

        Aws::SDKOptions aws_options;
        Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<S3::AWSLogger>(true));

        Aws::InitAPI(aws_options);
        String roleSessionName = "mvpgetobject";

        Aws::Auth::AWSCredentials credentials;
        AssumeRole(access_key, secret_key, conn->aws_role_arn, roleSessionName, conn->aws_role_external_id, credentials, conn->aws_role_credential_duration);

        /// Return the temporary credentials one string 
        /// Concat the results to a String.
        saved_access_key_id = fromAwsString(credentials.GetAWSAccessKeyId());
        saved_secret_key = fromAwsString(credentials.GetAWSSecretKey());
        saved_session_token = fromAwsString(credentials.GetSessionToken());
        saved_expiration = credentials.GetExpiration();
        temp_cred = "AccessKeyId=" + saved_access_key_id + "&SecretAccessKey=" + saved_secret_key + "&SessionToken=" + saved_session_token;

        Aws::ShutdownAPI(aws_options);
    }
    /**
     * Assume an IAM role defined on an external account.
     */
   Aws::Auth::AWSCredentials * AssumeRole(const String & access_key,
        const String & secret_key,
        const String & roleArn, 
        const String & roleSessionName, 
        const String & externalId,
        Aws::Auth::AWSCredentials & credentials,
        const UInt32 & duration) const
    {
        Aws::STS::Model::AssumeRoleRequest assumeRoleRequest;
        assumeRoleRequest.WithRoleArn(toAwsString(roleArn))
            .WithRoleSessionName(toAwsString(roleSessionName));

        if (!externalId.empty())
            assumeRoleRequest.SetExternalId(toAwsString(externalId));
 
        if (duration > 0)
            assumeRoleRequest.SetDurationSeconds(duration);

        /// Get temporary credentials from assume role
        Aws::Auth::AWSCredentials user_creds(toAwsString(access_key), toAwsString(secret_key));
        Aws::STS::STSClient sts_client(user_creds);

        auto response = sts_client.AssumeRole(assumeRoleRequest);

        if (!response.IsSuccess())
        {
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Error assuming IAM role. {}", response.GetError().GetMessage());
        }

        auto result = response.GetResult();
        auto temp_credentials = result.GetCredentials();

        // Store temporary credentials in return argument
        // Note: The credentials object returned by AssumeRole differs
        // from the AWSCredentials object used in most situations.
        credentials.SetAWSAccessKeyId(temp_credentials.GetAccessKeyId());
        credentials.SetAWSSecretKey(temp_credentials.GetSecretAccessKey());
        credentials.SetSessionToken(temp_credentials.GetSessionToken());
        credentials.SetExpiration(temp_credentials.GetExpiration());
        return &credentials;
    }

    Aws::String toAwsString(const String & s) const
    {
        return Aws::String(s.begin(), s.end());
    }

    String fromAwsString(const Aws::String & s) const
    {
        return String(s.c_str());
    }

    const String config_path = "/config/s3";
    const String access_key_file_name = "AWS_ACCESS_KEY_ID";
    const String secret_key_file_name = "AWS_SECRET_ACCESS_KEY";
    mutable String saved_access_key_id;
    mutable String saved_secret_key;
    mutable String saved_session_token;
    mutable Aws::Utils::DateTime saved_expiration;
};


void registerFunctionGetObject(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetObject>();
}

}

#endif
