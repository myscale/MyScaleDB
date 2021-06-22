#include <Access/AWSConnection.h>


namespace DB
{

bool AWSConnection::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_conn = typeid_cast<const AWSConnection &>(other);
    return (provider_name == other_conn.provider_name) && (aws_role_arn == other_conn.aws_role_arn) && 
           (aws_role_external_id == other_conn.aws_role_external_id) && (aws_role_credential_duration == other_conn.aws_role_credential_duration);
}

}
