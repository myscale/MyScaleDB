#pragma once

#include <Access/IAccessEntity.h>


namespace DB
{

struct AWSConnection : public IAccessEntity
{
    String provider_name;
    String aws_role_arn;
    String aws_role_external_id;
    UInt32 aws_role_credential_duration;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<AWSConnection>(); }
    static constexpr const AccessEntityType TYPE = AccessEntityType::CONNECTION;
    AccessEntityType getType() const override { return TYPE; }
};

using AWSConnectionPtr = std::shared_ptr<const AWSConnection>;
}
