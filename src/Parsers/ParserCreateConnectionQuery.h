#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] connection_name ON CLUSTER cluster_name
  *     PROVIDER = 'AWS' 
  *     AWS_ROLE_ARN = role_arn
  *     [AWS_ROLE_EXTERNAL_ID = external_id]
  *     [AWS_ROLE_CREDENTIAL_DURATION = duration]
  *
  * ALTER CONNECTION [IF EXISTS] connection_name ON CLUSTER cluster_name
  *     [RENAME TO new_connection_name]
  *     [AWS_ROLE_ARN = value]
  *     [AWS_ROLE_EXTERNAL_ID = value]
  *     [AWS_ROLE_CREDENTIAL_DURATION = value]
  */
class ParserCreateConnectionQuery : public IParserBase
{
public:
    void useAttachMode(bool attach_mode_ = true) { attach_mode = attach_mode_; }

protected:
    const char * getName() const override { return "CREATE CONNECTION or ALTER CONNECTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
