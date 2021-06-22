#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `connections` system table, which allows you to get information about connections for external storages.
class StorageSystemConnections final : public IStorageSystemOneBlock<StorageSystemConnections>, boost::noncopyable
{
public:
    std::string getName() const override { return "SystemConnections"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
