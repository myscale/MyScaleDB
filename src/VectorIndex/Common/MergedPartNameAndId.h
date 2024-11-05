#pragma once

#include <base/types.h>

namespace DB
{
struct MergedPartNameAndId
{
    String name;
    int id;
    bool with_index{true};

    MergedPartNameAndId(const String & name_, const int & id_, bool with_index_ = true) : name(name_), id(id_), with_index(with_index_) { }
};
}
