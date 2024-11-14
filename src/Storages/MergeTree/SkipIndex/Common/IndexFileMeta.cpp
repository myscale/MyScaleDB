
#include <Storages/MergeTree/SkipIndex/Common/IndexFileMeta.h>

namespace DB
{

IndexFileMeta::IndexFileMeta() : offset_begin(0), offset_end(0)
{
    file_name[0] = '\0';
}

IndexFileMeta::IndexFileMeta(const String & file_name_, UInt64 offset_begin_, UInt64 offset_end_)
{
    strcpy(file_name, file_name_.c_str());
    offset_begin = offset_begin_;
    offset_end = offset_end_;
}

bool IndexFileMeta::operator==(const IndexFileMeta & other) const
{
    return strcmp(file_name, other.file_name) == 0 && offset_begin == other.offset_begin && offset_end == other.offset_end;
}

}
