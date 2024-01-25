#include <VectorIndex/PartReader.h>

namespace VectorIndex
{
    template class PartReader<DB::VectorSearchType::Float32Vector>;
    template class PartReader<DB::VectorSearchType::BinaryVector>;
}
