#include <VectorIndex/Common/VIPartReader.h>

namespace VectorIndex
{
    template class VIPartReader<Search::DataType::FloatVector>;
    template class VIPartReader<Search::DataType::BinaryVector>;
}
