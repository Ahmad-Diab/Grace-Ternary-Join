#ifndef EXTERNAL_JOIN_CONSTANTS_H
#define EXTERNAL_JOIN_CONSTANTS_H

#include <utility>

namespace external_join {
    constexpr static std::size_t UINT64_SIZE = 8;
    constexpr static std::size_t KILOBYTE_SIZE = 1024;
    constexpr static std::size_t MEGABYTE_SIZE = KILOBYTE_SIZE * KILOBYTE_SIZE;
    constexpr static std::size_t GIGABYTE_SIZE = KILOBYTE_SIZE * MEGABYTE_SIZE;
}
#endif //EXTERNAL_JOIN_CONSTANTS_H
