#ifndef EXTERNAL_JOIN_TYPEDEFS_H
#define EXTERNAL_JOIN_TYPEDEFS_H

#include <string>
#include <functional>

namespace external_join {

    // attribute name
    using attribute_name_t = std::string;
    using attribute_t = std::uint64_t;

    struct JoinPair {
        size_t first_join_index;
        size_t second_join_index;
    };

    using ConsumeCallback = std::function<void(size_t threadId, std::vector<attribute_t> &tuple)>;

    constexpr static char LEFT_RELATION_DIRECTORY_NAME[] = "left-relation";
    constexpr static char MIDDLE_RELATION_DIRECTORY_NAME[] = "middle-relation";
    constexpr static char RIGHT_RELATION_DIRECTORY_NAME[] = "outer-relation";

    constexpr static short TERNARY_LOG_BUCKET_SIZE = 5;
    constexpr static short TERNARY_BUCKET_SHIFT  = 64 - TERNARY_LOG_BUCKET_SIZE;
    constexpr static short TERNARY_BUCKET_SIZE_MASK = (1 << TERNARY_LOG_BUCKET_SIZE) - 1;

    constexpr static short BINARY_LOG_BUCKET_SIZE = 5;
    constexpr static short BINARY_BUCKET_SHIFT  = 64 - BINARY_LOG_BUCKET_SIZE;
    constexpr static short BINARY_BUCKET_SIZE_MASK = (1 << BINARY_LOG_BUCKET_SIZE) - 1;

    constexpr static short LOG_RADIX_SORT_BIT_SIZE = 8;
    constexpr static short RADIX_SORT_MASK = (1 << LOG_RADIX_SORT_BIT_SIZE) - 1;


}
#endif //EXTERNAL_JOIN_TYPEDEFS_H
