
#include "Logger.h"
#include "typedefs.h"
#include "ternary/GraceHashJoin.h"

using namespace external_join;

#include "Utils.h"
using namespace external_join::ternary;

struct VectorHash {
    std::size_t operator()(const std::vector<attribute_t >& vec) const {
        std::size_t hash = vec.size();
        for (attribute_t value: vec) {
            hash = Utils::hashCRC32(value, hash);
        }
        // a, b, b, c
        return hash;
    }
};

uint64_t getBucketIndex(const std::span<attribute_t> &tuple, size_t seed) {
    uint64_t hash = seed;
    for (size_t value : tuple) {
        hash = Utils::hashCRC32(value, hash);
    }
    return hash;
}
int main() {
    std::vector<uint64_t> x = {43 , 84};
    std::vector<uint64_t> y = {123 , 434};
    for(int i = 0 ; i < 20 ; i++) {
        std::cout << (getBucketIndex(x, i) >> BINARY_BUCKET_SHIFT) << std::endl;
        std::cout << (getBucketIndex(y, i) >> BINARY_BUCKET_SHIFT) << std::endl;
        std::cout << "\n";
    }
    return 0;
}

