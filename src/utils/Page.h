#ifndef EXTERNAL_JOIN_PAGE_H
#define EXTERNAL_JOIN_PAGE_H

#include <vector>
#include <cassert>
#include <span>
#include "TemporaryFile.h"

namespace external_join {
    class Page {
        std::vector<uint64_t > elements;
        size_t tupleWidth;
    public:
        explicit Page(){}
        explicit Page(size_t tupleWidth, size_t pageSize);

        void addTuple(std::span<const uint64_t > tuple);
        std::span<uint64_t > getTuple(size_t index);

        void clear();
        bool empty();
        uint64_t* data();
        size_t size();
        void resize(size_t sz);
        size_t capacity() const;

        bool reachEnd(size_t index);

        size_t getNumTuples() const;

        void swapTuples(size_t i, size_t j);

        void serializePage(size_t pageIdx, TemporaryFile* tempFile);
        void deserializePage(size_t pageIdx, TemporaryFile* tempFile);
        void deserializePage(size_t pageIdx, int file_descriptor);

        bool isFull();
        bool isDirty();
    };

}

#endif //EXTERNAL_JOIN_PAGE_H
