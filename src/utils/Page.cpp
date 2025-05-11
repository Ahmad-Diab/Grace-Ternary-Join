#include "utils/Page.h"
#include "Utils.h"

namespace external_join {

    Page::Page(size_t tupleWidth, size_t pageSize) : tupleWidth(tupleWidth) {
        size_t numElements = (pageSize / sizeof (uint64_t));
        elements.reserve(numElements);
        assert(elements.capacity() % tupleWidth == 0);
    }

    void Page::addTuple(std::span<const uint64_t >tuple) {
        assert(elements.capacity());
        assert(tuple.size() == tupleWidth);
        elements.insert(elements.end(), tuple.begin(), tuple.end());
    }

    void Page::serializePage(size_t pageIdx, TemporaryFile* temporaryFile) {
        assert(elements.capacity());
        size_t pageSize = elements.capacity() * sizeof(uint64_t);
        int64_t pageOffset = pageIdx * pageSize;
        Utils::writeBlock(temporaryFile, pageOffset, elements.data(), pageSize);
        elements.clear();
        assert(elements.capacity() == (pageSize / sizeof(uint64_t)));
    }

    void Page::deserializePage(size_t pageIdx, TemporaryFile* tempFile) {
        assert(elements.capacity());
        size_t pageSize = elements.capacity() * sizeof(uint64_t);
        int64_t pageOffset = pageIdx * pageSize;
        elements.resize(elements.capacity());
        Utils::readBlock(tempFile, pageOffset, elements.data(), pageSize);
    }

    void Page::deserializePage(size_t pageIdx, int file_descriptor) {
        assert(elements.capacity());
        size_t pageSize = elements.capacity() * sizeof(uint64_t);
        int64_t pageOffset = pageIdx * pageSize;
        elements.resize(elements.capacity());
        Utils::readBlock(file_descriptor, pageOffset, elements.data(), pageSize);
    }

    std::span<uint64_t> Page::getTuple(size_t index) {
        assert(elements.capacity());
        size_t startIndex = index * tupleWidth;
        assert(startIndex + tupleWidth <= elements.size());
        return {&elements[startIndex], tupleWidth};
    }

    void Page::clear() {
        assert(elements.capacity());
        size_t oldCapacity = elements.capacity();
        elements.resize(0);
        assert(oldCapacity == elements.capacity());
    }

    bool Page::empty() {
        assert(elements.capacity());
        return elements.empty();
    }

    uint64_t* Page::data() {
        assert(elements.capacity());
        return elements.data();
    }

    size_t Page::size() {
        assert(elements.capacity());
        return elements.size();
    }

    void Page::resize(size_t sz) {
        assert(elements.capacity());
        elements.resize(sz);
    }

    size_t Page::capacity() const {
        return elements.capacity();
    }

    bool Page::reachEnd(size_t index) {
        assert(elements.capacity());
        return (index * tupleWidth) >= elements.size();
    }

    size_t Page::getNumTuples() const {
        assert(elements.capacity());
        return elements.size() / tupleWidth;
    }

    void Page::swapTuples(size_t i, size_t j) {
        assert(elements.capacity());
        size_t start_i = i * tupleWidth;
        size_t start_j = j * tupleWidth;
        for (size_t k = 0; k < tupleWidth; ++k) {
            std::swap(elements[start_i + k], elements[start_j + k]);
        }
    }

    bool Page::isFull() {
        assert(elements.capacity());
        return capacity() == size();
    }

    bool Page::isDirty() {
        assert(elements.capacity());
        return this->size() > 0;
    }


}