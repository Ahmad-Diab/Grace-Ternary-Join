#ifndef EXTERNAL_JOIN_TERNARY_BUCKET_H
#define EXTERNAL_JOIN_TERNARY_BUCKET_H

#include <utility>
#include <vector>
#include <cassert>
#include "utils/Page.h"
#include "utils/TemporaryDirectory.h"
#include "utils/TemporaryFile.h"
#include "utils/Utils.h"
#include "constants.h"
#include "mio/mio.hpp"

namespace external_join {

    struct Bucket {

//    private:
        // The total number of buckets
        std::size_t tuple_count;
        // bucket folder always come from operator -> left, middle or right
        // The bucket file which will be used to serialize all tuples inside the file
        std::unique_ptr<TemporaryFile> bucketFile; // 16

        // Cache the newly consumed tuples in a page before writing them into bucketFile
        std::unique_ptr<Page> cachedPage;

        // it's always pre-processed, maybe we don't need to pass it here
        std::size_t max_tuples;
        std::size_t tuple_width;

        void clearInMemoryPage();
        void clearPersistentPages();

        void writeCachedPage();

    public:

        Bucket() {}

        explicit Bucket(size_t max_tuples,
                        size_t tuple_width,
                        std::unique_ptr<TemporaryDirectory>* bucketFolder);

        void addTuple(std::span<const uint64_t > tuple) {
            if(cachedPage->isFull()) {
                writeCachedPage();
            }
            cachedPage->addTuple(tuple);
            tuple_count++;
        }


        void serialize_bucket() const;

        //Iterator -> iterate over all tuples in bucket
        struct Iterator {
        public:
            explicit Iterator(size_t totalTupleIdx):
                    totalTupleIdx(totalTupleIdx),
                    bucket(nullptr) {}

            explicit Iterator(Bucket* bucket, size_t totalTupleIdx):
                    bucket(bucket),
                    totalTupleIdx(totalTupleIdx) {
                assert(bucket);
                tuple.resize(bucket->tuple_width);
                if (bucket->getFileDescriptor() == -1) {
                    this->totalTupleIdx = bucket->tuple_count;
                } else {
                    mmap = {bucket->getFileDescriptor(), 0, mio::map_entire_file};
                    if (!mmap.is_open()) {
                        std::cerr << "Error mapping file.\n";
//                        close(fd);
                        return;
                    }
                }
            }

            explicit Iterator(Bucket* bucket): totalTupleIdx(0), bucket(bucket) {

                tuple.resize(bucket->tuple_width);
                if (bucket->getFileDescriptor() == -1) {
                    this->totalTupleIdx = bucket->tuple_count;
                } else {
                    mmap = {bucket->getFileDescriptor(), 0, mio::map_entire_file};
                    if (!mmap.is_open()) {
                        std::cerr << "Error mapping file.\n";
                        return;
                    }
                }
            }

            Iterator& operator++() {
                assert(bucket);
                ++totalTupleIdx;
                return *this;
            }

            bool operator != (const Iterator& other) const {
                return !(*this == other);
            }
            bool operator ==(const Iterator& other) const {
                return (totalTupleIdx == other.totalTupleIdx);
            }

            std::span<uint64_t > operator*() {
                size_t width = bucket->tuple_width;
                const char * tuple_ptr = mmap.data() + width * sizeof(uint64_t) * totalTupleIdx;
                for (size_t i = 0 ; i < width; ++i) {
                   tuple[i] =  *(reinterpret_cast<const uint64_t*>(tuple_ptr) + i);
                }
                return tuple;
            }

        public:
            size_t totalTupleIdx = 0;
            Bucket* bucket;
            mio::mmap_source mmap;
            std::vector<uint64_t > tuple;
        };

        [[nodiscard]] Iterator begin()  {
            if (this->tuple_count)
                return Iterator(this);
            return Iterator(this->tuple_count);
        }

        [[nodiscard]] Iterator end() const {
            return Iterator(this->tuple_count);
        }

        [[nodiscard]] std::string getFilename() const {
            return bucketFile->name;
        };

        [[nodiscard]] int32_t getFileDescriptor() const {
            return bucketFile->file_descriptor;
        }

        size_t getPageCount() const {
            return (tuple_count + max_tuples - 1) / max_tuples;
        }

        bool isInitialized() {
            return bucketFile != nullptr;
        }
        void initialize(size_t max_tuples,
                       size_t tuple_width,
                       std::unique_ptr<TemporaryDirectory>* bucketFolder);
    };
}

#endif //EXTERNAL_JOIN_TERNARY_BUCKET_H
