//
// Created by Ahmed Diab on 09.02.25.
//

#include "BucketOperator.h"

namespace external_join {

    BucketOperator::BucketOperator(Bucket *bucket): bucket(bucket), consumer_(nullptr) {
        assert(bucket);
    }

    void BucketOperator::Prepare(Operator *consumer) {
        this->consumer_ = consumer;
        tuple_width = bucket->tuple_width;
    }

    void BucketOperator::Produce() {
        // READ CHUNK at a time
        size_t pageCount = bucket->getPageCount();
        // page
        size_t lastPageTupleCount = bucket->tuple_count % bucket->max_tuples;
        for (size_t pageIdx = 0; pageIdx < pageCount; ++pageIdx) {
            // WARNING -> we assume all those tables are only graph edges with width = 2
            // 2[width] * 8[sizeof (uint64_t)] = 16
            Page p(bucket->tuple_width, bucket->max_tuples * bucket->tuple_width * UINT64_SIZE);

            p.deserializePage(pageIdx, bucket->bucketFile.get());
            size_t n = p.getNumTuples();
            if (pageIdx + 1 == pageCount) {
                assert(lastPageTupleCount <= n);
                n = lastPageTupleCount == 0 ? n : lastPageTupleCount;
            }
            for (size_t i = 0; i < n; i++) {
                auto tupleSpan = p.getTuple(i);
                consumer_->Consume(0, this, tupleSpan);
            }
        }
    }


}