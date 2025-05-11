//
// Created by Ahmed Diab on 07.03.25.
//

#include "joinstrategy/binary/GraceHashJoin.h"
#include "BucketOperator.h"
#include <ranges>
#include "robin_hood.h"

namespace external_join::binary {

    void GraceHashJoin::applyRecursiveBinaryOperator(Bucket* leftBucket, Bucket* rightBucket, std::vector<size_t> build_join_indices, std::vector<size_t> probe_join_indices) {
        if (this->getCurrentLevel() >= 200) {
            throw std::runtime_error("We can't create binary operator more than 200 levels.");
        }
        assert(this->getLinkedOperator());
        BinaryJoinOperator* currentOperator = dynamic_cast<BinaryJoinOperator*>(this->getLinkedOperator());
//        assert(currentOperator-> == TernaryJoinOperator::EXTERNAL_HYBRID_JOIN);
        std::unique_ptr<external_join::Operator> leftBucketOperator = std::make_unique<external_join::BucketOperator>(leftBucket);
        std::unique_ptr<external_join::Operator> rightBucketOperator = std::make_unique<external_join::BucketOperator>(rightBucket);
        std::unique_ptr<external_join::BinaryJoinOperator> join = std::make_unique<external_join::BinaryJoinOperator>(
                std::move(leftBucketOperator),
                std::move(rightBucketOperator),

                build_join_indices,
                probe_join_indices,

                external_join::BinaryJoinOperator::grace_join_strategy_tag{},
                this->getCurrentLevel() + 1
        );
//        std::cerr << "Apply Recursive Operator " << this->getCurrentLevel() + 1 << std::endl;
        join->Prepare(this->getConsumerOperator());
        join->Produce();
    }

    struct VectorHash {
        std::size_t operator()(const std::vector<attribute_t >& vec) const {
            std::size_t hash = vec.size();
            for (attribute_t value: vec) {
                hash = Utils::hashCRC32(value, hash);
            }
            return hash;
        }
    };

    uint64_t getBucketIndex(const std::vector<size_t>& indices, const std::span<attribute_t> &tuple, size_t seed) {
        uint64_t hash = seed;
        for (size_t index : indices) {
            hash = Utils::hashCRC32(tuple[index], hash);
        }
        return hash;
    }

    size_t getSizeInBytes(size_t tupleCount, size_t tupleWidth) {
        return tupleCount * tupleWidth * sizeof(attribute_t);
    }
    bool buildBucketsExceedMemorySize(const Bucket& leftBucket) {
        return getSizeInBytes(leftBucket.tuple_count, leftBucket.tuple_width) > MAX_MEMORY_SIZE;
    }

    void GraceHashJoin::consumeBuild(size_t thread_id, std::span<attribute_t> tuple,
                                     std::vector<size_t> &build_join_indices) {
        size_t bucketIndex = getBucketIndex(build_join_indices, tuple, this->getCurrentLevel()) >> BINARY_BUCKET_SHIFT;
        Bucket& bucket = leftDirectory.bucketList[bucketIndex];
        if (!bucket.isInitialized()) {
            size_t tupleWidth = tuple.size();
            size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
            leftDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
        }
        bucket.addTuple(tuple);
    }

    void GraceHashJoin::consumeBuild(size_t thread_id, Page* page,
                                     std::vector<size_t> &build_join_indices) {
        size_t num_tuples = page->getNumTuples();
        size_t currentLevel = this->getCurrentLevel();
        for (size_t i = 0; i < num_tuples; ++i) {
            std::span<attribute_t> tuple = page->getTuple(i);
            size_t bucketIndex = getBucketIndex(build_join_indices, tuple, currentLevel) >> BINARY_BUCKET_SHIFT;
            Bucket& bucket = leftDirectory.bucketList[bucketIndex];
            if (!bucket.isInitialized()) {
                size_t tupleWidth = tuple.size();
                size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
                leftDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
            }
            bucket.addTuple(tuple);
        }
    }

    void GraceHashJoin::finishBuild(std::vector<size_t> &) {
        for (size_t i = 0 ; i < leftDirectory.bucketListSize; i++) {
            Bucket &currentBucket = leftDirectory.bucketList[i];
            if (currentBucket.isInitialized() && currentBucket.tuple_count) {
                currentBucket.serialize_bucket();
            }
        }
    }

    void GraceHashJoin::consumeProbe(size_t thread_id, std::span<attribute_t> tuple,
                                     std::vector<size_t> &, std::vector<size_t> &probe_join_indices,
                                     ConsumeCallback &) {
        size_t bucketIndex = getBucketIndex(probe_join_indices, tuple, this->getCurrentLevel()) >> BINARY_BUCKET_SHIFT;
        Bucket& bucket = rightDirectory.bucketList[bucketIndex];
        if (!bucket.isInitialized()) {
            size_t tupleWidth = tuple.size();
            size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
            rightDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
        }
        bucket.addTuple(tuple);
    }

    void GraceHashJoin::consumeProbe(size_t thread_id, external_join::Page *page,
                                     std::vector<size_t> &build_join_indices, std::vector<size_t> &probe_join_indices,
                                     external_join::ConsumeCallback &consumeCallback) {
        size_t num_tuples = page->getNumTuples();
        size_t currentLevel = this->getCurrentLevel();
        for (size_t i = 0; i < num_tuples; ++i) {
            std::span<attribute_t> tuple = page->getTuple(i);
            size_t bucketIndex = getBucketIndex(probe_join_indices, tuple, currentLevel) >> BINARY_BUCKET_SHIFT;
            Bucket& bucket = rightDirectory.bucketList[bucketIndex];
            if (!bucket.isInitialized()) {
                size_t tupleWidth = tuple.size();
                size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
                rightDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
            }
            bucket.addTuple(tuple);

        }
    }

    void GraceHashJoin::finishProbe(std::vector<size_t> &build_join_indices, std::vector<size_t> &probe_join_indices,
                                    external_join::ConsumeCallback &consumeCallback) {

        /*
         * SELECT *
         * FROM R, S, T
         *  <a, b>  <b, c> -> (1, 0) [build = 1 , probe = 0]
         * WHERE R.b = S.b -> one join index
         *          <a, b, b, c> <a, c>  -> (0, 0) , (3, 1) [build = (0, 3) , probe = (0, 1)]
         *          and  S.c = T.c and R.a = T.a
         */

        for (size_t bucketIndex = 0; bucketIndex < leftDirectory.bucketListSize; ++bucketIndex) {
            robin_hood::unordered_flat_map<std::vector<attribute_t> ,std::vector<std::vector<attribute_t>>, VectorHash> leftBucketTable;
            Bucket &leftBucket = leftDirectory.bucketList[bucketIndex];
            if (!leftBucket.isInitialized() || leftBucket.tuple_count == 0) {
                continue;
            }

            Bucket &rightBucket = rightDirectory.bucketList[bucketIndex];
            if (!rightBucket.isInitialized() || rightBucket.tuple_count == 0) {
                continue;
            }
            rightBucket.serialize_bucket();

            if (buildBucketsExceedMemorySize(leftBucket)) {
                applyRecursiveBinaryOperator(&leftBucket, &rightBucket, build_join_indices, probe_join_indices);
            } else {
                for (std::span<attribute_t> leftTuple: leftBucket) {
                    std::vector<attribute_t> left_join_values(build_join_indices.size());
                    for (size_t i = 0; i < build_join_indices.size(); ++i) {
                        left_join_values[i] = leftTuple[build_join_indices[i]];
                    }
                    leftBucketTable[left_join_values].emplace_back(leftTuple.begin(), leftTuple.end());
                }

                for(std::span<attribute_t> rightTuple: rightBucket) {
                    std::vector<attribute_t> right_join_values(probe_join_indices.size());
                    for (size_t i = 0; i < probe_join_indices.size(); ++i) {
                        right_join_values[i] = rightTuple[probe_join_indices[i]];
                    }
                    if (leftBucketTable.contains(right_join_values)) {
                        std::vector<std::vector<attribute_t>> probedTuples = leftBucketTable.at(right_join_values);
                        for (const std::vector<attribute_t>& leftTuple: probedTuples) {
                            // For testing:
//                            for (size_t i = 0 ; i < build_join_indices.size(); i++) {
//                                assert(leftTuple[build_join_indices[i]] == rightTuple[probe_join_indices[i]]);
//                            }
                            std::vector<attribute_t> result(leftTuple);
                            result.insert(result.end(), rightTuple.begin(), rightTuple.end());
                            consumeCallback(0, result);
                        }
                    }

                }

            }
        }
    }
}