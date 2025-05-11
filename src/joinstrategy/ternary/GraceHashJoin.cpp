//
// Created by Ahmed Diab on 08.02.25.
//
#include "GraceHashJoin.h"
#include "Logger.h"
#include "Operator.h"
#include "BucketOperator.h"
#include "TernaryJoinOperator.h"

#include <robin_hood.h>


namespace external_join::ternary {

    void GraceHashJoin::applyRecursiveTernaryOperator(Bucket* leftBucket, Bucket* middleBucket, Bucket* rightBucket ) {
        if (this->getCurrentLevel() >= 3) {
            throw std::runtime_error("We can't create ternary operators more than 3 levels.");
        }
        assert(this->getLinkedOperator());
        auto* currentOperator = dynamic_cast<TernaryJoinOperator*>(this->getLinkedOperator());
        assert(currentOperator->joinStrategyType == TernaryJoinOperator::EXTERNAL_GRACE_JOIN);
        std::unique_ptr<external_join::Operator> bucketR = std::make_unique<external_join::BucketOperator>(leftBucket);
        std::unique_ptr<external_join::Operator> bucketS = std::make_unique<external_join::BucketOperator>(middleBucket);
        std::unique_ptr<external_join::Operator> bucketT = std::make_unique<external_join::BucketOperator>(rightBucket);
        std::unique_ptr<external_join::Operator> join = std::make_unique<external_join::TernaryJoinOperator>(
                std::move(bucketR),
                std::move(bucketS),
                std::move(bucketT),

                currentOperator->getLeftMiddleJoin(),
                currentOperator->getMiddleRightJoin(),
                currentOperator->getLeftRightJoin(),

                external_join::TernaryJoinOperator::grace_join_strategy_tag{},
                this->getCurrentLevel() + 1
                );
        join->Prepare(this->getConsumerOperator());
        join->Produce();
    }


   size_t getSizeInBytes(size_t tupleCount, size_t tupleWidth) {
    //  return tupleCount * tupleWidth * sizeof(attribute_t);
        return tupleCount << 4;
    }
    bool buildBucketsExceedMemorySize(const Bucket& leftBucket, const Bucket& middleBucket) {
        return getSizeInBytes(leftBucket.tuple_count, leftBucket.tuple_width) + getSizeInBytes(middleBucket.tuple_count, middleBucket.tuple_width) > MAX_MEMORY_SIZE;
    }
    void GraceHashJoin::consumeBuildLeft(size_t, std::span<attribute_t> build_tuple,
                                                   size_t left_join_idx) {

        attribute_t left_join_value = build_tuple[left_join_idx];
        size_t bucketIndex = Utils::hashCRC32(left_join_value, this->getCurrentLevel()) >> TERNARY_BUCKET_SHIFT;
        Bucket& bucket = leftDirectory.bucketList[bucketIndex];
        if (!bucket.isInitialized()) {
            size_t tupleWidth = build_tuple.size();
            size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
            leftDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
        }
        bucket.addTuple(build_tuple);
    }

    void GraceHashJoin::consumeBuildLeft(size_t, Page *page,
                                                   size_t left_join_idx) {
        size_t num_tuples = page->getNumTuples();
        size_t currentLevel = this->getCurrentLevel();
        for(size_t i = 0; i < num_tuples; ++i) {
            auto build_tuple = page->getTuple(i);
            attribute_t left_join_value = build_tuple[left_join_idx];
            size_t bucketIndex = Utils::hashCRC32(left_join_value, currentLevel) >> TERNARY_BUCKET_SHIFT;
            Bucket &bucket = leftDirectory.bucketList[bucketIndex];
            if (!bucket.isInitialized()) {
                size_t tupleWidth = build_tuple.size();
                size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
                leftDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
            }
            bucket.addTuple(build_tuple);

        }
    }

    void GraceHashJoin::consumeBuildMiddle(size_t, std::span<attribute_t> build_tuple,
                                                     size_t middle_join_idx) {
        attribute_t middle_join_value = build_tuple[middle_join_idx];
        size_t bucketIndex = Utils::hashCRC32(middle_join_value, this->getCurrentLevel()) >> TERNARY_BUCKET_SHIFT;
        Bucket& bucket = middleDirectory.bucketList[bucketIndex];
        if (!bucket.isInitialized()) {
            size_t tupleWidth = build_tuple.size();
            size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
            middleDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
        }
        bucket.addTuple(build_tuple);
    }

    void GraceHashJoin::consumeBuildMiddle(size_t, Page* page,
                                                     size_t middle_join_idx) {
        size_t num_tuples = page->getNumTuples();
        size_t currentLevel = this->getCurrentLevel();
        for(size_t i = 0; i < num_tuples; ++i) {
            auto build_tuple = page->getTuple(i);
            attribute_t middle_join_value = build_tuple[middle_join_idx];
            size_t bucketIndex = Utils::hashCRC32(middle_join_value, currentLevel) >> TERNARY_BUCKET_SHIFT;
            Bucket &bucket = middleDirectory.bucketList[bucketIndex];
            if (!bucket.isInitialized()) {
                size_t tupleWidth = build_tuple.size();
                size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
                middleDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
            }
            bucket.addTuple(build_tuple);

        }
    }

    void GraceHashJoin::consumeProbe(size_t, std::span<attribute_t> build_tuple,
                                               external_join::JoinPair,
                                               external_join::JoinPair middle_right_join,
                                               external_join::JoinPair left_right_join,
                                               external_join::ConsumeCallback &) {
        size_t currentLevel = this->getCurrentLevel();
        // left_right.second = r.a, middle_right.second = r.c
        size_t leftJoinIndex = left_right_join.second_join_index;
        attribute_t leftJoinValue = build_tuple[leftJoinIndex];
        size_t leftBucketIndex = Utils::hashCRC32(leftJoinValue, currentLevel) >> TERNARY_BUCKET_SHIFT;

        size_t middleJoinIndex = middle_right_join.second_join_index;
        size_t middleJoinValue = build_tuple[middleJoinIndex];
        size_t middleBucketIndex = Utils::hashCRC32(middleJoinValue, currentLevel) >> TERNARY_BUCKET_SHIFT;

        size_t bucketIndex = (leftBucketIndex << TERNARY_LOG_BUCKET_SIZE) | middleBucketIndex;
        Bucket &probeBucket = rightDirectory.bucketList[bucketIndex];
        if (!probeBucket.isInitialized()) {
            size_t tupleWidth = build_tuple.size();
            size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
            rightDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
        }
        probeBucket.addTuple(build_tuple);

    }

    void GraceHashJoin::consumeProbe(size_t, Page* page,
                                               external_join::JoinPair,
                                               external_join::JoinPair middle_right_join,
                                               external_join::JoinPair left_right_join,
                                               external_join::ConsumeCallback &) {
        size_t num_tuples = page->getNumTuples();
        size_t currentLevel = this->getCurrentLevel();
        for(size_t i = 0; i < num_tuples; ++i) {
            auto build_tuple = page->getTuple(i);
            // left_right.second = r.a, middle_right.second = r.c
            size_t leftJoinIndex = left_right_join.second_join_index;
            attribute_t leftJoinValue = build_tuple[leftJoinIndex];
            size_t leftBucketIndex = Utils::hashCRC32(leftJoinValue, currentLevel) >> TERNARY_BUCKET_SHIFT;

            size_t middleJoinIndex = middle_right_join.second_join_index;
            size_t middleJoinValue = build_tuple[middleJoinIndex];
            size_t middleBucketIndex = Utils::hashCRC32(middleJoinValue, currentLevel) >> TERNARY_BUCKET_SHIFT;

            size_t bucketIndex = (leftBucketIndex << TERNARY_LOG_BUCKET_SIZE) | middleBucketIndex;
            Bucket &probeBucket = rightDirectory.bucketList[bucketIndex];
            if (!probeBucket.isInitialized()) {
                size_t tupleWidth = build_tuple.size();
                size_t max_tuples = PAGE_SIZE / (UINT64_SIZE * tupleWidth);
                rightDirectory.initBucket(bucketIndex, max_tuples, tupleWidth);
            }
            probeBucket.addTuple(build_tuple);
        }
    }
    void GraceHashJoin::finishBuildLeft(size_t) {
        Logger::info("Starting `finishBuildLeft() method` level -> {}", getCurrentLevel());
        // MAX_SIZE / 2 -> LEFT AND MIDDLE DIRECTORY
        // remaining memory size
        for(size_t i = 0; i < (1ull << TERNARY_LOG_BUCKET_SIZE); ++i) {
            Bucket &currentBucket = leftDirectory.bucketList[i];
            if (currentBucket.isInitialized() && currentBucket.tuple_count) {
                currentBucket.serialize_bucket();
            }
        }
        Logger::info("Ending `finishBuildMiddle() method` level -> {}", getCurrentLevel());
    }
    void GraceHashJoin::finishBuildMiddle(size_t) {
        Logger::info("Starting `finishBuildMiddle() method`", getCurrentLevel());
        for(size_t i = 0; i < (1ull << TERNARY_LOG_BUCKET_SIZE); ++i) {
            Bucket &currentBucket = middleDirectory.bucketList[i];
            if (currentBucket.isInitialized() && currentBucket.tuple_count) {
                currentBucket.serialize_bucket();
            }
        }
        Logger::info("Ending `finishBuildMiddle() method` level -> {}", getCurrentLevel());
    }

    // left_middle_join -> b (l.b = l.tuple[first] & m.b = m.tuple[second])
    // middle_right_join -> c (m.c = m.tuple[first] & r.c = r.tuple[second])
    // left_right_join -> a (l.a = l.tuple[first] & r.a = r.tuple[second])
    void GraceHashJoin::finishProbe(external_join::JoinPair left_middle_join,
                                              external_join::JoinPair middle_right_join,
                                              external_join::JoinPair left_right_join,
                                              external_join::ConsumeCallback &consumeCallback) {
        Logger::info("Starting `finishProbe()` method level -> {}", getCurrentLevel());
        robin_hood::unordered_flat_map<attribute_t, robin_hood::unordered_flat_map<attribute_t, size_t> > leftBucketTable;
        for (size_t leftBucketIndex = 0; leftBucketIndex < leftDirectory.bucketListSize ; ++leftBucketIndex) {
            Bucket &leftBucket = leftDirectory.bucketList[leftBucketIndex];
            if (!leftBucket.isInitialized() || leftBucket.tuple_count == 0) {
                if (!leftBucketTable.empty())
                    leftBucketTable.clear();
                continue;
            }
            // Construct leftBucketTable
            leftBucketTable.clear();
            for (std::span<attribute_t> leftTuple: leftBucket) {
                // l.a
                uint64_t leftKey = leftTuple[left_right_join.first_join_index];
                // l.b
                uint64_t leftSecondKey = leftTuple[left_middle_join.first_join_index];
                leftBucketTable[leftKey][leftSecondKey]++;
            }

            for (size_t middleBucketIndex = 0; middleBucketIndex < middleDirectory.bucketListSize; ++middleBucketIndex) {
                size_t probeBucketIndex = leftBucketIndex << TERNARY_LOG_BUCKET_SIZE | middleBucketIndex;
                Bucket &probeBucket = rightDirectory.bucketList[probeBucketIndex];
                if (!probeBucket.isInitialized() || probeBucket.tuple_count == 0) {
                    continue;
                }
                probeBucket.serialize_bucket();

                Bucket &middleBucket = middleDirectory.bucketList[middleBucketIndex];
                if (!middleBucket.isInitialized() || middleBucket.tuple_count == 0) {
                    continue;
                }
                if (buildBucketsExceedMemorySize(leftBucket, middleBucket)) {
                    // Bucket Operator leftOperator, middleOperator, rightOperator
                    applyRecursiveTernaryOperator(&leftBucket, &middleBucket, &probeBucket);
                }
                else
                {
                    // c -> {b: cnt(b)}
                    // Construct middleBucketTable
                    robin_hood::unordered_flat_map<attribute_t, robin_hood::unordered_flat_map<attribute_t, size_t> > middleBucketTable;
                    for (std::span<attribute_t> middleTuple: middleBucket) {
                        // m.c
                        uint64_t middleKey = middleTuple[middle_right_join.first_join_index];
                        // m.b
                        uint64_t middleSecondKey = middleTuple[left_middle_join.second_join_index];
                        middleBucketTable[middleKey][middleSecondKey]++;
                    }

                    for (std::span<attribute_t> probeTuple: probeBucket) {
                        // r.a
                        size_t probeLeftKey = probeTuple[left_right_join.second_join_index];
                        // r.c
                        size_t probeMiddleKey = probeTuple[middle_right_join.second_join_index];
                        robin_hood::unordered_flat_map<attribute_t, size_t> &lb = leftBucketTable[probeLeftKey];
                        robin_hood::unordered_flat_map<attribute_t, size_t> &mb = middleBucketTable[probeMiddleKey];
                        // a, b , b , c , a , c
                        std::vector<uint64_t> result = {probeLeftKey, 0, 0, probeMiddleKey, probeLeftKey, probeMiddleKey};
                        if (lb.size() < mb.size()) {
                            for (auto &[intersectionLeftMiddleValue, cntL]: lb) {
                                size_t totalIntersectionValue = cntL * mb[intersectionLeftMiddleValue];
                                result[1] = result[2] = intersectionLeftMiddleValue;
                                while (totalIntersectionValue > 0) {
                                    consumeCallback(0, result);
                                    totalIntersectionValue--;
                                }
                            }
                        } else {
                            for (auto &[intersectionLeftMiddleValue, cntM]: mb) {
                                result[1] = result[2] = intersectionLeftMiddleValue;
                                size_t totalIntersectionValues = lb[intersectionLeftMiddleValue] * cntM;
                                while (totalIntersectionValues > 0) {
                                    consumeCallback(0, result);
                                    totalIntersectionValues--;
                                }
                            }
                        }
                    }
                }
                if (middleBucketIndex == TERNARY_BUCKET_SIZE_MASK) {
                    Logger::info("Finished {} of probe buckets", probeBucketIndex + 1);
                }
            }
        }
        Logger::info("Ending `finishProbe()` method level -> {}", getCurrentLevel());
    }}