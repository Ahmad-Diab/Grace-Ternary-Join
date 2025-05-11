//
// Created by Ahmed Diab on 08.02.25.
//

#ifndef EXTERNAL_JOIN_TERNARY_GRACEHASHJOIN_H
#define EXTERNAL_JOIN_TERNARY_GRACEHASHJOIN_H

#include "ternary/JoinStrategy.h"
#include "TemporaryDirectory.h"
#include "Bucket.h"
#include "GraceHashJoin.h"
#include "Logger.h"
#include "Operator.h"
#include "BucketOperator.h"
#include "TernaryJoinOperator.h"


namespace external_join::ternary {

    class GraceHashJoin : public JoinStrategy {
    public:
        template<bool isProbe>
        struct BucketDirectory {
            std::unique_ptr<TemporaryDirectory> folder;
            std::unique_ptr<Bucket[]> bucketList;
            const size_t bucketListSize;
            explicit BucketDirectory() :
                bucketListSize(isProbe ? (1 << (TERNARY_LOG_BUCKET_SIZE << 1)) : (1 << TERNARY_LOG_BUCKET_SIZE)) {
                bucketList = std::make_unique<Bucket[]>(bucketListSize);
            }

            void initBucket(size_t bucketIdx, size_t max_tuples, size_t tuple_width) {
                assert(bucketIdx < bucketListSize);
                Bucket& bucket = bucketList[bucketIdx];
                bucket.initialize(max_tuples, tuple_width, &folder);
            }
        };

        static constexpr bool PROBE_RELATION = true;
        static constexpr bool NOT_PROBE_RELATION = false;

        BucketDirectory<NOT_PROBE_RELATION> leftDirectory;
        BucketDirectory<NOT_PROBE_RELATION> middleDirectory;
        BucketDirectory<PROBE_RELATION> rightDirectory;
    public:

        GraceHashJoin(size_t leftWidth, size_t middleWidth, size_t rightWidth) {
            std::shared_ptr<TemporaryDirectory> external_join_directory = std::make_shared<TemporaryDirectory>();
            leftDirectory.folder = std::make_unique<TemporaryDirectory>(LEFT_RELATION_DIRECTORY_NAME, external_join_directory);
            middleDirectory.folder = std::make_unique<TemporaryDirectory>(MIDDLE_RELATION_DIRECTORY_NAME, external_join_directory);
            rightDirectory.folder = std::make_unique<TemporaryDirectory>(RIGHT_RELATION_DIRECTORY_NAME, external_join_directory);
        }

        void consumeBuildLeft(size_t thread_id,  std::span<attribute_t> build_tuple, size_t left_join_idx) final;
        void consumeBuildLeft(size_t thread_id, Page* page, size_t left_join_idx) final;

        void consumeBuildMiddle(size_t thread_id, std::span<attribute_t> build_tuple, size_t middle_join_idx) final;
        void consumeBuildMiddle(size_t thread_id, Page* page, size_t middle_join_idx) final;

        void consumeProbe(size_t thread_id,  std::span<attribute_t> probe_tuple, JoinPair left_middle_join, JoinPair middle_right_join, JoinPair left_right_join, ConsumeCallback& consumeCallback) final;
        void consumeProbe(size_t thread_id, Page* page, JoinPair left_middle_join, JoinPair middle_right_join, JoinPair left_right_join, ConsumeCallback& consumeCallback) final;

        void finishBuildLeft(size_t left_join_idx) final;
        void finishBuildMiddle(size_t middle_join_idx) final;
        void finishProbe(JoinPair left_middle_join, JoinPair middle_right_join, JoinPair left_right_join, ConsumeCallback& consumeCallback) final;

        void applyRecursiveTernaryOperator(Bucket* leftBucket, Bucket* middleBucket, Bucket* rightBucket);

        size_t getCurrentLevel() {
            assert(this->getLinkedOperator());
            auto* currentOperator = static_cast<TernaryJoinOperator*>(this->getLinkedOperator());
            assert(currentOperator->joinStrategyType == TernaryJoinOperator::EXTERNAL_GRACE_JOIN);
            return currentOperator->getLevel();
        }
    };
}
#endif //EXTERNAL_JOIN_TERNARY_GRACEHASHJOIN_H
