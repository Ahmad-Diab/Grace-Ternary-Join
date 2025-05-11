//
// Created by Ahmed Diab on 04.03.25.
//

#ifndef EXTERNAL_JOIN_BINARY_GRACEHASHJOIN_H
#define EXTERNAL_JOIN_BINARY_GRACEHASHJOIN_H

#include "JoinStrategy.h"
#include "TemporaryDirectory.h"
#include "joinstrategy/Bucket.h"
#include "BinaryJoinOperator.h"
#include <tuple>

namespace external_join::binary {
    class GraceHashJoin final : public JoinStrategy {

        struct BucketDirectory {
            std::unique_ptr<TemporaryDirectory> folder;
            std::unique_ptr<Bucket[]> bucketList;
            const size_t bucketListSize;
            explicit BucketDirectory() :
                    bucketListSize(1 << BINARY_LOG_BUCKET_SIZE) {
                bucketList = std::make_unique<Bucket[]>(bucketListSize);
            }

            void initBucket(size_t bucketIdx, size_t max_tuples, size_t tuple_width) {
                assert(bucketIdx < bucketListSize);
                Bucket& bucket = bucketList[bucketIdx];
                bucket.initialize(max_tuples, tuple_width, &folder);
            }
        };

        BucketDirectory leftDirectory;
        BucketDirectory rightDirectory;
    public:
        explicit GraceHashJoin(size_t leftWidth, size_t rightWidth) {
            std::shared_ptr<TemporaryDirectory> external_join_directory = std::make_shared<TemporaryDirectory>();
            leftDirectory.folder = std::make_unique<TemporaryDirectory>(LEFT_RELATION_DIRECTORY_NAME, external_join_directory);
            rightDirectory.folder = std::make_unique<TemporaryDirectory>(RIGHT_RELATION_DIRECTORY_NAME, external_join_directory);
        }
        /*
         * SELECT *
         * FROM R, S, T
         *  <a, b>  <b, c> -> (1, 0) [build = 1 , probe = 0]
         * WHERE R.b = S.b -> one join index
         *          <a, b, b, c> <a, c>  -> (0, 0) , (3, 1) [build = (0, 3) , probe = (0, 1)]
         *          and  S.c = T.c and R.a = T.a -> two join indices
         */
        // vector<pair<size_t, size_t>>
        void consumeBuild(size_t thread_id,  std::span<attribute_t> tuple, std::vector<size_t>& build_join_indices) final;

        void consumeBuild(size_t thread_id, Page* page, std::vector<size_t>& build_join_indices) final;

        void finishBuild(std::vector<size_t>& build_join_indices) final;

        void consumeProbe(size_t thread_id,  std::span<attribute_t> tuple, std::vector<size_t>& build_join_indices, std::vector<size_t>& probe_join_indices, ConsumeCallback& consumeCallback) final;

        void consumeProbe(size_t thread_id, Page* page, std::vector<size_t>& build_join_indices, std::vector<size_t>& probe_join_indices, ConsumeCallback& consumeCallback) final;

        void finishProbe(std::vector<size_t>& build_join_indices, std::vector<size_t>& probe_join_indices, ConsumeCallback& consumeCallback) final;

        void applyRecursiveBinaryOperator(Bucket* leftBucket, Bucket* rightBucket, std::vector<size_t> build_join_indices, std::vector<size_t> probe_join_indices);

        size_t getCurrentLevel() {
            assert(this->getLinkedOperator());
            BinaryJoinOperator* currentOperator = static_cast<BinaryJoinOperator*>(this->getLinkedOperator());
//            assert(currentOperator->joinStrategyType == TernaryJoinOperator::EXTERNAL_HYBRID_JOIN);
            return currentOperator->getCurrentLevel();
        }

    };
}
#endif //EXTERNAL_JOIN_BINARY_GRACEHASHJOIN_H
