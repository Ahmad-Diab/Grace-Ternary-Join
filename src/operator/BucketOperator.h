//
// Created by Ahmed Diab on 09.02.25.
//

#ifndef EXTERNAL_JOIN_BUCKETOPERATOR_H
#define EXTERNAL_JOIN_BUCKETOPERATOR_H

#include "Operator.h"
#include "joinstrategy/Bucket.h"

namespace external_join {
    class BucketOperator: public Operator {
    private:
        Bucket* bucket;
        std::vector<attribute_t> output;
        // Consumer
        Operator *consumer_;
    public:
        explicit BucketOperator(Bucket* bucket);
        ~BucketOperator() noexcept override = default;

        // Prepare the operator
        void Prepare(Operator* consumer) override;
        // Produce all tuples
        void Produce() override;
        // Consume tuple
        void Consume(size_t /*threadId*/, const Operator* /*child*/, std::span<attribute_t> /*tuple*/) override {}
        // Consume page
        void Consume(size_t, const Operator*, Page*) override{}

        OperatorType getType() override {
            return Operator::BUCKET_OPERATOR;
        }

        [[maybe_unused]] Bucket& getBucket() {
            assert(bucket);
            return *bucket;
        }
    };

}

#endif //EXTERNAL_JOIN_BUCKETOPERATOR_H
