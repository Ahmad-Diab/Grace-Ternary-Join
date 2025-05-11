#ifndef EXTERNAL_JOIN_BINARYJOINOPERATOR_H
#define EXTERNAL_JOIN_BINARYJOINOPERATOR_H

#include "Operator.h"
#include "typedefs.h"

#include <memory>
#include <unordered_map>
#include "joinstrategy/binary/JoinStrategy.h"
#include "benchmark/BenchmarkCounter.h"

namespace external_join {
    //
    class BinaryJoinOperator : public Operator {
    private:

        // left, right are the producers
        std::unique_ptr<Operator> left, right;
        std::vector<size_t> leftJoinIndices, rightJoinIndices;

        Operator* consumer_;
        ConsumeCallback consumeCallback;

        std::unique_ptr<binary::JoinStrategy> joinStrategy;
        enum JoinStrategyType {
            BASIC,
            HYBRID,
            GRACE
        } joinStrategyType;
        size_t level = 0;

        JoinBenchmarkCounter benchmarkCounter;

    public:
        struct grace_join_strategy_tag {};

        explicit BinaryJoinOperator(std::unique_ptr <Operator> &&left, std::unique_ptr <Operator> &&right,
                                    std::vector<size_t>& leftJoinIndices, std::vector<size_t>& rightJoinIndices, grace_join_strategy_tag, size_t level);

        ~BinaryJoinOperator() noexcept override = default;

        // Prepare the operator
        void Prepare(Operator* consumer) override;
        // Produce all tuples
        void Produce() override;
        // Consume tuple
        void Consume(size_t threadId, const Operator* child, std::span<attribute_t> tuple) override;
        void Consume(size_t threadId, const Operator* child, Page* page) override;

        OperatorType getType() override {
            return Operator::BinaryJoin;
        }

        std::vector<size_t>& getLeftJoinIndices() {
            return leftJoinIndices;
        }
        std::vector<size_t>& getRightJoinIndices() {
            return rightJoinIndices;
        }
        size_t getCurrentLevel() const {
            return level;
        }
        JoinBenchmarkCounter getBenchmarkCounter() const {
            return benchmarkCounter;
        }
    };
}

#endif //EXTERNAL_JOIN_BINARYJOINOPERATOR_H
