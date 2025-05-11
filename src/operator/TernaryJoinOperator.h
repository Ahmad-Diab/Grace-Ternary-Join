#ifndef EXTERNAL_JOIN_TERNARYJOINOPERATOR_H
#define EXTERNAL_JOIN_TERNARYJOINOPERATOR_H

#include "Operator.h"
#include "typedefs.h"
#include "joinstrategy/ternary/JoinStrategy.h"
#include <memory>
#include "Page.h"
#include "benchmark/BenchmarkCounter.h"

namespace external_join {
    class TernaryJoinOperator : public Operator {

    private:
        std::unique_ptr<Operator> left, middle, right;
        // consumeBuildLeft -> left_right_join.first_join_index
        // consumeBuildMiddle -> middle_right_join.first_join_index

        const JoinPair left_middle_join, middle_right_join, left_right_join;

        Operator* consumer_;
        ConsumeCallback consumeCallback;

        std::unique_ptr<ternary::JoinStrategy> joinStrategy;
        size_t level = 0;
        JoinBenchmarkCounter benchmarkCounter;
    public:
        struct grace_join_strategy_tag {};

        explicit TernaryJoinOperator(std::unique_ptr<Operator>&& left, std::unique_ptr<Operator>&& middle, std::unique_ptr<Operator>&& right, JoinPair left_middle_join, JoinPair middle_right_join, JoinPair left_right_join, grace_join_strategy_tag, size_t level = 0);

        ~TernaryJoinOperator() noexcept override = default;

        // Prepare the operator
        void Prepare(Operator* consumer) override;
        // Produce all tuples
        void Produce() override;
        // Consume tuple
        void Consume(size_t threadId, const Operator* child, std::span<attribute_t> tuple) override;
        // Consume page
        void Consume(size_t threadId, const Operator* child, Page* page) override;

        OperatorType getType() override {
            return Operator::TernaryJoin;
        }

        JoinBenchmarkCounter getBenchmarkCounter() const {
            return benchmarkCounter;
        }

        enum JoinStrategyType {
            EXTERNAL_GRACE_JOIN,
        } joinStrategyType;

        JoinPair getLeftMiddleJoin() {
            return left_middle_join;
        }

        JoinPair getMiddleRightJoin() {
            return middle_right_join;
        }

        JoinPair getLeftRightJoin() {
            return left_right_join;
        }
        size_t getLevel() {
            return level;
        }
    };
}

#endif //EXTERNAL_JOIN_TERNARYJOINOPERATOR_H
