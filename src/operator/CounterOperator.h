
#ifndef EXTERNAL_JOIN_COUNTEROPERATOR_H
#define EXTERNAL_JOIN_COUNTEROPERATOR_H

#include "Operator.h"
#include "benchmark/BenchmarkCounter.h"
namespace external_join {

    class CounterOperator : public Operator {
    private:
        std::unique_ptr<Operator> input;
        size_t cnt;
        JoinBenchmarkCounter joinBenchmarkCounter;
    public:
        explicit CounterOperator(std::unique_ptr<Operator> &&input);

        ~CounterOperator() noexcept override = default;

        void Prepare(Operator *consumer = nullptr) override;

        void Produce() override;

        void Consume(size_t, const Operator *child, std::span<attribute_t> ) override;
        void Consume(size_t, const Operator *child, Page*) override;

        OperatorType getType() override {
            return Operator::COUNTER_OPERATOR;
        }

        size_t getCountTuples() const;
        JoinBenchmarkCounter getBenchmarkCounter() {
            return joinBenchmarkCounter;
        }
    };

}
#endif //EXTERNAL_JOIN_COUNTEROPERATOR_H
