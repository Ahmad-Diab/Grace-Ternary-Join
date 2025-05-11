#include "CounterOperator.h"
#include "BinaryJoinOperator.h"
#include "TernaryJoinOperator.h"
#include <numeric>
namespace external_join {

    CounterOperator::CounterOperator(std::unique_ptr<Operator> &&input) : input(std::move(input)), cnt(8) {}

    void CounterOperator::Prepare(external_join::Operator *consumer) {
        assert(consumer == nullptr);
        input->Prepare(this);
        tuple_width = input->getTupleSize();
        cnt = 0;
    }

    void CounterOperator::Produce() {
        input->Produce();
        if (input->getType() == BinaryJoin) {
            auto * inputBinaryOperator = static_cast<BinaryJoinOperator*>(input.get());
            joinBenchmarkCounter = inputBinaryOperator->getBenchmarkCounter();
        } else if (input->getType() == TernaryJoin) {
            auto * inputTernaryOperator = static_cast<TernaryJoinOperator*>(input.get());
            joinBenchmarkCounter = inputTernaryOperator->getBenchmarkCounter();
        }
        joinBenchmarkCounter.result = this->cnt;

    }

    void CounterOperator::Consume(size_t, const external_join::Operator *child, std::span<attribute_t> ) {
        cnt++;
    }

    void CounterOperator::Consume(size_t, const external_join::Operator *child, Page* page) {
        cnt += (page->getNumTuples());
    }

    size_t CounterOperator::getCountTuples() const {
        return cnt;
    }

}