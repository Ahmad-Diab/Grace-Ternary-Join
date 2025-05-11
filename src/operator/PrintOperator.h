#ifndef EXTERNAL_JOIN_PRINTOPERATOR_H
#define EXTERNAL_JOIN_PRINTOPERATOR_H

#include "Operator.h"
#include <memory>
#include <mutex>
#include <atomic>

namespace external_join {
    class Table;

    class PrintOperator : public Operator {
    private:
        // input is the producer
        std::unique_ptr<Operator> input;
        std::ostream& out;
        // Doesn't have consumer
    public:
        PrintOperator(std::unique_ptr<Operator>&& input, std::ostream& out);
        ~PrintOperator() noexcept override = default;

        // Prepare the operator
        void Prepare(Operator* consumer) override;
        // Produce all tuples
        void Produce() override;
        // Consume tuple
        void Consume(size_t threadId, const Operator* child, std::span<attribute_t> tuple) override;
        void Consume(size_t threadId, const Operator* child, Page* page) override;

        OperatorType getType() override {
            return Operator::Print;
        }

    };
}

#endif //EXTERNAL_JOIN_PRINTOPERATOR_H
