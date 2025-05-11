#ifndef EXTERNAL_JOIN_SCANOPERATOR_H
#define EXTERNAL_JOIN_SCANOPERATOR_H

#include "Operator.h"

namespace external_join {
    class Table;

    class ScanOperator: public Operator {
    private:
        Table &table;
        // Consumer
        Operator *consumer_;
    public:
        explicit ScanOperator(Table& table);
        ~ScanOperator() noexcept override = default;

        // Prepare the operator
        void Prepare(Operator* consumer) override;
        // Produce all tuples
        void Produce() override;
        // Consume tuple
        void Consume(size_t /*threadId*/, const Operator* /*child*/, std::span<attribute_t> /*tuple*/) override {}
        void Consume(size_t, const Operator*, Page*) override {}

        OperatorType getType() override {
            return Operator::Scan;
        }

        [[maybe_unused]] Table& getTable() { return table; }
    };
}
#endif //EXTERNAL_JOIN_SCANOPERATOR_H
