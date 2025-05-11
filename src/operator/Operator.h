#ifndef EXTERNAL_JOIN_OPERATOR_H
#define EXTERNAL_JOIN_OPERATOR_H

#include <vector>
#include "typedefs.h"
#include "Page.h"

namespace external_join {

    using Tuple = std::span<attribute_t>;

    class Operator {
    protected:
        size_t tuple_width;
        Operator* consumer;
        enum OperatorType {
            TernaryJoin,
            Scan,
            Print,
            COUNTER_OPERATOR,
            BUCKET_OPERATOR,
            BinaryJoin,
            Materialize
        };
    public:
        /// Constructor
        Operator() = default;
        /// Copy constructor
        Operator(const Operator&) = delete;
        /// Destructor
        virtual ~Operator() noexcept = default;

        // Prepare the operator
        virtual void Prepare(Operator* parent) = 0;
        // Produce tuples
        virtual void Produce() = 0;
        // Consume tuple
        virtual void Consume(size_t threadId, const Operator* child, Tuple tuple) = 0;
        // Consume page
        virtual void Consume(size_t threadId, const Operator* child, Page* page) = 0;

        virtual OperatorType getType() = 0;

        std::size_t getTupleSize() const {
            return tuple_width;
        }
    };
}


#endif //EXTERNAL_JOIN_OPERATOR_H
