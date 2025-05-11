#include "PrintOperator.h"
#include <algorithm>
#include <iostream>
#include <cassert>

using namespace std;

namespace {
    std::ostream& operator<<(std::ostream& os, std::span<external_join::attribute_t> vec) {
        os << "(";
        for (size_t i = 0; i < vec.size(); ++i) {
            if (i > 0) os << ", ";
            os << vec[i];
        }
        os << ")";
        return os;
    }
}
namespace external_join {

    PrintOperator::PrintOperator(unique_ptr <Operator> &&input, ostream &out)
        :input(std::move(input)), out(out){}


    void PrintOperator::Prepare(Operator *consumer) {
        assert(consumer == nullptr);
        input->Prepare(this);
        tuple_width = input->getTupleSize();
    }

    void PrintOperator::Produce() {
        input->Produce();
    }

    // maybe the thread id will be used to partition output on multiple buffers based on thread id
    void PrintOperator::Consume(size_t /*threadId*/, const Operator *child, span <attribute_t> tuple) {
        assert(child == input.get());
        out << tuple << '\n';
    }
}