#ifndef EXTERNAL_JOIN_BINARY_JOINSTRATEGY_H
#define EXTERNAL_JOIN_BINARY_JOINSTRATEGY_H

#include<vector>
#include <functional>
#include "typedefs.h"
#include "Operator.h"

namespace external_join::binary {

    class JoinStrategy {
    protected:
        Operator* consumerOperator;
        Operator* linkedOperator;
    public:
        virtual ~JoinStrategy() noexcept = default;
        virtual void consumeBuild(size_t thread_id,  std::span<attribute_t>  tuple, std::vector<size_t>& build_join_indices) = 0;
        virtual void consumeBuild(size_t thread_id, Page* page, std::vector<size_t>& build_join_indices) = 0;
        virtual void finishBuild(std::vector<size_t>& build_join_indices) = 0;

        virtual void consumeProbe(size_t thread_id,  std::span<attribute_t> right_tuple_values, std::vector<size_t>& build_join_indices, std::vector<size_t>& probe_join_indices, ConsumeCallback& consumeCallback) = 0;
        virtual void consumeProbe(size_t thread_id, Page* page, std::vector<size_t>& build_join_indices, std::vector<size_t>& probe_join_indices, ConsumeCallback& consumeCallback) = 0;
        virtual void finishProbe(std::vector<size_t>& build_join_indices, std::vector<size_t>& probe_join_indices, ConsumeCallback& consumeCallback) = 0;

        void setConsumerOperator(Operator* consumerOperator) {
            this->consumerOperator = consumerOperator;
        }

        Operator* getConsumerOperator() {
            return consumerOperator;
        }

        void setLinkedOperator(Operator* linkedOperator) {
            this->linkedOperator = linkedOperator;
        }
        Operator* getLinkedOperator() {
            return linkedOperator;
        }

    };

}

#endif //EXTERNAL_JOIN_BINARY_JOINSTRATEGY_H
