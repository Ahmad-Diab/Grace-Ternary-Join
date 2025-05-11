#ifndef EXTERNAL_JOIN_TERNARY_JOINSTRATEGY_H
#define EXTERNAL_JOIN_TERNARY_JOINSTRATEGY_H

#include<vector>
#include <functional>
#include "typedefs.h"
#include "Operator.h"

namespace external_join::ternary {
    class JoinStrategy {
    protected:
        Operator* consumerOperator;
        Operator* linkedOperator;
    public:
        virtual ~JoinStrategy() noexcept = default;

        virtual void consumeBuildLeft(size_t thread_id, std::span<attribute_t>  build_tuple, size_t left_join_idx) = 0;
        virtual void consumeBuildMiddle(size_t thread_id, std::span<attribute_t> build_tuple, size_t middle_join_idx) = 0;
        virtual void consumeProbe(size_t thread_id,
                                  std::span<attribute_t> probe_tuple,
                                  JoinPair left_middle_join,
                                  JoinPair middle_right_join,
                                  JoinPair left_right_join,
                                  ConsumeCallback& consumeCallback) = 0;


        virtual void consumeBuildLeft(size_t thread_id, Page* page, size_t left_join_idx) = 0;
        virtual void consumeBuildMiddle(size_t thread_id, Page* page, size_t middle_join_idx) = 0;
        virtual void consumeProbe(size_t thread_id,
                                  Page* page,
                                  JoinPair left_middle_join,
                                  JoinPair middle_right_join,
                                  JoinPair left_right_join,
                                  ConsumeCallback& consumeCallback) = 0;

        virtual void finishBuildLeft(size_t left_join_idx) = 0;
        virtual void finishBuildMiddle(size_t middle_join_idx) = 0;
        virtual void finishProbe(JoinPair left_middle_join,
                                 JoinPair middle_right_join,
                                 JoinPair left_right_join,
                                 ConsumeCallback& consumeCallback) = 0;

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

#endif //EXTERNAL_JOIN_TERNARY_JOINSTRATEGY_H
