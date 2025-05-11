#include "BinaryJoinOperator.h"
#include <cassert>
#include "joinstrategy/binary/GraceHashJoin.h"
#include "typedefs.h"
#include <memory>
#include <utility>

using namespace std;
namespace external_join {

    BinaryJoinOperator::BinaryJoinOperator(unique_ptr <Operator> &&left, unique_ptr <Operator> &&right,
                                           std::vector<size_t>& leftJoinIndices, std::vector<size_t>& rightJoinIndices, grace_join_strategy_tag, size_t level)
            :left(std::move(left)), right(std::move(right)),
             leftJoinIndices(leftJoinIndices), rightJoinIndices(rightJoinIndices), consumer_(nullptr),
             joinStrategyType(GRACE), level(level){}

    void BinaryJoinOperator::Prepare(Operator *consumer) {
        left->Prepare(this);
        right->Prepare(this);
        this->consumer_ = consumer;
        tuple_width = left->getTupleSize() + right->getTupleSize();
        consumeCallback = [this](size_t threadId, std::span<attribute_t> tuple)
        {
            consumer_->Consume(threadId, this, tuple);
        };
        if (joinStrategyType == GRACE) {
            joinStrategy = make_unique<binary::GraceHashJoin>(left->getTupleSize(), right->getTupleSize());
        }
        joinStrategy->setLinkedOperator(this);
        joinStrategy->setConsumerOperator(consumer_);

    }

    void BinaryJoinOperator::Produce() {
        { // Build Phase
            benchmarkCounter.startBuild();

            left->Produce();
            joinStrategy->finishBuild(leftJoinIndices);
            right->Produce();

            benchmarkCounter.endBuild();
        }

        { // Probe Phase
            benchmarkCounter.startProbe();
            joinStrategy->finishProbe(leftJoinIndices, rightJoinIndices, consumeCallback);
            benchmarkCounter.endProbe();
        }
    }

    void BinaryJoinOperator::Consume(size_t threadId, const Operator *child, std::span <attribute_t> tuple) {
        if (child == left.get()) {
            joinStrategy->consumeBuild(threadId, tuple, leftJoinIndices);
        } else if (child == right.get()) {
            joinStrategy->consumeProbe(threadId, tuple,leftJoinIndices,  rightJoinIndices, consumeCallback);
        } else {
            assert(false);
        }
    }

    void BinaryJoinOperator::Consume(size_t threadId, const external_join::Operator *child, external_join::Page *page) {
        if (child == left.get()) {
            joinStrategy->consumeBuild(threadId, page, leftJoinIndices);
        } else if (child == right.get()) {
            joinStrategy->consumeProbe(threadId, page,leftJoinIndices,  rightJoinIndices, consumeCallback);
        } else {
            assert(false);
        }
    }

}
