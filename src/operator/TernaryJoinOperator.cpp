#include "TernaryJoinOperator.h"
#include "joinstrategy/ternary/GraceHashJoin.h"
#include <cassert>
#include "Logger.h"
using namespace std;

namespace external_join {


    TernaryJoinOperator::TernaryJoinOperator(unique_ptr <Operator> &&left, unique_ptr <Operator> &&middle,
                                             unique_ptr <Operator> &&right, JoinPair left_middle_join,
                                             JoinPair middle_right_join, JoinPair left_right_join,
                                             TernaryJoinOperator::grace_join_strategy_tag, size_t level)
            : left(std::move(left)), middle(std::move(middle)), right(std::move(right)),
              left_middle_join(left_middle_join), middle_right_join(middle_right_join),
              left_right_join(left_right_join), consumer_(nullptr), level(level), joinStrategyType(EXTERNAL_GRACE_JOIN) {}

    void TernaryJoinOperator::Prepare(Operator *consumer) {
        left->Prepare(this);
        middle->Prepare(this);
        right->Prepare(this);

        this->consumer_ = consumer;
        tuple_width = left->getTupleSize() + middle->getTupleSize() + right->getTupleSize();
        consumeCallback = [this](size_t threadId, std::span<attribute_t> tuple)
        {
            consumer_->Consume(threadId, this, tuple);
        };
        if (joinStrategyType == EXTERNAL_GRACE_JOIN) {
            joinStrategy = make_unique<ternary::GraceHashJoin>(left->getTupleSize(), middle->getTupleSize(), right->getTupleSize());
        } else {
            assert(false);
        }
        joinStrategy->setLinkedOperator(this);
        joinStrategy->setConsumerOperator(consumer);
    }

    void TernaryJoinOperator::Produce() {
        // buildLeft
        Logger::info("Start Produce Operator");
        {
            benchmarkCounter.startBuild();

            Logger::info("Left Operator is now started");
            left->Produce();
            // l.a
            joinStrategy->finishBuildLeft(left_right_join.first_join_index);
            Logger::info("Left Operator is now finished");

            Logger::info("Middle Operator is now started");
            middle->Produce();
            // m.c
            joinStrategy->finishBuildMiddle(middle_right_join.first_join_index);
            Logger::info("Middle Operator is now ended");

            Logger::info("Right Operator is now started in building process");
            right->Produce();
            Logger::info("Right Operator is now ended in building process");

            benchmarkCounter.endBuild();
        }
        {
            benchmarkCounter.startProbe();
            joinStrategy->finishProbe(left_middle_join, middle_right_join, left_right_join, consumeCallback);
            benchmarkCounter.endProbe();
        }
        Logger::info("End Produce Operator");

    }

    // left_middle_join -> b (l.b = l.tuple[first] & m.b = m.tuple[second])
    // middle_right_join -> c (m.c = m.tuple[first] & r.c = r.tuple[second])
    // left_right_join -> a (l.a = l.tuple[first] & r.a = r.tuple[second])

    void TernaryJoinOperator::Consume(size_t threadId, const Operator *child, span <attribute_t> tuple) {
        if (child == left.get()) {
            joinStrategy->consumeBuildLeft(threadId, tuple, left_right_join.first_join_index);
        } else if (child == middle.get()) {
            joinStrategy->consumeBuildMiddle(threadId, tuple, middle_right_join.first_join_index);
        } else if (child == right.get()) {
            joinStrategy->consumeProbe(threadId, tuple,left_middle_join,  middle_right_join, left_right_join, consumeCallback);
        } else {
            assert(false);
        }
    }

    void TernaryJoinOperator::Consume(size_t threadId, const external_join::Operator *child,
                                      external_join::Page *page) {

        if (child == left.get()) {
            joinStrategy->consumeBuildLeft(threadId, page, left_right_join.first_join_index);
        } else if (child == middle.get()) {
            joinStrategy->consumeBuildMiddle(threadId, page, middle_right_join.first_join_index);
        } else if (child == right.get()) {
            joinStrategy->consumeProbe(threadId, page,left_middle_join,  middle_right_join, left_right_join, consumeCallback);
        } else {
            assert(false);
        }
    }

}