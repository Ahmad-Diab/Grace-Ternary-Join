#include "benchmark/Benchmark.h"
#include "Database.h"
#include "Table.h"
#include "Operator.h"
#include "ScanOperator.h"
#include "TernaryJoinOperator.h"
#include "CounterOperator.h"

using namespace external_join;

external_join::Database db;

std::unique_ptr<external_join::CounterOperator> constructJoinOperator(external_join::TernaryJoinOperator::JoinStrategyType strategyType) {

    external_join::Table &r = db.GetTable("twitter_R");
    external_join::Table &s = db.GetTable("twitter_R");
    external_join::Table &t = db.GetTable("twitter_R");

    std::unique_ptr<external_join::Operator> scanR = std::make_unique<external_join::ScanOperator>(r);
    std::unique_ptr<external_join::Operator> scanS = std::make_unique<external_join::ScanOperator>(s);
    std::unique_ptr<external_join::Operator> scanT = std::make_unique<external_join::ScanOperator>(t);

    std::unique_ptr<external_join::Operator> join;
    // l(a,b) = l(0,1) -> m(b,c) = m(0, 1) -> r(c, a) -> r(0,1)
    // left_middle_join -> b (l.b = l.tuple[first] & m.b = m.tuple[second])
    // middle_right_join -> c (m.c = m.tuple[first] & r.c = r.tuple[second])
    // left_right_join -> a (l.a = l.tuple[first] & r.a = r.tuple[second])

    if (strategyType == external_join::TernaryJoinOperator::EXTERNAL_GRACE_JOIN) {
        join = std::make_unique<external_join::TernaryJoinOperator>(std::move(scanR), std::move(scanS),
                                                                    std::move(scanT),
                                                                    JoinPair{1, 0},
                                                                    JoinPair{1, 1},
                                                                    JoinPair{0, 0},
                                                                    external_join::TernaryJoinOperator::grace_join_strategy_tag{});

    }
    return std::make_unique<CounterOperator>(std::move(join));
}

int main() {
    db.Create(std::string(BENCHMARK_RESOURCES_DIR) +  "twitter/", "dbconfig.csv");
    BenchmarkFramework benchmarkFramework;

    std::unique_ptr<CounterOperator> topOperator = constructJoinOperator(external_join::TernaryJoinOperator::EXTERNAL_GRACE_JOIN);
    assert(topOperator);

    std::function<JoinBenchmarkCounter()> f = [&topOperator]() {
        topOperator->Prepare();
        topOperator->Produce();
        return topOperator->getBenchmarkCounter();
    };
    benchmarkFramework.addTask("twitter-ternary-external-hash-join", &f, 2420766);
    benchmarkFramework.runBenchmarks(1);

    return 0;
}