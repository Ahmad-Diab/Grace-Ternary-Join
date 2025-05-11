#include "benchmark/Benchmark.h"
#include "Database.h"
#include "Table.h"
#include "Operator.h"
#include "ScanOperator.h"
#include "TernaryJoinOperator.h"
#include "CounterOperator.h"

using namespace external_join;

std::unique_ptr<external_join::CounterOperator> constructJoinOperator(external_join::TernaryJoinOperator::JoinStrategyType strategyType, Database& db) {

    external_join::Table &r = db.GetTable("livejournal_R");
    external_join::Table &s = db.GetTable("livejournal_R");
    external_join::Table &t = db.GetTable("livejournal_R");

    std::unique_ptr<external_join::Operator> scanR = std::make_unique<external_join::ScanOperator>(r);
    std::unique_ptr<external_join::Operator> scanS = std::make_unique<external_join::ScanOperator>(s);
    std::unique_ptr<external_join::Operator> scanT = std::make_unique<external_join::ScanOperator>(t);

    std::unique_ptr<external_join::Operator> join;

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
    BenchmarkFramework benchmarkFramework;

    external_join::Database db;
    db.Create(std::string(BENCHMARK_RESOURCES_DIR) +  "livejournal/", "dbconfig.csv");
    std::unique_ptr<CounterOperator> topOperator = constructJoinOperator(external_join::TernaryJoinOperator::EXTERNAL_GRACE_JOIN, db);
    assert(topOperator);

    std::function<JoinBenchmarkCounter()> f = [&topOperator]() {
        topOperator->Prepare();
        topOperator->Produce();
        return topOperator->getBenchmarkCounter();
    };
    benchmarkFramework.addTask("livejournal-ternary-external-hash-join", &f, 68993773);
    benchmarkFramework.runBenchmarks(1);

    return 0;
}
