#include "benchmark/Benchmark.h"
#include "Database.h"
#include "Table.h"
#include "Operator.h"
#include "ScanOperator.h"
#include "BinaryJoinOperator.h"
#include "CounterOperator.h"

using namespace external_join;

/*
 * SELECT *
 * FROM R, S, T
 *  <a, b>  <b, c> -> (1, 0) [build = 1 , probe = 0]
 * WHERE R.b = S.b -> one join index
 *          <a, b, b, c> <a, c>  -> (0, 0) , (3, 1) [build = (0, 3) , probe = (0, 1)]
 *          and  S.c = T.c and R.a = T.a -> two join indices
 */

std::unique_ptr<external_join::CounterOperator> constructJoinOperator(Database& db) {

    external_join::Table &r = db.GetTable("twitch_R");
    external_join::Table &s = db.GetTable("twitch_R");
    external_join::Table &t = db.GetTable("twitch_R");

    std::unique_ptr<external_join::Operator> scanR = std::make_unique<external_join::ScanOperator>(r);
    std::unique_ptr<external_join::Operator> scanS = std::make_unique<external_join::ScanOperator>(s);
    std::unique_ptr<external_join::Operator> scanT = std::make_unique<external_join::ScanOperator>(t);
    std::vector<size_t> s1 = {1};
    std::vector<size_t> s2 = {0};
    std::unique_ptr<external_join::Operator> leftJoin = std::make_unique<external_join::BinaryJoinOperator>(
            std::move(scanR),
            std::move(scanS),
            s1,
            s2,
            external_join::BinaryJoinOperator::grace_join_strategy_tag{},
            0);
    std::vector<size_t> s3 = {0, 3};
    std::vector<size_t> s4 = {0, 1};
    std::unique_ptr<external_join::Operator> join = std::make_unique<external_join::BinaryJoinOperator>(
            std::move(scanT),
            std::move(leftJoin),
            s4,
            s3,
            external_join::BinaryJoinOperator::grace_join_strategy_tag{},
            0
    );


    return std::make_unique<CounterOperator>(std::move(join));
}

int main() {
    BenchmarkFramework benchmarkFramework;

    external_join::Database db;
    db.Create(std::string(BENCHMARK_RESOURCES_DIR) +  "twitch/", "dbconfig.csv");
    std::unique_ptr<CounterOperator> topOperator = constructJoinOperator(db);
    assert(topOperator);

    std::function<JoinBenchmarkCounter()> f = [&topOperator]() {
        topOperator->Prepare();
        topOperator->Produce();
        return topOperator->getBenchmarkCounter();
    };
    benchmarkFramework.addTask("twitch-binary-external-hash-join", &f, 6797558);
    benchmarkFramework.runBenchmarks(1);

    return 0;
}