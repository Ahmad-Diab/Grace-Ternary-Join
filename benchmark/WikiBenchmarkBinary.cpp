#include "benchmark/Benchmark.h"
#include "Database.h"
#include "Table.h"
#include "Operator.h"
#include "ScanOperator.h"
#include "BinaryJoinOperator.h"
#include "CounterOperator.h"

using namespace external_join;

external_join::Database db;

/*
 * SELECT *
 * FROM R, S, T
 *  S<b, c>  T<a, c> -> (1, 1) [build = 1 , probe = 1]
 * WHERE R.b = S.b -> one join index
 *          <a, b> <b,c, a, c>  -> (0, 2) , (1, 0) [build = (0, 1) , probe = (2, 0)]
 *          and  S.c = T.c and R.a = T.a -> two join indices
 */

std::unique_ptr<external_join::CounterOperator> constructJoinOperator(Database& db) {

    external_join::Table &r = db.GetTable("wiki_R");
    external_join::Table &s = db.GetTable("wiki_R");
    external_join::Table &t = db.GetTable("wiki_R");

    std::unique_ptr<external_join::Operator> scanR = std::make_unique<external_join::ScanOperator>(r);
    std::unique_ptr<external_join::Operator> scanS = std::make_unique<external_join::ScanOperator>(s);
    std::unique_ptr<external_join::Operator> scanT = std::make_unique<external_join::ScanOperator>(t);
    // a,b   b,c a,c
    std::vector<size_t> s1 = {1};
    std::vector<size_t> s2 = {1};
    std::unique_ptr<external_join::Operator> leftJoin = std::make_unique<external_join::BinaryJoinOperator>(
            std::move(scanS),
            std::move(scanT),
            s1,
            s2,
            external_join::BinaryJoinOperator::grace_join_strategy_tag{},
            0);
    std::vector<size_t> s3 = {0, 1};
    std::vector<size_t> s4 = {2, 0};
    std::unique_ptr<external_join::Operator> join = std::make_unique<external_join::BinaryJoinOperator>(
            std::move(scanR),
            std::move(leftJoin),
            s3,
            s4,
            external_join::BinaryJoinOperator::grace_join_strategy_tag{},
            0
    );


    return std::make_unique<CounterOperator>(std::move(join));
}

int main() {
    db.Create(std::string(BENCHMARK_RESOURCES_DIR) +  "wiki/", "dbconfig.csv");
    BenchmarkFramework benchmarkFramework;

    std::unique_ptr<CounterOperator> topOperator = constructJoinOperator(db);
    assert(topOperator);

    std::function<JoinBenchmarkCounter()> f = [&topOperator]() {
        topOperator->Prepare();
        topOperator->Produce();
        return topOperator->getBenchmarkCounter();
    };
    benchmarkFramework.addTask("wiki-binary-external-hash-join", &f, 5021410);
    benchmarkFramework.runBenchmarks(1);

    return 0;
}