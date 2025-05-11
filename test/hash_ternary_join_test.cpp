#include <gtest/gtest.h>

#include "Database.h"
#include "TernaryJoinOperator.h"
#include "CounterOperator.h"
#include "PrintOperator.h"
#include "typedefs.h"
#include <memory>
#include "utils/TemporaryDirectory.h"
#include <format>
#include <fstream>

using namespace std;
using namespace external_join;

namespace {
    struct TableConstructor {
        string testDir = TEST_DIR;
        string configFile = testDir + "test_config.csv";
        string leftFile = testDir + "Test_R.bin";
        string middleFile = testDir + "Test_S.bin";
        string rightFile = testDir + "Test_T.bin";
        std::ofstream o1;
        std::ofstream o2;
        std::ofstream o3;
        std::ofstream o4;

        TableConstructor() {
            o1 = std::ofstream(configFile);
            o2 = std::ofstream(leftFile);
            o3 = std::ofstream(middleFile);
            o4 = std::ofstream(rightFile);
        }

        ~TableConstructor() {
            std::remove(configFile.c_str());
            std::remove(leftFile.c_str());
            std::remove(middleFile.c_str());
            std::remove(rightFile.c_str());
        }

        template<typename CallableConfig, typename CallableR, typename CallableS, typename CallableT>
        void createTables(CallableConfig callbackConfig, CallableR callbackR, CallableS callbackS, CallableT callbackT) {

            o1 << "custom-db\n";
            o1 << "table-name,cardinality,attr1,attr2\n";
            callbackConfig(o1);
            o1.flush();

            callbackR(o2);
            o2.flush();

            callbackS(o3);
            o3.flush();

            callbackT(o4);
            o4.flush();
        }

    };

    class MaterializeOperator : public Operator {
    private:
        std::unique_ptr<Operator> input;
        std::vector<std::vector<attribute_t>> resultTuples;
        std::mutex mx;
        bool isSorted = false;
    public:
        explicit MaterializeOperator(std::unique_ptr<Operator> &&input) : input(std::move(input)) {}

        ~MaterializeOperator() noexcept override = default;

        void Prepare(Operator *consumer = nullptr) override {
            assert(consumer == nullptr);
            input->Prepare(this);
            tuple_width = input->getTupleSize();
        }

        void Produce() override {
            input->Produce();
        }

        void Consume(size_t, const Operator *child, std::span<attribute_t> tuple) override {
            assert(child == input.get());
            std::lock_guard<mutex> l(mx);
            resultTuples.push_back({tuple.begin(), tuple.end()});
        }

        void Consume(size_t, const Operator* , Page* ) override {}

        OperatorType getType() override {
            return Materialize;
        }
        std::vector<std::vector<attribute_t >> getTuples() {
            if (!isSorted) {
                sort(resultTuples.begin(), resultTuples.end(),
                     [](const std::vector<attribute_t> &a, const std::vector<attribute_t> &b) {
                         assert(a.size() == b.size());
                         for (size_t i = 0; i < a.size(); ++i) {
                             if (a[i] != b[i]) {
                                 return a[i] < b[i];
                             }
                         }
                         return false;
                     });
                isSorted = true;
            }
            return resultTuples;
        }


    };

    external_join::Database db;

    std::unique_ptr<MaterializeOperator> makeMaterializeOperator(
            std::vector<std::vector<attribute_t>> tableR,
            std::vector<std::vector<attribute_t>> tableS,
            std::vector<std::vector<attribute_t>> tableT
    ) {
        TableConstructor constructor;

        auto config = [&tableR, &tableS, &tableT](std::ofstream &oConfig) {
            oConfig << std::format("Test_R,{},a,b\n", tableR.size());
            oConfig << std::format("Test_S,{},b,c\n", tableS.size());
            oConfig << std::format("Test_T,{},a,c\n", tableT.size());
        };

        auto leftTableWriter = [&tableR](std::ofstream &oLeft) {
            for (const vector<attribute_t> &tuple: tableR) {
                for (size_t i = 0; i < tuple.size(); ++i) {
                    oLeft.write(reinterpret_cast<const char*>(&tuple[i]), sizeof(attribute_t));
                }
            }
        };

        auto middleTableWriter = [&tableS](std::ofstream &oMiddle) {
            for (const vector<attribute_t> &tuple: tableS) {
                for (size_t i = 0; i < tuple.size(); ++i) {
                    oMiddle.write(reinterpret_cast<const char*>(&tuple[i]), sizeof(attribute_t));
                }
            }

        };

        auto rightTableWriter = [&tableT](std::ofstream &oRight) {
            for (const vector<attribute_t> &tuple: tableT) {
                for (size_t i = 0; i < tuple.size(); ++i) {
                    oRight.write(reinterpret_cast<const char*>(&tuple[i]), sizeof(attribute_t));
                }
            }

        };

        constructor.createTables(config, leftTableWriter, middleTableWriter, rightTableWriter);
        db.Create(TEST_DIR, "test_config.csv");
        external_join::Table &r = db.GetTable("Test_R");
        external_join::Table &s = db.GetTable("Test_S");
        external_join::Table &t = db.GetTable("Test_T");

        std::unique_ptr<external_join::Operator> scanR = std::make_unique<external_join::ScanOperator>(r);
        std::unique_ptr<external_join::Operator> scanS = std::make_unique<external_join::ScanOperator>(s);
        std::unique_ptr<external_join::Operator> scanT = std::make_unique<external_join::ScanOperator>(t);
        std::unique_ptr<external_join::Operator> join;

        join = std::make_unique<external_join::TernaryJoinOperator>(std::move(scanR), std::move(scanS),
                                                                    std::move(scanT),
                                                                    JoinPair{1, 0},
                                                                    JoinPair{1, 1},
                                                                    JoinPair{0, 0},
                                                                    external_join::TernaryJoinOperator::grace_join_strategy_tag{});
        assert(join);
        return make_unique<MaterializeOperator>(std::move(join));
    }

    template<typename CallableConfig, typename CallableR, typename CallableS, typename CallableT>
    std::unique_ptr<CounterOperator> makeCounterOperator(
            CallableConfig callbackConfig,
            CallableR callbackR,
            CallableS callbackS,
            CallableT callbackT
    ) {
        TableConstructor constructor;

        constructor.createTables(callbackConfig, callbackR, callbackS, callbackT);

        db.Create(TEST_DIR, "test_config.csv");
        external_join::Table &r = db.GetTable("Test_R");
        external_join::Table &s = db.GetTable("Test_S");
        external_join::Table &t = db.GetTable("Test_T");

        std::unique_ptr<external_join::Operator> scanR = std::make_unique<external_join::ScanOperator>(r);
        std::unique_ptr<external_join::Operator> scanS = std::make_unique<external_join::ScanOperator>(s);
        std::unique_ptr<external_join::Operator> scanT = std::make_unique<external_join::ScanOperator>(t);
        std::unique_ptr<external_join::Operator> join = std::make_unique<external_join::TernaryJoinOperator>(std::move(scanR), std::move(scanS),
                                                                    std::move(scanT),
                                                                    JoinPair{1, 0},
                                                                    JoinPair{1, 1},
                                                                    JoinPair{0, 0},
                                                                    external_join::TernaryJoinOperator::grace_join_strategy_tag{});

        assert(join);
        return make_unique<CounterOperator>(std::move(join));
    }


    TEST(GraceTernaryJoinOperator, JoinNoElement) {
        std::vector<std::vector<attribute_t >> leftTable = {
                {2, 3}
        };
        std::vector<std::vector<attribute_t >> middleTable = {
                {4, 5}
        };
        std::vector<std::vector<attribute_t >> rightTable = {
                {2, 5}
        };
        std::unique_ptr<MaterializeOperator> topOperator = makeMaterializeOperator(leftTable,
                                                                                   middleTable,
                                                                                   rightTable);
        topOperator->Prepare();
        topOperator->Produce();
        ASSERT_TRUE(topOperator->getTuples().empty());
        db.Close();
    }


    TEST(GraceTernaryJoinOperator, JoinOneElement) {
        std::vector<std::vector<attribute_t >> leftTable = {
                {2, 3}
        };
        std::vector<std::vector<attribute_t >> middleTable = {
                {3, 5}
        };
        std::vector<std::vector<attribute_t >> rightTable = {
                {2, 5}
        };
        std::unique_ptr<MaterializeOperator> topOperator = makeMaterializeOperator(leftTable,
                                                                                   middleTable,
                                                                                   rightTable);

        topOperator->Prepare();
        topOperator->Produce();
        ASSERT_FALSE(topOperator->getTuples().empty());
        ASSERT_EQ(topOperator->getTuples(), std::vector<vector<attribute_t >>({{2, 3, 3, 5, 2, 5}}));
        db.Close();
    }

    TEST(GraceTernaryJoinOperator, JoinManyWithMatch) {
        const int n = 100, m = 50, k = 150;
        auto config = [n, m, k](ostream &oConfig) {
            oConfig << std::format("Test_R,{},a,b\n", n);
            oConfig << std::format("Test_S,{},b,c\n", m);
            oConfig << std::format("Test_T,{},a,c\n", k);
        };
        auto leftTableWriter = [](std::ofstream &oLeft) {
            std::vector<attribute_t > tuple = {2, 3};
            for (int i = 0; i < n; ++i) {
                oLeft.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                oLeft.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
            }
        };
        auto middleTableWriter = [](std::ofstream &oMiddle) {
            std::vector<attribute_t > tuple = {3, 5};
            for (int i = 0; i < m; ++i) {
                oMiddle.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                oMiddle.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
            }
        };
        auto rightTableWriter = [](std::ofstream &oRight) {
            std::vector<attribute_t > tuple_0 = {0, 5};
            std::vector<attribute_t > tuple = {2, 5};

            for (int i = 0; i < k; ++i) {
                if (i == 0) {
                    oRight.write(reinterpret_cast<const char*>(&tuple_0[0]), sizeof(attribute_t));
                    oRight.write(reinterpret_cast<const char*>(&tuple_0[1]), sizeof(attribute_t));
                }
                else {
                    oRight.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                    oRight.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));

                }
            }
        };
        std::unique_ptr<CounterOperator> topOperator = makeCounterOperator(config,
                                                                           leftTableWriter,
                                                                           middleTableWriter,
                                                                           rightTableWriter);
        topOperator->Prepare();
        topOperator->Produce();

        ASSERT_EQ(topOperator->getCountTuples(), n * m * (k - 1));
        db.Close();
    }

    TEST(GraceTernaryJoinOperator, JoinManyWithNoMatch) {
        const int n = 100, m = 50, k = 150;
        auto config = [n, m, k](ostream &oConfig) {
            oConfig << std::format("Test_R,{},a,b\n", n);
            oConfig << std::format("Test_S,{},b,c\n", m);
            oConfig << std::format("Test_T,{},a,c\n", k);
        };
        auto leftTableWriter = [](std::ofstream &oLeft) {
            std::vector<attribute_t > tuple = {2, 3};
            for (int i = 0; i < n; ++i) {
                oLeft.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                oLeft.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
            }
        };
        auto middleTableWriter = [](std::ofstream &oMiddle) {
            std::vector<attribute_t > tuple = {8, 5};

            for (int i = 0; i < m; ++i) {
                oMiddle.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                oMiddle.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
            }
        };
        auto rightTableWriter = [](std::ofstream &oRight) {
            std::vector<attribute_t > tuple = {2, 5};

            for (int i = 0; i < k; ++i) {
                oRight.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                oRight.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
            }
        };
        std::unique_ptr<CounterOperator> topOperator = makeCounterOperator(config,
                                                                           leftTableWriter,
                                                                           middleTableWriter,
                                                                           rightTableWriter);
        topOperator->Prepare();
        topOperator->Produce();

        ASSERT_EQ(topOperator->getCountTuples(), 0);
        db.Close();

    }

    TEST(GraceTernaryJoinOperator, EmptyTables) {
        for (int msk = 0; msk < 0b111; ++msk) {
            int n = (msk & (0b001)) ? 1 : 0;
            int m = (msk & (0b010)) ? 1 : 0;
            int k = (msk & (0b100)) ? 1 : 0;
            auto config = [n, m, k](ostream &oConfig) {
                oConfig << std::format("Test_R,{},a,b\n", n);
                oConfig << std::format("Test_S,{},b,c\n", m);
                oConfig << std::format("Test_T,{},a,c\n", k);
            };
            auto leftTableWriter = [n](std::ofstream &oLeft) {
                std::vector<attribute_t > tuple = {2, 3};
                for (int i = 0; i < n; ++i) {
                    oLeft.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                    oLeft.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
                }
            };
            auto middleTableWriter = [m](std::ofstream &oMiddle) {
                std::vector<attribute_t > tuple = {3, 5};
                for (int i = 0; i < m; ++i) {
                    oMiddle.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                    oMiddle.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
                }
            };
            auto rightTableWriter = [k](std::ofstream &oRight) {
                std::vector<attribute_t > tuple = {2, 5};
                for (int i = 0; i < k; ++i) {
                    oRight.write(reinterpret_cast<const char*>(&tuple[0]), sizeof(attribute_t));
                    oRight.write(reinterpret_cast<const char*>(&tuple[1]), sizeof(attribute_t));
                }
            };
            std::unique_ptr<CounterOperator> topOperator = makeCounterOperator(config,
                                                                               leftTableWriter,
                                                                               middleTableWriter,
                                                                               rightTableWriter);

            topOperator->Prepare();
            topOperator->Produce();
            ASSERT_EQ(topOperator->getCountTuples(), 0);
            db.Close();
        }
    }
}