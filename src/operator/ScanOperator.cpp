#include "ScanOperator.h"
#include "Table.h"
#include "utils/Utils.h"
#include "Page.h"

using namespace std;

namespace external_join {

    ScanOperator::ScanOperator(Table &table): table(table), consumer_(nullptr) {}


    void ScanOperator::Prepare(Operator *consumer) {
        this->consumer_ = consumer;
        tuple_width = 2;
    }

    void ScanOperator::Produce() {
        assert(table.file_descriptor != -1);
        constexpr size_t max_tuples_per_page = PAGE_SIZE >> 4;

        size_t pageCount = (table.cardinality + max_tuples_per_page - 1) / max_tuples_per_page;;
        size_t lastPageTupleCount = table.cardinality % max_tuples_per_page;
        for(size_t pageIdx = 0 ; pageIdx < pageCount; ++pageIdx) {
            // WARNING -> we assume all those tables are only graph edges with width = 2
            // 2[width] * 8[sizeof (uint64_t)] = 16
            Page p(tuple_width, max_tuples_per_page << 4);

            p.deserializePage(pageIdx, table.file_descriptor);
            size_t n = p.getNumTuples();
            if (pageIdx + 1 == pageCount) {
                assert(lastPageTupleCount <= n);
                n = lastPageTupleCount == 0 ? n : lastPageTupleCount;
                if (lastPageTupleCount != 0)
                    p.resize(n << 1);
            }
            consumer_->Consume(0, this, &p);
        }
    }

}