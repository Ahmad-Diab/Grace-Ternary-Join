
#include <gtest/gtest.h>
#include "joinstrategy/Bucket.h"
#include <filesystem>
#include "constants.h"
#include <unordered_map>
using namespace std;
using namespace external_join;

namespace {
    struct TestTuple {
        size_t tupleWidth;
        std::vector<uint64_t > values;
        explicit TestTuple(size_t tupleWidth) : tupleWidth(tupleWidth), values(tupleWidth){}
        explicit TestTuple(std::vector<uint64_t> values): tupleWidth(values.size()), values(values) {}
        void setValues(std::vector<uint64_t > values) {
            assert(tupleWidth = values.size());
            this->values = values;
        }
        
    };
    struct TestBucket {
        vector<Page> pages;

        explicit TestBucket(size_t num_pages): pages(num_pages) {}

        void read_pages(int fd, size_t max_tuples, size_t tuple_width, size_t totalTuples) {
            // size in bytes
            size_t page_size = max_tuples * tuple_width * UINT64_SIZE;
            for (size_t page_id = 0 ; page_id < pages.size(); ++page_id) {
                assert(totalTuples);
                // offset in bytes
                Page page = Page(tuple_width, page_size);
                size_t page_offset = page_id * page_size;
                // size in bytes
                size_t tuple_size = UINT64_SIZE * tuple_width;
                size_t num_tuples = std::min(totalTuples, max_tuples);

//                vector<TestTuple > tuples(num_tuples, TestTuple(tuple_width));

                for(size_t tuple_idx = 0 ; tuple_idx < num_tuples ; ++tuple_idx) {
                    TestTuple t = TestTuple(tuple_width);
                    int64_t tuple_offset = page_offset + tuple_idx * tuple_size;
                    vector<uint64_t > values(tuple_width);
                    Utils::readBlock(fd, tuple_offset, values.data(), UINT64_SIZE * tuple_width);
                    t.setValues(values);
                    page.addTuple(t.values);
                }
                pages[page_id] = page;
                totalTuples -= num_tuples;
            }
        }
    };

    TEST(TernaryBucketTest, FileExist) {
        unique_ptr<TemporaryDirectory> folder = make_unique<TemporaryDirectory>();
        string folderPath = folder->getFilename();
        // max_tuples per page
        Bucket b = Bucket(5, 2, &folder);
        string bucketFilename = b.getFilename();
        std::string filepath = folderPath + "/" + bucketFilename;
        ASSERT_TRUE(std::filesystem::exists(filepath));
    }

    TEST(TernaryBucketTest, WriteTuples) {
        size_t max_tuples = 5;
        size_t tuple_width = 2;
        unique_ptr<TemporaryDirectory> folder = make_unique<TemporaryDirectory>();
        Bucket b = Bucket( max_tuples, tuple_width, &folder);
        int fd = b.getFileDescriptor();
        vector<TestTuple> tuples = {
                TestTuple({1, 2}),
                TestTuple({6, 2}),
                TestTuple({23, 2}),
                TestTuple({9, 2}),
                TestTuple({10, 2}),
        };
        for(auto& t: tuples) {
            b.addTuple(t.values);
        }
        b.serialize_bucket();

        TestBucket tb(1);
        tb.read_pages(fd, max_tuples, tuple_width, tuples.size());
        for (size_t page_idx = 0 ; page_idx < 1; ++page_idx) {
            Page& p = tb.pages[page_idx];
            for (size_t tuple_idx = 0 ; tuple_idx < p.getNumTuples(); ++tuple_idx) {
                std::span<uint64_t > tupleSpan = p.getTuple(tuple_idx);
                std::vector<uint64_t > acutalTuple(tupleSpan.begin(), tupleSpan.end());
                ASSERT_EQ(tuples[page_idx * max_tuples + tuple_idx].values, acutalTuple);
            }
        }
    }

    TEST(TernaryBucketTest, ReadTuples) {
        // PAGE SIZE = 16KB , tuple_width = 2, max_tuples = 16KB / (2 * 8) -> 1K tuples per page
        size_t max_tuples = 2;
        size_t tuple_width = 2;
        unique_ptr<TemporaryDirectory> folder = make_unique<TemporaryDirectory>();
        Bucket b = Bucket(max_tuples, tuple_width, &folder);
        int fd = b.getFileDescriptor();
        vector<TestTuple> tuples = {
                TestTuple({1, 2}),
                TestTuple({6, 2}),
                TestTuple({23, 2}),
                TestTuple({9, 2}),
                TestTuple({10, 2}),
        };
        for (auto &t: tuples) {
            b.addTuple(t.values);
        }
        b.serialize_bucket();

        size_t i = 0;
        for (std::span<uint64_t >  persistedTuple : b) {
            std::vector<uint64_t > persistedTupleValues(persistedTuple.begin(), persistedTuple.end());
            ASSERT_EQ(persistedTupleValues, tuples[i].values);
            i++;
        }
    }

    TEST(TernaryBucketTest, ExtensiveTuples)
    {
        const size_t tuple_width = 2;
        const size_t max_tuples = PAGE_SIZE >> 4;
        unique_ptr<TemporaryDirectory> folder = make_unique<TemporaryDirectory>();
        Bucket b = Bucket(max_tuples, tuple_width, &folder);
        size_t totalTuples = (GIGABYTE_SIZE) >> 4;

        for(size_t i = 0 ; i < totalTuples; ++i) {
            TestTuple t = TestTuple({1, 2});
            b.addTuple(t.values);
        }

        b.serialize_bucket();

        size_t count = 0;

        vector<uint64_t > expectedValues = {1 , 2};

        for (std::span<uint64_t > t : b) {
            std::vector<uint64_t > actualValues(t.begin(), t.end());
            ASSERT_EQ(actualValues, expectedValues);
            count++;
        }
        ASSERT_EQ(count, totalTuples);
    }

}