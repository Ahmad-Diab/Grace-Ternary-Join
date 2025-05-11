#include "Bucket.h"
#include "typedefs.h"

namespace external_join {

    void Bucket::clearInMemoryPage() {
        cachedPage->clear();
    }

    void Bucket::clearPersistentPages() {
        this->tuple_count = 0;
        this->clearInMemoryPage();
    }

    void Bucket::writeCachedPage() {
        assert(cachedPage->isFull() || cachedPage->empty());
        if (cachedPage->isFull()) {
            serialize_bucket();
            clearInMemoryPage();
        }
    }


    Bucket::Bucket(
            size_t max_tuples,
            size_t tuple_width,
            std::unique_ptr<TemporaryDirectory> *bucketFolder) :
                tuple_count(0),
                bucketFile(std::make_unique<TemporaryFile>(bucketFolder)),
                cachedPage(std::make_unique<Page>(tuple_width, max_tuples * tuple_width * sizeof(uint64_t))),
                max_tuples(max_tuples),
                tuple_width(tuple_width) {}

    void Bucket::initialize(
            size_t max_tuples,
            size_t tuple_width,
            std::unique_ptr<TemporaryDirectory> *bucketFolder)
    {
        this->tuple_count = 0;
        this->bucketFile = std::make_unique<TemporaryFile>(bucketFolder);
        this->cachedPage = std::make_unique<Page>(tuple_width, max_tuples * tuple_width * sizeof(uint64_t));
        this->max_tuples = max_tuples;
        this->tuple_width = tuple_width;
    }

    void Bucket::serialize_bucket() const {
        if (tuple_count == 0) {
            return;
        }
        size_t page_count = getPageCount();
        assert(page_count > 0);
        cachedPage->serializePage(page_count - 1, bucketFile.get());
    }

}