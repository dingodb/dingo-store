//
// Created by 咖啡豆 on 2022/11/29.
//

#ifndef BLOCK_ROCKSWRAPPER_H
#define BLOCK_ROCKSWRAPPER_H

#include <string>
#include "rocksdb/db.h"
#include "rocksdb/convenience.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace example {

class RocksWrapper {
    public:
        RocksWrapper(std::string dbPath);
        rocksdb::Status Get(const rocksdb::Slice &key,
                            std::string *value);

        rocksdb::Status Put(const rocksdb::Slice &key,
                            const rocksdb::Slice &value);
    private:
        rocksdb::DB* instance = nullptr;
        std::string dbPath;
    };
}

#endif //BLOCK_ROCKSWRAPPER_H
