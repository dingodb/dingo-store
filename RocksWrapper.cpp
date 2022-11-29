//
// Created by 咖啡豆 on 2022/11/29.
//

#include "RocksWrapper.h"
#include <braft/util.h>
namespace example {

    RocksWrapper::RocksWrapper(std::string dbPath) {
        this->dbPath = dbPath;
        rocksdb::Options options;
        options.IncreaseParallelism();
        options.create_if_missing = true;

        // open DB
        rocksdb::Status s = rocksdb::DB::Open(options, dbPath, &this->instance);
        if (!s.ok()) {
            LOG(ERROR) << "Fail to start db on path:" << dbPath;
            return;
        }
        LOG(INFO) << "Create DB on Path:" << this->dbPath << "Success" << std::endl;
    }

    rocksdb::Status RocksWrapper::Get(const rocksdb::Slice &key, std::string *value) {
         return this->instance->Get(rocksdb::ReadOptions(), key, value);
    }

    rocksdb::Status RocksWrapper::Put(const rocksdb::Slice &key,const rocksdb::Slice &value) {
        return this->instance->Put(rocksdb::WriteOptions(), key, value);
    }
}