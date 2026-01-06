#pragma once

#include <drogon/HttpSimpleController.h>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace drogon;


using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::Iterator;

using namespace drogon;

class MainCtrl : public drogon::HttpSimpleController<MainCtrl>
{
  public:
    void asyncHandleHttpRequest(const HttpRequestPtr& req, std::function<void (const HttpResponsePtr &)> &&callback) override;
    PATH_LIST_BEGIN
    // list path definitions here;
    PATH_ADD("/", Get);
    PATH_LIST_END
    MainCtrl();
    DB *topplayersdb;
};
