#pragma once

#include <drogon/HttpController.h>
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

class Players : public drogon::HttpController<Players>
{
  public:
    METHOD_LIST_BEGIN
    METHOD_ADD(Players::getTopPlayers, "/top-players", Get);
    METHOD_ADD(Players::getTopWomen, "/top-women", Get);
    METHOD_ADD(Players::getTopJuniors, "/top-juniors", Get);
    METHOD_LIST_END
    Players();
    void getTopPlayers(const HttpRequestPtr &req,
		       std::function<void (const HttpResponsePtr &)> &&callback);
    void getTopWomen(const HttpRequestPtr &req,
		       std::function<void (const HttpResponsePtr &)> &&callback);
    void getTopJuniors(const HttpRequestPtr &req,
		       std::function<void (const HttpResponsePtr &)> &&callback);
    DB *topplayersdb;
};
