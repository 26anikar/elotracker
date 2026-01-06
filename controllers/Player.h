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

class Player : public drogon::HttpController<Player>
{
  public:
    METHOD_LIST_BEGIN
    METHOD_ADD(Player::getPlayer, "/{1}", Get);
    METHOD_LIST_END
    Player();
    void getPlayer(const HttpRequestPtr &req,
		   std::function<void (const HttpResponsePtr &)> &&callback,
		   std::string playerArg);
  DB *fidememberdb;
  DB *uscfmemberdb;
  DB *fidemembersupplementdb;
  DB *uscfmembersupplementdb;

};
