#include "Player.h"

#include <dirent.h>
#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "/mnt/disks/data2/common.h"
#include "string_utils.h"

using namespace std;
using nop::Deserializer;
using nop::Optional;
using nop::Serializer;
using nop::StreamReader;
using nop::StreamWriter;

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

const char kUSCFMemberDBPath[] =
    "/mnt/disks/data2/elotracker/data/uscf_members";
const char kUSCFMemberSecondaryDBPath[] =
    "/mnt/disks/data2/uscf_members_secondary";

const char kFIDEMemberDBPath[] =
    "/mnt/disks/data2/elotracker/data/fide_members";
const char kFIDEMemberSecondaryDBPath[] =
    "/mnt/disks/data2/fide_members_secondary";

const char kUSCFMemberSupplementDBPath[] =
    "/mnt/disks/data2/elotracker/data/uscf_member_supplement";
const char kUSCFMemberSupplementSecondaryDBPath[] =
    "/mnt/disks/data2/uscf_member_supplement_secondary";

const char kFIDEMemberSupplementDBPath[] =
    "/mnt/disks/data2/elotracker/data/fide_member_supplement";
const char kFIDEMemberSupplementSecondaryDBPath[] =
    "/mnt/disks/data2/fide_member_supplement_secondary";

static std::atomic<int>& ShouldSecondaryWait() {
  static std::atomic<int> should_secondary_wait{1};
  return should_secondary_wait;
}

static DB* getSecondaryDB(const char* dbPath, const char* secondaryPath) {
  // Create directory if necessary
  if (nullptr == opendir(secondaryPath)) {
    int ret = mkdir(secondaryPath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    if (ret < 0) {
      perror("failed to create directory for secondary instance");
      exit(0);
    }
  }

  // open DB
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = false;
  options.max_open_files = -1;
  options.skip_checking_sst_file_sizes_on_db_open = true;

  DB* tmp_db;
  Status s = DB::OpenAsSecondary(options, dbPath, secondaryPath, &tmp_db);
  cerr << " opening secondary path " << secondaryPath << endl;
  assert(s.ok());
  return tmp_db;
}

Player::Player() {
  fidemembersupplementdb = getSecondaryDB(kFIDEMemberSupplementDBPath,
                                          kFIDEMemberSupplementSecondaryDBPath);
  uscfmemberdb = getSecondaryDB(kUSCFMemberDBPath, kUSCFMemberSecondaryDBPath);
  fidememberdb = getSecondaryDB(kFIDEMemberDBPath, kFIDEMemberSecondaryDBPath);
  uscfmembersupplementdb = getSecondaryDB(kUSCFMemberSupplementDBPath,
                                          kUSCFMemberSupplementSecondaryDBPath);
}

void Player::getPlayer(const HttpRequestPtr& req,
                       std::function<void(const HttpResponsePtr&)>&& callback,
                       std::string playerInfo) {
  HttpViewData data;

  auto v = string_utils::split(playerInfo, '-');

  if (v.size() == 0) {
    auto resp = HttpResponse::newHttpViewResponse("BadRequestLayout.csp", data);
    callback(resp);
    return;
  }
  int pos = v.size() - 1;
  string playerId = std::string(v[pos]);
  string uscfId = "";
  string fideId = "";

  if (!playerId.empty()) {
    if (playerId[0] == 'U') {
      uscfId = playerId.substr(1);
    } else if (playerId[0] == 'F') {
      fideId = playerId.substr(1);
    } else {
      auto resp =
          HttpResponse::newHttpViewResponse("BadRequestLayout.csp", data);
      callback(resp);
      return;
    }
  }

  ROCKSDB_NAMESPACE::ReadOptions ropts;
  ropts.verify_checksums = true;
  ropts.total_order_seek = true;

  string value;
  Member fideMember;
  Member uscfMember;
  MemberSupplementRating fideMemberSupplement;
  MemberSupplementRating uscfMemberSupplement;
  string name;
  if (!fideId.empty()) {
    ROCKSDB_NAMESPACE::Status s = fidememberdb->Get(ropts, fideId, &value);
    if (!s.ok()) {
      cout << " Couldnt find id " << fideId << endl;
      auto resp =
          HttpResponse::newHttpViewResponse("BadRequestLayout.csp", data);
      callback(resp);
      return;
    }
    Deserializer<StreamReader<std::stringstream>> deserializer{value};
    deserializer.Read(&fideMember);
    name = fideMember.name;
    data.insert("is_fide", 1);
    data.insert("is_uscf", 0);

    {
      s = fidemembersupplementdb->Get(ropts, fideId, &value);
      Deserializer<StreamReader<std::stringstream>> deserializer{value};
      deserializer.Read(&fideMemberSupplement);
    }
  }
  if (!uscfId.empty()) {
    data.insert("is_uscf", 1);
    ROCKSDB_NAMESPACE::Status s = uscfmemberdb->Get(ropts, uscfId, &value);
    if (!s.ok()) {
      cout << " Couldnt find uscfid " << uscfId << endl;
      auto resp =
          HttpResponse::newHttpViewResponse("BadRequestLayout.csp", data);
      callback(resp);
      return;
    }
    Deserializer<StreamReader<std::stringstream>> deserializer{value};
    deserializer.Read(&uscfMember);
    name = uscfMember.name;
    if (uscfMember.fide_id != "") {
      fideId = uscfMember.fide_id;
    }
    if (!fideId.empty()) {
      {
        s = fidememberdb->Get(ropts, fideId, &value);
        Deserializer<StreamReader<std::stringstream>> deserializer{value};
        deserializer.Read(&fideMember);
      }
      {
        s = fidemembersupplementdb->Get(ropts, fideId, &value);
        Deserializer<StreamReader<std::stringstream>> deserializer{value};
        deserializer.Read(&fideMemberSupplement);
      }
      data.insert("is_fide", 1);
    } else {
      data.insert("is_fide", 0);
    }
    {
      s = uscfmembersupplementdb->Get(ropts, uscfId, &value);
      Deserializer<StreamReader<std::stringstream>> deserializer{value};
      deserializer.Read(&uscfMemberSupplement);
    }
  }

  string seo_description = "FIDE and USCF rating for " + name;
  string title = name + " - EloTracker";
  data.insert("description", seo_description);
  data.insert("title", title);

  data.insert("uscf_member", uscfMember);
  data.insert("fide_member", fideMember);
  data.insert("uscf_member_supplements", uscfMemberSupplement);
  data.insert("fide_member_supplements", fideMemberSupplement);
  auto resp = HttpResponse::newHttpViewResponse("PlayerView.csp", data);
  callback(resp);
}
