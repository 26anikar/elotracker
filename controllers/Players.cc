#include "Players.h"
// Folly removed - not used in this file
#include <dirent.h>
#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "/mnt/disks/data2/common.h"

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

const char kTopPlayersDBPath[] = "/mnt/disks/data2/elotracker/data/top_players";
const char kTopPlayersSecondaryDBPath[] =
    "/mnt/disks/data2/top_players_secondary";

static std::atomic<int> &ShouldSecondaryWait() {
  static std::atomic<int> should_secondary_wait{1};
  return should_secondary_wait;
}

static DB *getSecondaryDB(const char *dbPath, const char *secondaryPath) {
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
  DB *tmp_db;
  Status s = DB::OpenAsSecondary(options, dbPath, secondaryPath, &tmp_db);
  assert(s.ok());
  return tmp_db;
}

Players::Players() {
  topplayersdb = getSecondaryDB(kTopPlayersDBPath, kTopPlayersSecondaryDBPath);
}

static void fetchfromdb(DB *db, ReadOptions ropts, const vector<string> &keys,
                        HttpViewData &data) {
  for (auto &k : keys) {
    string v;
    Status s = db->Get(ropts, "202401-" + k, &v);
    if (s.ok()) {
      // cout << " Got okay for key = " << k << endl;
      Deserializer<StreamReader<std::stringstream>> deserializer{v};
      TopPlayers topPlayers;
      deserializer.Read(&topPlayers);
      data.insert(k, topPlayers);
    }
  }
}

void Players::getTopPlayers(
    const HttpRequestPtr &req,
    std::function<void(const HttpResponsePtr &)> &&callback) {
  HttpViewData data;
  ReadOptions ropts;
  ropts.verify_checksums = true;
  ropts.total_order_seek = true;

  vector<string> topPlayersKeys{"Overall", "Quick Overall", "Blitz Overall"};

  fetchfromdb(topplayersdb, ropts, topPlayersKeys, data);

  data.insert("top_players_keys", topPlayersKeys);

  // cant add char* directly as second argument in data.insert - it takes a
  // string
  string s = "top_players";
  data.insert("page_type", s);

  string seo_description = "Top Chess Players ratings and rankings";
  string title = "EloTracker - Top Chess Players";
  data.insert("description", seo_description);
  data.insert("title", title);

  auto resp = HttpResponse::newHttpViewResponse("PlayersLayout.csp", data);
  callback(resp);
}

void Players::getTopWomen(
    const HttpRequestPtr &req,
    std::function<void(const HttpResponsePtr &)> &&callback) {
  HttpViewData data;
  ReadOptions ropts;
  ropts.verify_checksums = true;
  ropts.total_order_seek = true;

  vector<string> topWomenKeys{"Top Women", "Top Quick Women",
                              "Top Blitz Women"};

  fetchfromdb(topplayersdb, ropts, topWomenKeys, data);

  data.insert("top_women_keys", topWomenKeys);

  // cant add char* directly as second argument in data.insert - it takes a
  // string
  string s = "top_women";
  data.insert("page_type", s);

  string seo_description = "Top Women Chess Players ratings and rankings";
  string title = "EloTracker - Top Women Chess Players";
  data.insert("description", seo_description);
  data.insert("title", title);

  auto resp = HttpResponse::newHttpViewResponse("PlayersLayout.csp", data);
  callback(resp);
}

void Players::getTopJuniors(
    const HttpRequestPtr &req,
    std::function<void(const HttpResponsePtr &)> &&callback) {
  HttpViewData data;
  ReadOptions ropts;
  ropts.verify_checksums = true;
  ropts.total_order_seek = true;

  vector<string> topJuniorsKeys{"Top Under Age 21", "Top Quick Under Age 21",
                                "Top Blitz Under Age 21"};

  fetchfromdb(topplayersdb, ropts, topJuniorsKeys, data);
  string s = "top_juniors";
  data.insert("page_type", s);

  data.insert("top_juniors_keys", topJuniorsKeys);

  string seo_description = "Top Juniors Chess Players ratings and rankings";
  string title = "EloTracker - Top Junior Chess Players";
  data.insert("description", seo_description);
  data.insert("title", title);

  auto resp = HttpResponse::newHttpViewResponse("PlayersLayout.csp", data);
  callback(resp);
}
