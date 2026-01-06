#include "MainCtrl.h"

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
      return nullptr;
    }
  }

  // open DB
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = false;
  options.max_open_files = -1;
  options.skip_checking_sst_file_sizes_on_db_open = true;
  DB* tmp_db;
  Status s = DB::OpenAsSecondary(options, dbPath, secondaryPath, &tmp_db);
  if (!s.ok()) {
    std::cerr << "Failed to open secondary DB: " << s.ToString() << std::endl;
    return nullptr;
  }
  return tmp_db;
}

MainCtrl::MainCtrl() {
  topplayersdb = getSecondaryDB(kTopPlayersDBPath, kTopPlayersSecondaryDBPath);
}

void fetchfromdb(DB* db, ReadOptions ropts, const vector<string>& keys,
                 HttpViewData& data, bool isFide) {
  for (auto& k : keys) {
    string key;
    string v;
    if (isFide) {
      key = "202407-fide-" + k;
    } else {
      key = "202406-" + k;
    }
    Status s = db->Get(ropts, key, &v);
    if (s.ok()) {
      // cout << " Got okay for key = " << k << endl;
      Deserializer<StreamReader<std::stringstream>> deserializer{v};
      TopPlayers topPlayers;
      deserializer.Read(&topPlayers);
      data.insert(k, topPlayers);
    }
  }
}

void MainCtrl::asyncHandleHttpRequest(
    const HttpRequestPtr& req,
    std::function<void(const HttpResponsePtr&)>&& callback) {
  HttpViewData data;
  ReadOptions ropts;
  ropts.verify_checksums = true;
  ropts.total_order_seek = true;
#if 0
  Iterator* iter = topplayersdb->NewIterator(ropts);
  int count = 0;
  iter->SeekToFirst();
  for (; iter->Valid() ; iter->Next()) {
    string key = iter->key().ToString() ;
    string value = iter->value().ToString() ;
    //cerr << " key = *" << key << "*" << endl;
    count++;
    if (key.find("202401-Overall") == -1) {
      continue;
    }

    Deserializer<StreamReader<std::stringstream>> deserializer{value};
    TopPlayers topPlayers;
    deserializer.Read(&topPlayers);
    cerr << " key = *" << key << "*" << endl;
    for (const auto& kv: topPlayers.playerMap) {
      cerr << "map key = " << kv.first << " name val = " << kv.second.name << endl;
    }
    
  }
#endif

  vector<string> uscfClassicalKeys{"Overall", "Top Women", "Top Under Age 21",
                                   "Top Girls Under 21"};
  vector<string> uscfRapidKeys{"Quick Overall", "Top Quick Women",
                               "Top Quick Under Age 21",
                               "Top Quick Girls Under 21"};
  vector<string> uscfBlitzKeys{"Blitz Overall", "Top Blitz Women",
                               "Top Blitz Under Age 21",
                               "Top Blitz Girls Under Age 21"};

  vector<string> fideClassicalKeys{"Top 100 Players", "Top 100 Women",
                                   "Top 100 Juniors", "Top 100 Girls"};
  vector<string> fideRapidKeys{"Rapid Top 100 Players", "Rapid Top 100 Women",
                               "Rapid Top 100 Juniors", "Rapid Top 100 Girls"};
  vector<string> fideBlitzKeys{"Blitz Top 100 Players", "Blitz Top 100 Women",
                               "Blitz Top 100 Juniors", "Blitz Top 100 Girls"};

#if 0
  std::vector<rocksdb::PinnableSlice> values(24);
  std::vector<rocksdb::Status> statuses(24);
  std::vector<rocksdb::Slice> querySlices;
  //querySlices.emplace_back("key1");
  //querySlices.emplace_back("key2");
  //querySlices.emplace_back("key3");
  for (const auto& k: uscfClassicalKeys) {
    string key = "202402-" + k;
    //string key = k;
    //querySlices.emplace_back("202402-" + k);
    querySlices.push_back(key);

    string v;
    Status s = topplayersdb->Get(ropts, key, &v);
    if (s.ok()) {
      cout << " Got success \n";
    }
  }
  for (const auto& k: uscfRapidKeys) {
    //break;
    querySlices.emplace_back("202402-" + k);
  }
  for (const auto& k: uscfBlitzKeys) {
    //break;
    querySlices.emplace_back("202402-" + k);
  }

  for (const auto& k: fideClassicalKeys) {
    //break;
    querySlices.emplace_back("202402-fide-" + k);
  }
  for (const auto& k: fideRapidKeys) {
    //break;
    querySlices.emplace_back("202402-fide-" + k);
  }
  for (const auto& k: fideBlitzKeys) {
    //break;
    querySlices.emplace_back("202402-fide-" + k);
  }
  cout << " query slices size = " << querySlices.size() << endl;

#if 0
  std::vector<string> val(1);
  statuses = topplayersdb->MultiGet(rocksdb::ReadOptions(), querySlices, &val);
  for (int i=0; i < querySlices.size(); i++) {
    const auto status = statuses[i];
    if (status.ok()) {
      const auto value = values[i].ToString();
      cout << "Got value = " << value << endl;
    }
  }
#endif

  topplayersdb->MultiGet(rocksdb::ReadOptions(),
			 topplayersdb->DefaultColumnFamily(),
			 querySlices.size(),
			 querySlices.data(),
			 values.data(),
			 statuses.data());
  for (int i=0; i < querySlices.size(); i++) {
    const auto status = statuses[i];
    if (status.ok()) {
      const auto value = values[i].ToString();
      cout << "Got value = " << value << endl;
      //cout << " Got okay for key = " << k << endl;
      Deserializer<StreamReader<std::stringstream>> deserializer{value};
      TopPlayers topPlayers;
      deserializer.Read(&topPlayers);
      string k;
      if (i < 4) {
	k = uscfClassicalKeys[i];
      }
      if (i >= 4 && i < 8) {
	k = uscfRapidKeys[i];
      }
      if (i >= 8 && i < 12) {
	k = uscfBlitzKeys[i];
      }
      if (i >= 12 && i < 16) {
	k = fideClassicalKeys[i];
      }
      if (i >= 16 && i < 20) {
	k = fideRapidKeys[i];
      }
      if (i >= 20 && i < 23) {
	k = fideBlitzKeys[i];
      }
      data.insert(k, topPlayers);      
    } else {
      if (status.IsNotFound()) {
	cout << " Key not found \n";
      }
    }
  }
#endif

#if 1
  fetchfromdb(topplayersdb, ropts, uscfClassicalKeys, data, false);
  fetchfromdb(topplayersdb, ropts, uscfRapidKeys, data, false);
  fetchfromdb(topplayersdb, ropts, uscfBlitzKeys, data, false);

  fetchfromdb(topplayersdb, ropts, fideClassicalKeys, data, true);
  fetchfromdb(topplayersdb, ropts, fideRapidKeys, data, true);
  fetchfromdb(topplayersdb, ropts, fideBlitzKeys, data, true);
#endif

  string seo_description =
      "Discover your global standing with elotracker.com – the ultimate "
      "platform for tracking FIDE and USCF chess ratings. Monitor and compare "
      "your chess Elo ratings, explore player statistics, and stay updated "
      "with international chess federation rankings.";
  string title = "EloTracker - chess ratings and rankings in one place";
  data.insert("description", seo_description);
  data.insert("title", title);

  data.insert("uscf_classical_keys", uscfClassicalKeys);
  data.insert("uscf_rapid_keys", uscfRapidKeys);
  data.insert("uscf_blitz_keys", uscfBlitzKeys);
  data.insert("fide_classical_keys", fideClassicalKeys);
  data.insert("fide_rapid_keys", fideRapidKeys);
  data.insert("fide_blitz_keys", fideBlitzKeys);

  auto resp = HttpResponse::newHttpViewResponse("Main.csp", data);
  callback(resp);
}
