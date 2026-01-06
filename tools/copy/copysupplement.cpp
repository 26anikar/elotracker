#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>

#include <boost/algorithm/string.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/rfc2818_verification.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time/gregorian/greg_month.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/filesystem.hpp>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "boost/date_time/gregorian/gregorian_types.hpp"
#include "common.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using nop::Deserializer;
using nop::Optional;
using nop::Serializer;
using nop::StreamReader;
using nop::StreamWriter;

using namespace std;

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

namespace beast = boost::beast;
namespace asio = boost::asio;
namespace ssl = asio::ssl;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;
using namespace std;
using json = nlohmann::json;

const char kMemberSupplementPath[] =
    "/mnt/disks/data2/elotracker/data/uscf_member_supplement";
DB *memberSupplementDb;

// trim from start (in place)
static inline void ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
            return !std::isspace(ch);
          }));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](unsigned char ch) { return !std::isspace(ch); })
              .base(),
          s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
  rtrim(s);
  ltrim(s);
}

int main(int argc, char *argv[]) {
  Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  setenv("TZ", "PST8PDT", 1);
  std::time_t t = std::time(0);  // get time now
  std::tm *now = std::localtime(&t);
  int year = now->tm_year;
  int month = now->tm_mon;
  int day = now->tm_mday;
  std::stringstream ss;
  ss << (year + 1900) << "-";
  ss << std::setw(2) << std::setfill('0') << (month + 1) << "-";
  ss << std::setw(2) << std::setfill('0') << day;

  // open DB
  Status s = DB::Open(options, kMemberSupplementPath, &memberSupplementDb);
  assert(s.ok());

  for (boost::filesystem::recursive_directory_iterator end,
       dir("./uscf_supplements");
       dir != end; ++dir) {
    if (!boost::filesystem::is_directory(dir->path())) {
      string filename = dir->path().filename().string();
      if ((filename.find(".txt") == -1) && (filename.find(".TXT") == -1)) {
        continue;
      }
      string path = dir->path().string();
      // cout << " filename is " << filename << " path is  " << path << endl;
      std::ifstream infile(path);
      std::string line;
      int linecount = 0;
      vector<string> v;

      string date = filename;
      date = path;
      bool isQuick = false;
      bool isBlitz = false;
      int pos = date.find("RSQ");
      if (pos == -1) {
        pos = date.find("RSB");
        if (pos == -1) {
          pos = date.find("RS");
          date.replace(0, pos + 2, "");
        } else {
          if (filename.find("RSB") != -1) {
            isBlitz = true;
          }
          date.replace(0, pos + 3, "");
        }

      } else {
        isQuick = true;
        date.replace(0, pos + 3, "");
      }
      pos = date.find("/");
      if (pos != -1) {
        date.replace(pos, date.length() - pos + 1, "");
      }
      pos = date.find("T");
      if (pos != -1) {
        date.replace(pos, date.length() - pos + 1, "");
      }
      // lets add "20" before year
      date = "20" + date;
      cout << " date is " << date << endl;

      while (std::getline(infile, line)) {
        SupplementRating supplementRating;
        boost::split(v, line, boost::is_any_of("\t"));
        string memberId = "";
        string regular_rating = "";
        string quick_rating = "";
        string blitz_rating = "";
        string online_regular_rating = "";
        string online_quick_rating = "";
        string online_blitz_rating = "";
        for (int i = 0; i < v.size(); i++) {
          // cout << v[i] << endl;
          if (i == 1) {
            memberId = v[i];
          }
          if (i == 4) {
            supplementRating.regular_rating = v[i];
            // cout << " regular ating is " << supplementRating.regular_rating
            // << endl;
          }
          if (i == 5) {
            if (isBlitz) {
              supplementRating.blitz_rating = v[i];
              trim(supplementRating.blitz_rating);
            } else {
              supplementRating.quick_rating = v[i];
              trim(supplementRating.quick_rating);
            }
          }
          if (i == 6) {
            supplementRating.blitz_rating = v[i];
          }
          if (i == 7) {
            supplementRating.online_regular_rating = v[i];
          }
          if (i == 8) {
            supplementRating.online_quick_rating = v[i];
          }
          if (i == 9) {
            supplementRating.online_blitz_rating = v[i];
          }
        }
        MemberSupplementRating memberSupplementRating;
        using Writer = nop::StreamWriter<std::stringstream>;
        nop::Serializer<Writer> serializer;
        const std::string key = memberId;
        std::string value;
        Status s = memberSupplementDb->Get(ReadOptions(), key, &value);
        if (!s.ok()) {
          memberSupplementRating.supplement_ratings[date] = supplementRating;
        } else {
          Deserializer<StreamReader<std::stringstream>> deserializer{value};
          deserializer.Read(&memberSupplementRating);

          const auto &it = memberSupplementRating.supplement_ratings.find(date);
          if (it != memberSupplementRating.supplement_ratings.end()) {
            auto &suppRating = it->second;
            // cout << "** member id = " << key << " existing quick = " <<
            // suppRating.quick_rating << " blitz = " << suppRating.blitz_rating
            // << endl;
            if (isQuick) {
              suppRating.quick_rating = supplementRating.quick_rating;
            }
            if (isBlitz) {
              suppRating.blitz_rating = supplementRating.blitz_rating;
            }
            // cout << " new  quick = " << suppRating.quick_rating << " blitz =
            // " << suppRating.blitz_rating << endl;
            memberSupplementRating.supplement_ratings[date] = suppRating;

          } else {
            memberSupplementRating.supplement_ratings[date] = supplementRating;
          }
        }
        serializer.Write(memberSupplementRating);
        const std::string data = serializer.writer().stream().str();
        Status status = memberSupplementDb->Put(WriteOptions(), key, data);
        assert(status.ok());
      }
    }
  }

  memberSupplementDb->Close();
}
