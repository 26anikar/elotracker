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
    "/mnt/disks/data2/elotracker/data/fide_member_supplement";
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

  string filename_match = "";
  if (argc >= 2) {
    filename_match = argv[1];
    cout << " filenamematch is " << filename_match << endl;
  }

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
       dir("./fide_supplements");
       dir != end; ++dir) {
    if (!boost::filesystem::is_directory(dir->path())) {
      string filename = dir->path().filename().string();
      string path = dir->path().string();
      // cout << " filename is " << filename << " path is  " << path << endl;
      if (filename == "readme" || filename == "readme.txt" ||
          filename == "TARATSUP.TXT") {
        continue;
      }
      int pos = filename.find(".txt");
      if (pos == -1) {
        continue;
      }

      if (!filename_match.empty()) {
        if (filename.find(filename_match) == -1) {
          cout << " filename is " << filename << endl;
          continue;
        } else {
          cout << " got filename = " << filename << endl;
          // exit(0);
        }
      } else {
        // exit(0);
      }

      std::ifstream infile(path);
      std::string line;
      vector<string> v;
      bool isStandard = true;
      bool isRapid = false;
      bool isBlitz = false;
      if (filename.find("standard") != -1) {
        isStandard = true;
      }
      if (filename.find("rapid") != -1) {
        isStandard = false;
        isRapid = true;
      }
      if (filename.find("blitz") != -1) {
        isStandard = false;
        isBlitz = true;
      }

      string date = filename;
      if (date.find("rapid_") != -1) {
        date.replace(0, 6, "");
      }
      if (date.find("blitz_") != -1) {
        date.replace(0, 6, "");
      }
      if (date.find("standard_") != -1) {
        date.replace(0, 9, "");
      }

      int x = date.find("frl.txt");
      if (x != -1) {
        date.replace(x, 7, "");
      }
      x = date.find("FRL.TXT");
      if (x != -1) {
        date.replace(x, 7, "");
      }
      transform(date.begin(), date.end(), date.begin(), ::tolower);
      string origDate = date;

      map<string, string> mon = {{"jan", "01"}, {"feb", "02"}, {"mar", "03"},
                                 {"apr", "04"}, {"may", "05"}, {"jun", "06"},
                                 {"jul", "07"}, {"aug", "08"}, {"sep", "09"},
                                 {"oct", "10"}, {"nov", "11"}, {"dec", "12"}};

      for (auto x : mon) {
        string month_name = x.first;
        string month_num = x.second;

        int idx = date.find(month_name);
        if (idx == -1) {
          continue;
        }
        string year = date.substr(idx + 3);
        date = "20" + year + month_num;
      }
      cout << " newdate is " << date << endl;

      int linecount = 0;
      map<string, int> columnStart;
      map<string, int> columnEnd;
      bool isBadLine = false;
      while (std::getline(infile, line)) {
        if (linecount == 0) {
          int pos = line.find("ID Number");
          if (pos != -1) {
            line.replace(pos, 9, "ID_Number");
          }
          pos = line.find("ID number");
          if (pos != -1) {
            line.replace(pos, 9, "ID_number");
          }
          pos = line.find("id number");
          if (pos != -1) {
            line.replace(pos, 9, "id_number");
          }
          pos = line.find("ID NUMBER");
          if (pos != -1) {
            line.replace(pos, 9, "id_number");
          }
          transform(line.begin(), line.end(), line.begin(), ::tolower);
          boost::split(v, line, boost::is_any_of(" "),
                       boost::token_compress_on);
          string prevCol = "";
          for (int i = 0; i < v.size(); i++) {
            string k = v[i];

            int index = line.find(k);
            columnStart[k] = index;
            if (i > 0) {
              columnEnd[prevCol] = index - 1;
            }
            cout << " Got col  = " << k << " columnstart = " << index << endl;
            prevCol = k;
          }
          // last column
          columnEnd[prevCol] = -1;
          linecount++;
          continue;
        }
        linecount++;

        string memberId = "";
        string rating = "";
        string birthday = "";
        string name = "";
        string numgames = "";
        string federation = "";

        // cout <<  " Got line = " << line << endl;
        isBadLine = false;

        for (auto &k : columnStart) {
          string key = k.first;
          trim(key);
          int colStart = k.second;
          int colEnd = columnEnd[key];
          // cout << " got colstart = " << colStart << " col end = " << colEnd
          // << " for key = " << key << endl;
          if (colStart > line.length()) {
            cout << " bad line " << line << " with colstart = " << colStart
                 << " at line num " << linecount << " key = " << key << endl;
            // isBadLine = true;
            continue;
          }
          string val;
          if (colEnd != -1) {
            val = line.substr(colStart, colEnd - colStart + 1);
          } else {
            val = line.substr(colStart);
          }
          trim(val);
          // this is id number
          if (key.find("number") != -1) {
            memberId = val;
            // cout << " member id is " << memberId << endl;
          }
          if (key.find("name") != -1) {
            name = val;
          }
          string lowerKey = key;
          transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(),
                    ::tolower);

          if (lowerKey == origDate) {
            rating = val;
          }
          if (key.find("birth") != -1 || key.find("b-day") != -1 ||
              key.find("bday") != -1) {
            birthday = val;
          }
          if (key.find("fed") != -1) {
            federation = val;
          }
          if (key.find("gms") != -1) {
            numgames = val;
          }
        }
        if (isBadLine) {
          continue;
        }

        SupplementRating supplementRating;
        if (isStandard) {
          supplementRating.regular_rating = rating;
        } else if (isRapid) {
          supplementRating.quick_rating = rating;
        } else if (isBlitz) {
          supplementRating.blitz_rating = rating;
        } else {
          cerr << " This can't happen " << endl;
          assert(0);
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

          /* temporary change to remove some bad keys
          // dont increment in the for
          for (auto it = memberSupplementRating.supplement_ratings.begin(); it
          != memberSupplementRating.supplement_ratings.end() ; ) { string date =
          it->first; if (date.find("FRL") != -1) {
              memberSupplementRating.supplement_ratings.erase(it++);
            } else {
              it++;
            }
          }
          */

          const auto &it = memberSupplementRating.supplement_ratings.find(date);
          if (it != memberSupplementRating.supplement_ratings.end()) {
            auto &suppRating = it->second;
            if (isStandard) {
              suppRating.regular_rating = rating;
            }
            if (isRapid) {
              suppRating.quick_rating = rating;
            }
            if (isBlitz) {
              suppRating.blitz_rating = rating;
            }
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
