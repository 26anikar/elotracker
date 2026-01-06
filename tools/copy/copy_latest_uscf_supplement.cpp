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
const char kMemberPath[] = "/mnt/disks/data2/elotracker/data/uscf_members";
DB *memberSupplementDb;
DB *memberDb;

std::string capitalize(const std::string &str) {
  if (str.empty()) return str;

  std::string result = str;
  result[0] = std::toupper(result[0]);
  for (size_t i = 1; i < result.length(); ++i) {
    // if the prev char was a '-' we don't lower case this one
    if (result[i - 1] == '-') {
      continue;
    } else {
      result[i] = std::tolower(result[i]);
    }
  }
  return result;
}

std::string capitalizeMultiWords(const std::string &str) {
  vector<string> v;
  boost::split(v, str, boost::is_any_of(" "));
  string n;
  for (int i = 0; i < v.size(); i++) {
    if (i > 0) {
      n += " ";
    }
    n += capitalize(v[i]);
  }
  return n;
}

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
  s = DB::Open(options, kMemberPath, &memberDb);
  assert(s.ok());

  for (boost::filesystem::recursive_directory_iterator end,
       dir("./golden_uscf_supplements");
       dir != end; ++dir) {
    if (!boost::filesystem::is_directory(dir->path())) {
      string filename = dir->path().filename().string();
      string path = dir->path().string();
      if (path.find("2412") == -1) {
        continue;
      }
      if (path.find("2501") == -1) {
        continue;
      }
      if (path.find("2502") == -1) {
        continue;
      }
      if (path.find("2503") == -1) {
        continue;
      }
      if (path.find("2504") == -1) {
        continue;
      }
      if (path.find("2505") == -1) {
        continue;
      }
      if (path.find("2506") == -1) {
        continue;
      }
      if (path.find("2507") == -1) {
        continue;
      }
      if (path.find("2508") == -1) {
        continue;
      }
      if (path.find("2509") == -1) {
        continue;
      }
      cout << " filename is " << filename << " path is  " << path << endl;
      if (filename != "rtglist.txt") {
        continue;
      }

      std::ifstream infile(path);
      std::string line;
      int linecount = 0;
      vector<string> v;

      string date = filename;
      if (filename == "rtglist.txt") {
        date = path;
        int pos = date.find("ALLRTG");
        if (pos == -1) {
          cerr << "Incorrect filename \n";
          exit(1);
        }
        date.replace(0, pos + 6, "");
        pos = date.find("/");
        if (pos != -1) {
          date.replace(pos, date.length() - pos + 1, "");
        }
      }
      date = "20" + date;
      cout << " date is " << date << endl;
      string derivedRatingSuppDate =
          date.substr(0, 4) + "-" + date.substr(4, 2) + "-01";
      // cout << " derived rating supp date is " << derivedRatingSuppDate <<
      // endl;

      while (std::getline(infile, line)) {
        SupplementRating supplementRating;
        if (line.find("rtglistdate") != -1 || line.find("rtgreg") != -1) {
          cerr << " skipping line " << line << endl;
          continue;
        }
        boost::split(v, line, boost::is_any_of("\t"));
        string memberId = "";
        string regular_rating = "";
        string quick_rating = "";
        string blitz_rating = "";
        string online_regular_rating = "";
        string online_quick_rating = "";
        string online_blitz_rating = "";
        string rating_supp_date = "";
        string name = "";
        string state = "";
        string active_flag = "";
        string country = "";
        string fide_id = "";

        int countCols = v.size();
        bool newFormat = false;
        if (countCols >= 19) {
          newFormat = true;
        }
        if (filename.find("rtglist") != -1) {
          /*
    for (int i = 0; i < v.size(); i++) {
      cout << " i is " << i << " val is " <<  v[i] << endl;
    }
    */

          for (int i = 0; i < v.size(); i++) {
            // cout << v[i] << endl;
            if (i == 0) {
              memberId = v[i];
            }
            if (i == 1) {
              name = v[i];
            }
            if (i == 2) {
              state = v[i];
            }
            if (i == 3) {
              country = v[i];
            }
            if (i == 6) {
              fide_id = v[i];
            }
            // this is the actual FIDE federation
            if (i == 7) {
              if (v[i] != "") {
                country = v[i];
              }
            }
            if (i == 5) {
              active_flag = v[i];
            }
            if (i == 8) {
              // this is the rating supplementdate for the member - if this is
              // different from the current supplement date it means this was
              // the last rating supplement date available for that person
              string derivedDate = v[i];
              // cout << " derived date is " << derivedDate << endl;
              // some people dont have date if they haven't ever played
              if (derivedDate != "") {
                date = derivedDate.substr(0, 4) + derivedDate.substr(5, 2);
                // cout << " Date is changed to " << date << endl;
              } else {
                // empty date - we will skip writing to supplement
                date = "";
              }
            }
            if (newFormat == true) {
              if (i == 9) {
                supplementRating.regular_rating = v[i];
                regular_rating = v[i];
                // cout << " regular ating is " <<
                // supplementRating.regular_rating << endl;
              }
              if (i == 11) {
                supplementRating.quick_rating = v[i];
                quick_rating = v[i];
              }
              if (i == 13) {
                supplementRating.blitz_rating = v[i];
                blitz_rating = v[i];
              }
              if (i == 15) {
                supplementRating.online_regular_rating = v[i];
                online_regular_rating = v[i];
              }
              if (i == 17) {
                supplementRating.online_quick_rating = v[i];
                online_quick_rating = v[i];
              }
              if (i == 19) {
                supplementRating.online_blitz_rating = v[i];
                online_blitz_rating = v[i];
              }
            } else {
              if (i == 9) {
                supplementRating.regular_rating = v[i];
                regular_rating = v[i];
                // cout << " regular ating is " <<
                // supplementRating.regular_rating << endl;
              }
              if (i == 10) {
                supplementRating.quick_rating = v[i];
                quick_rating = v[i];
              }
              if (i == 11) {
                supplementRating.blitz_rating = v[i];
                blitz_rating = v[i];
              }
              if (i == 12) {
                supplementRating.online_regular_rating = v[i];
                online_regular_rating = v[i];
              }
              if (i == 13) {
                supplementRating.online_quick_rating = v[i];
                online_quick_rating = v[i];
              }
              if (i == 14) {
                supplementRating.online_blitz_rating = v[i];
                online_blitz_rating = v[i];
              }
            }
          }
        }
        const std::string key = memberId;
        std::string value;
        Member member;
        s = memberDb->Get(ReadOptions(), key, &value);
        if (s.ok()) {
          Deserializer<StreamReader<std::stringstream>> deserializer{value};
          deserializer.Read(&member);
        }
        int comma_pos = name.find(",");
        if (comma_pos != -1) {
          string lastName = name.substr(0, comma_pos);
          string firstName = name.substr(comma_pos + 1);
          trim(firstName);
          trim(lastName);
          name = firstName + " " + lastName;
          name = capitalizeMultiWords(name);
        }

        {
          member.name = name;
          member.id = memberId;
          member.supp_regular_rating = regular_rating;
          member.supp_quick_rating = quick_rating;
          member.supp_blitz_rating = blitz_rating;
          member.state = state;
          member.country = country;
          member.active_flag = active_flag;
          member.fide_id = fide_id;
          using Writer = nop::StreamWriter<std::stringstream>;
          nop::Serializer<Writer> serializer;
          serializer.Write(member);
          const std::string data2 = serializer.writer().stream().str();
          s = memberDb->Put(WriteOptions(), key, data2);
          assert(s.ok());
        }

        MemberSupplementRating memberSupplementRating;
        using Writer = nop::StreamWriter<std::stringstream>;
        nop::Serializer<Writer> serializer;
        s = memberSupplementDb->Get(ReadOptions(), key, &value);
        if (!s.ok()) {
          memberSupplementRating.supplement_ratings[date] = supplementRating;
        } else {
          Deserializer<StreamReader<std::stringstream>> deserializer{value};
          deserializer.Read(&memberSupplementRating);
          memberSupplementRating.supplement_ratings[date] = supplementRating;
        }
        // even for inactive put the latest supplement that they had
        if (date != "") {
          serializer.Write(memberSupplementRating);
          const std::string data = serializer.writer().stream().str();
          s = memberSupplementDb->Put(WriteOptions(), key, data);
          assert(s.ok());
        }
      }
    }
  }

  memberDb->Close();
  memberSupplementDb->Close();
}
