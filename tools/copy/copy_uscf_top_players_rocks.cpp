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
#include <ctime>
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

const char kTopPlayersDBPath[] = "/mnt/disks/data2/elotracker/data/top_players";
DB *topPlayersDb;

string finalDate;

map<string, string> isoMap = {
    {"AFG", "AF"}, {"ALA", "AX"}, {"ALB", "AL"}, {"DZA", "DZ"}, {"ASM", "AS"},
    {"AND", "AD"}, {"AGO", "AO"}, {"AIA", "AI"}, {"ATA", "AQ"}, {"ATG", "AG"},
    {"ARG", "AR"}, {"ARM", "AM"}, {"ABW", "AW"}, {"AUS", "AU"}, {"AUT", "AT"},
    {"AZE", "AZ"}, {"BHS", "BS"}, {"BHR", "BH"}, {"BGD", "BD"}, {"BRB", "BB"},
    {"BLR", "BY"}, {"BEL", "BE"}, {"BLZ", "BZ"}, {"BEN", "BJ"}, {"BMU", "BM"},
    {"BTN", "BT"}, {"BOL", "BO"}, {"BES", "BQ"}, {"BIH", "BA"}, {"BWA", "BW"},
    {"BVT", "BV"}, {"BRA", "BR"}, {"VGB", "VG"}, {"IOT", "IO"}, {"BRN", "BN"},
    {"BGR", "BG"}, {"BUL", "BG"}, {"BFA", "BF"}, {"BDI", "BI"}, {"KHM", "KH"},
    {"CMR", "CM"}, {"CAN", "CA"}, {"CPV", "CV"}, {"CYM", "KY"}, {"CAF", "CF"},
    {"TCD", "TD"}, {"CHL", "CL"}, {"CHN", "CN"}, {"HKG", "HK"}, {"MAC", "MO"},
    {"CXR", "CX"}, {"CCK", "CC"}, {"COL", "CO"}, {"COM", "KM"}, {"COG", "CG"},
    {"COD", "CD"}, {"COK", "CK"}, {"CRI", "CR"}, {"CIV", "CI"}, {"HRV", "HR"},
    {"CUB", "CU"}, {"CUW", "CW"}, {"CYP", "CY"}, {"CZE", "CZ"}, {"DNK", "DK"},
    {"DEN", "DK"}, {"DJI", "DJ"}, {"DMA", "DM"}, {"DOM", "DO"}, {"ECU", "EC"},
    {"EGY", "EG"}, {"SLV", "SV"}, {"SLO", "SV"}, {"GNQ", "GQ"}, {"ERI", "ER"},
    {"EST", "EE"}, {"ETH", "ET"}, {"FLK", "FK"}, {"FRO", "FO"}, {"FJI", "FJ"},
    {"FIN", "FI"}, {"FRA", "FR"}, {"GUF", "GF"}, {"PYF", "PF"}, {"ATF", "TF"},
    {"GAB", "GA"}, {"GMB", "GM"}, {"GEO", "GE"}, {"DEU", "DE"}, {"GER", "DE"},
    {"GHA", "GH"}, {"GIB", "GI"}, {"GRC", "GR"}, {"GRE", "GR"}, {"GRL", "GL"},
    {"GRD", "GD"}, {"GLP", "GP"}, {"GUM", "GU"}, {"GTM", "GT"}, {"GGY", "GG"},
    {"GIN", "GN"}, {"GNB", "GW"}, {"GUY", "GY"}, {"HTI", "HT"}, {"HMD", "HM"},
    {"VAT", "VA"}, {"HND", "HN"}, {"HUN", "HU"}, {"ISL", "IS"}, {"IND", "IN"},
    {"IDN", "ID"}, {"INA", "ID"}, {"IRN", "IR"}, {"IRI", "IR"}, {"IRQ", "IQ"},
    {"IRL", "IE"}, {"IMN", "IM"}, {"ISR", "IL"}, {"ITA", "IT"}, {"JAM", "JM"},
    {"JPN", "JP"}, {"JEY", "JE"}, {"JOR", "JO"}, {"KAZ", "KZ"}, {"KEN", "KE"},
    {"KIR", "KI"}, {"PRK", "KP"}, {"KOR", "KR"}, {"KWT", "KW"}, {"KGZ", "KG"},
    {"LAO", "LA"}, {"LVA", "LV"}, {"LBN", "LB"}, {"LSO", "LS"}, {"LBR", "LR"},
    {"LBY", "LY"}, {"LIE", "LI"}, {"LTU", "LT"}, {"LUX", "LU"}, {"MKD", "MK"},
    {"MDG", "MG"}, {"MWI", "MW"}, {"MYS", "MY"}, {"MDV", "MV"}, {"MLI", "ML"},
    {"MLT", "MT"}, {"MHL", "MH"}, {"MTQ", "MQ"}, {"MRT", "MR"}, {"MUS", "MU"},
    {"MYT", "YT"}, {"MEX", "MX"}, {"FSM", "FM"}, {"FID", "FF"}, {"MDA", "MD"},
    {"MCO", "MC"}, {"MNG", "MN"}, {"MON", "MN"}, {"MGL", "MN"}, {"MNE", "ME"},
    {"MSR", "MS"}, {"MAR", "MA"}, {"MOZ", "MZ"}, {"MMR", "MM"}, {"NAM", "NA"},
    {"NRU", "NR"}, {"NPL", "NP"}, {"NLD", "NL"}, {"NED", "NL"}, {"ANT", "AN"},
    {"NCL", "NC"}, {"NZL", "NZ"}, {"NIC", "NI"}, {"NER", "NE"}, {"NGA", "NG"},
    {"NIU", "NU"}, {"NFK", "NF"}, {"MNP", "MP"}, {"NOR", "NO"}, {"OMN", "OM"},
    {"PAK", "PK"}, {"PLW", "PW"}, {"PSE", "PS"}, {"PAN", "PA"}, {"PNG", "PG"},
    {"PRY", "PY"}, {"PER", "PE"}, {"PHL", "PH"}, {"PCN", "PN"}, {"POL", "PL"},
    {"PRT", "PT"}, {"POR", "PT"}, {"PRI", "PR"}, {"QAT", "QA"}, {"REU", "RE"},
    {"ROU", "RO"}, {"RUS", "RU"}, {"RWA", "RW"}, {"BLM", "BL"}, {"SHN", "SH"},
    {"KNA", "KN"}, {"LCA", "LC"}, {"MAF", "MF"}, {"SPM", "PM"}, {"VCT", "VC"},
    {"WSM", "WS"}, {"SMR", "SM"}, {"STP", "ST"}, {"SAU", "SA"}, {"SEN", "SN"},
    {"SRB", "RS"}, {"SYC", "SC"}, {"SLE", "SL"}, {"SGP", "SG"}, {"SXM", "SX"},
    {"SVK", "SK"}, {"SVN", "SI"}, {"SLB", "SB"}, {"SOM", "SO"}, {"ZAF", "ZA"},
    {"SGS", "GS"}, {"SSD", "SS"}, {"ESP", "ES"}, {"LKA", "LK"}, {"SDN", "SD"},
    {"SUR", "SR"}, {"SJM", "SJ"}, {"SWZ", "SZ"}, {"SWE", "SE"}, {"CHE", "CH"},
    {"SUI", "CH"}, {"SYR", "SY"}, {"TWN", "TW"}, {"TJK", "TJ"}, {"TZA", "TZ"},
    {"THA", "TH"}, {"TLS", "TL"}, {"TGO", "TG"}, {"TKL", "TK"}, {"TON", "TO"},
    {"TTO", "TT"}, {"TUN", "TN"}, {"TUR", "TR"}, {"TKM", "TM"}, {"TCA", "TC"},
    {"TUV", "TV"}, {"UGA", "UG"}, {"UKR", "UA"}, {"ARE", "AE"}, {"UAE", "AE"},
    {"GBR", "GB"}, {"ENG", "GB"}, {"USA", "US"}, {"UMI", "UM"}, {"URY", "UY"},
    {"UZB", "UZ"}, {"VUT", "VU"}, {"VEN", "VE"}, {"VNM", "VN"}, {"VIE", "VN"},
    {"VIR", "VI"}, {"WLF", "WF"}, {"ESH", "EH"}, {"YEM", "YE"}, {"ZMB", "ZM"},
    {"ZWE", "ZW"}, {"XKX", "XK"},

};

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
  string is_fide = "0";
  if (argc > 1) {
    is_fide = argv[1];
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
  // finalDate = to_string(year+1900) + "-" + to_string(month+1) + "-" +
  // to_string(day);
  finalDate = ss.str();
  cerr << " final date is " << finalDate << endl;

  // open DB
  Status s = DB::Open(options, kTopPlayersDBPath, &topPlayersDb);
  assert(s.ok());

  string filename;
  std::ifstream infile;
  if (is_fide == "1") {
    filename = "fide_topplayers_crawl/fidetopplayers.jsonlines";
  } else {
    filename = "uscf_topplayers_crawl/uscftopplayers.jsonlines";
  }
  infile.open(filename, std::ifstream::in);
  std::string line;
  int linecount = 0;
  while (std::getline(infile, line)) {
    // replace null
    boost::replace_all(line, ": null", ": \"\"");
    json info = json::parse(line, nullptr, false);
    // cerr << " line = " << line << endl;
    linecount++;
    if (linecount % 10000 == 0) {
      cerr << "line count = " << linecount << endl;
    }

    string composedKey = "";
    string list = "";
    string list_age = "";
    string list_gender = "";
    string list_rating_type = "";
    string name = "", id = "", rating = "", time = "";
    string state = "", country = "";
    int rank = -1;
    TopPlayer topPlayer;

    for (const auto &mapItem : info.items()) {
      try {
        string key = std::string(mapItem.key());
        string value = mapItem.value().get<std::string>();
        trim(value);
        // cout << " key is " << key << " value is " << value << endl;
        if (key == "list") {
          list = value;
        }
        if (key == "time") {
          time = value;
        }
        if (key == "rank") {
          int pos = value.find('=');
          if (pos != -1) {
            value = value.substr(pos);
            trim(value);
          }
          rank = std::stoi(value);
        }
        if (key == "gender") {
          list_gender = value;
        }
        if (key == "id") {
          id = value;
        }
        if (key == "name") {
          name = value;
        }
        if (key == "rating") {
          rating = value;
        }
        if (key == "state") {
          // currently state contains both country and state depending
          // on FIDE or USCF
          state = value;
          country = value;
          const auto &kv = isoMap.find(country);
          if (kv != isoMap.end()) {
            country = kv->second;
            transform(country.begin(), country.end(), country.begin(),
                      ::tolower);
          }
        }
        if (key == "rating_type") {
          list_rating_type = value;
        }
        // if (key == "is_only_us") {
        // is_only_us = value;
        //}

      } catch (...) {
        cerr << " Got bad line " << line;
      }
    }
    topPlayer.name = name;
    topPlayer.id = id;
    topPlayer.rating = rating;
    topPlayer.state = state;
    topPlayer.country = country;

    std::string key;
    if (is_fide == "1") {
      key = time + "-fide-";
      key = key + list;
    } else {
      key = time + "-" + list;
    }
    // cout << " key is " << key << endl;
    string value;
    Status s = topPlayersDb->Get(ReadOptions(), key, &value);
    TopPlayers topPlayers;
    if (!s.ok()) {
      topPlayers.playerMap[rank] = topPlayer;
    } else {
      Deserializer<StreamReader<std::stringstream>> deserializer{value};
      deserializer.Read(&topPlayers);
      topPlayers.playerMap[rank] = topPlayer;
    }
    using Writer = nop::StreamWriter<std::stringstream>;
    nop::Serializer<Writer> serializer;
    serializer.Write(topPlayers);
    const std::string data = serializer.writer().stream().str();
    Status status = topPlayersDb->Put(WriteOptions(), key, data);
    assert(status.ok());
  }
  topPlayersDb->Close();
  infile.close();
}
