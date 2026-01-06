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

// const char kMembersDBPath[] =
// "/mnt/disks/data2/elotracker/data/uscf_members";
const char kEventsDBPath[] = "/mnt/disks/data2/elotracker/data/uscf_events";
const char kMemberNewRatingDBPath[] =
    "/mnt/disks/data2/elotracker/data/uscf_member_new_rating";
const char kMemberWinLossDBPath[] =
    "/mnt/disks/data2/elotracker/data/uscf_member_win_loss";
// DB *membersDb;
DB *eventsDb, *memberNewRatingDb, *memberWinLossDb;

string finalDate;

int main(int argc, char* argv[]) {
  Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  setenv("TZ", "PST8PDT", 1);
  std::time_t t = std::time(0);  // get time now
  std::tm* now = std::localtime(&t);
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
  // Status s = DB::Open(options, kMembersDBPath, &membersDb);
  // assert(s.ok());
  Status s = DB::Open(options, kEventsDBPath, &eventsDb);
  assert(s.ok());
  s = DB::Open(options, kMemberNewRatingDBPath, &memberNewRatingDb);
  assert(s.ok());
  s = DB::Open(options, kMemberWinLossDBPath, &memberWinLossDb);
  assert(s.ok());

  std::ifstream infile("uscf_crawl/uscf.jsonlines");
  std::string line;
  int linecount = 0;
  while (std::getline(infile, line)) {
    // replace null
    boost::replace_all(line, ": null", ": \"\"");
    json info = json::parse(line, nullptr, false);
    // cerr << " line = " << line << endl;
    linecount++;
    if (linecount % 100000 == 0) {
      cerr << "line count = " << linecount << endl;
    }

    bool isGenderFound = false;
    bool isResultFound = false;
    bool isEventDateFound = false;
    bool isNewRatingFound = false;

    Member member;
    MemberWinLoss memberWinLoss;
    MemberNewRating memberNewRating;
    EventRating eventRating;
    WinLoss winloss;
    Event event;

    for (const auto& mapItem : info.items()) {
      try {
        string key = std::string(mapItem.key());
        string value = std::string(mapItem.key());
      } catch (...) {
        cerr << " Got bad line " << line;
      }

      string key = std::string(mapItem.key());
      string event_id, event_name, member_id, state, opponent_id, section,
          num_rounds;
      string gender;
      string event_location, event_date;
      string result;

      if (key == "event_id") {
        event_id = mapItem.value().get<std::string>();
        eventRating.event_id = event_id;
      }
      if (key == "event_name") {
        event.name = mapItem.value().get<std::string>();
      }
      if (key == "gender") {
        gender = mapItem.value().get<std::string>();
        isGenderFound = true;
        member.gender = gender;
      }
      if (key == "result") {
        result = mapItem.value().get<std::string>();
        isResultFound = true;
        winloss.result = result;
      }
      if (key == "event_location") {
        event_location = mapItem.value().get<std::string>();
      }
      if (key == "event_date") {
        event_date = mapItem.value().get<std::string>();
        isEventDateFound = true;
      }
      if (key == "name") {
        member.name = mapItem.value().get<std::string>();
      }
      if (key == "member" || key == "id") {
        member_id = mapItem.value().get<std::string>();
        memberNewRating.member_id = member_id;
        memberWinLoss.member_id = member_id;
        member.id = member_id;
      }
      if (key == "section") {
        section = mapItem.value().get<std::string>();
      }
      if (key == "num_rounds") {
        num_rounds = mapItem.value().get<std::string>();
      }
      if (key == "state") {
        state = mapItem.value().get<std::string>();
        member.state = state;
      }
      if (key == "regular_rating") {
        member.regular_rating = mapItem.value().get<std::string>();
      }
      if (key == "quick_rating") {
        member.quick_rating = mapItem.value().get<std::string>();
      }
      if (key == "blitz_rating") {
        member.blitz_rating = mapItem.value().get<std::string>();
      }
      if (key == "online_regular_rating") {
        member.online_regular_rating = mapItem.value().get<std::string>();
      }
      if (key == "online_quick_rating") {
        member.online_quick_rating = mapItem.value().get<std::string>();
      }
      if (key == "online_blitz_rating") {
        member.online_blitz_rating = mapItem.value().get<std::string>();
      }
      if (key == "old_rating") {
        eventRating.old_rating = mapItem.value().get<std::string>();
      }
      if (key == "new_rating") {
        eventRating.new_rating = mapItem.value().get<std::string>();
        isNewRatingFound = true;
      }
      if (key == "opponent_id") {
        opponent_id = mapItem.value().get<std::string>();
        winloss.opponent_id = opponent_id;
      }
    }

    using Writer = nop::StreamWriter<std::stringstream>;
    nop::Serializer<Writer> serializer;

    /*
    if (isGenderFound) {
      #if 0
      serializer.Write(member);
      const std::string key = member.id;
      const std::string data = serializer.writer().stream().str();
      Status status = membersDb->Put(WriteOptions(),key, data);
      assert(status.ok());
      #endif
    }
    */
    if (isEventDateFound) {
      serializer.Write(event);
      const std::string key = event.id;
      const std::string data = serializer.writer().stream().str();
      Status status = eventsDb->Put(WriteOptions(), key, data);
      assert(status.ok());
    }

    if (isResultFound) {
      const std::string key = member.id + "-" + event.id;
      std::string value;
      Status s = memberNewRatingDb->Get(ReadOptions(), key, &value);
      if (!s.ok()) {
        memberWinLoss.winlossList.push_back(winloss);
      } else {
        Deserializer<StreamReader<std::stringstream>> deserializer{value};
        deserializer.Read(&memberWinLoss);
        memberWinLoss.winlossList.push_back(winloss);
      }
      serializer.Write(memberWinLoss);
      const std::string data = serializer.writer().stream().str();
      Status status = memberWinLossDb->Put(WriteOptions(), key, data);
      assert(status.ok());
    }

    if (isNewRatingFound) {
      const std::string key = memberNewRating.member_id;
      std::string value;
      Status s = memberNewRatingDb->Get(ReadOptions(), key, &value);
      if (!s.ok()) {
        memberNewRating.event_ratings.push_back(eventRating);
      } else {
        Deserializer<StreamReader<std::stringstream>> deserializer{value};
        deserializer.Read(&memberNewRating);
        memberNewRating.event_ratings.push_back(eventRating);
      }
      serializer.Write(memberNewRating);
      const std::string data = serializer.writer().stream().str();
      Status status = memberNewRatingDb->Put(WriteOptions(), key, data);
      assert(status.ok());
    }
  }

  memberWinLossDb->Close();
  memberNewRatingDb->Close();
  // membersDb->Close();
  eventsDb->Close();
}
