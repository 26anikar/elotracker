#include <drogon/drogon.h>

#include <string>

int main(int argc, char *argv[]) {
  // Set HTTP listener address and port
  int portNum = 8081;
  std::string ipaddress = "0.0.0.0";
  if (argc > 1) {
    portNum = atoi(argv[1]);
  }
  std::cout << " port is " << portNum << std::endl;
  drogon::app().addListener(ipaddress.c_str(), portNum);
  drogon::app().setLogPath("/mnt/disks/data2/elotracker/logs/");
  drogon::app().setLogLevel(trantor::Logger::kWarn);
  // Load config file
  drogon::app().loadConfigFile("/mnt/disks/data2/elotracker/config.json");
  // drogon::app().loadConfigFile("../config.yaml");
  // Run HTTP framework,the method will block in the internal event loop
  drogon::app().run();
  return 0;
}
