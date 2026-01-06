#include "ScoresheetCtrl.h"

void ScoresheetCtrl::asyncHandleHttpRequest(
    const HttpRequestPtr &req,
    std::function<void(const HttpResponsePtr &)> &&callback) {
  HttpViewData data;

  std::string title = "Chess Scoresheet OCR - EloTracker";
  std::string description =
      "Upload a chess scoresheet image to digitize your game into PGN format "
      "using AI-powered OCR";

  data.insert("title", title);
  data.insert("description", description);

  auto resp = HttpResponse::newHttpViewResponse("ScoresheetView.csp", data);
  callback(resp);
}
