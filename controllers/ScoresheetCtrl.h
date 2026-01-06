#pragma once

#include <drogon/HttpSimpleController.h>

using namespace drogon;

class ScoresheetCtrl : public drogon::HttpSimpleController<ScoresheetCtrl>
{
  public:
    void asyncHandleHttpRequest(const HttpRequestPtr& req, std::function<void (const HttpResponsePtr &)> &&callback) override;
    PATH_LIST_BEGIN
    PATH_ADD("/scoresheet", Get);
    PATH_LIST_END
};
