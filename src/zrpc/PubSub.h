#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "Call.h"
#include "Context.h"
#include "zrpc_global.h"

namespace zrpc {
class PublisherPrivate;
class ZRPC_EXPORT Publisher final
{
public:
    Publisher(const std::shared_ptr<Context> &ctx);
    ~Publisher();

    void bind(const std::string &addr);
    void pubTopic(const std::string &topic, Payload &&data);

private:
    PublisherPrivate *_d{};
};

class SubscriberPrivate;
class ZRPC_EXPORT Subscriber final
{
public:
    Subscriber(const std::vector<std::string> &topics, const std::shared_ptr<Context> &ctx);
    ~Subscriber();

    using TopicCallback = std::function<void(std::string_view topic, PayloadView payload)>;
    void setCallback(const TopicCallback &cb);
    void connect(const std::string &addr);

private:
    SubscriberPrivate *_d{};
};
}
