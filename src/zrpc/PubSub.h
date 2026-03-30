#ifndef PUBSUB_H
#define PUBSUB_H

#include <memory>
#include <vector>
#include <functional>

#include "Context.h"

namespace zrpc {
class PublisherPrivate;
class Publisher final
{
public:
    Publisher(const std::shared_ptr<Context> &ctx);
    ~Publisher();

    void bind(const std::string &addr);
    void pubTopic(const std::string &topic, std::string &&data);

private:
    PublisherPrivate *_d{};
};

class SubscriberPrivate;
class Subscriber final
{
public:
    Subscriber(const std::vector<std::string> &topics, const std::shared_ptr<Context> &ctx);
    ~Subscriber();

    using TopicCallback = std::function<void(const std::string&, const std::string&)>;
    void setCallback(const TopicCallback &cb);
    void connect(const std::string &addr);

private:
    SubscriberPrivate *_d{};
};
}

#endif // PUBSUB_H
