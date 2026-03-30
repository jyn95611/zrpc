#include <iostream>
#include <mutex>

#include "utils.h"
#include "Message.h"
#include "Context.h"
#include "ContextPrivate.h"
#include "PubSub.h"

namespace zrpc {
class PublisherPrivate
{
public:
    PublisherPrivate(const std::shared_ptr<Context> &actx)
        : ctx(actx), dealer(actx->d()->createFrontendSocket())
    {
    }
    ~PublisherPrivate()
    {
        if (socketId > 0)
            ctx->d()->removePub(socketId);

        if (dealer)
            dealer->close();
    }

    void pubTopic(const std::string &topic, std::string &&data)
    {
        RpcTopic rpcTopic;
        rpcTopic.topic = topic;
        rpcTopic.data = std::move(data);

        auto *event = new PubTopicEvent;
        event->socketId = socketId;
        event->topicMsg = std::move(rpcTopic.serialize());

        std::lock_guard<std::mutex> locker(_mtxForDealer);
        DealerWriter(*dealer).writePtr(event, zmq::send_flags::none);

    }

    std::shared_ptr<Context> ctx;
    uint64_t socketId{0};
    std::unique_ptr<zmq::socket_t> dealer;
    std::mutex _mtxForDealer;
};

Publisher::Publisher(const std::shared_ptr<Context> &ctx)
    : _d(new PublisherPrivate(ctx))
{
}

Publisher::~Publisher()
{
    delete _d;
}

void Publisher::bind(const std::string &addr)
{
    _d->socketId = _d->ctx->d()->addPub(addr);
}

void Publisher::pubTopic(const std::string &topic, std::string &&data)
{
    _d->pubTopic(topic, std::move(data));
}

class SubscriberPrivate
{
public:
    SubscriberPrivate(const std::vector<std::string> &atopics, const std::shared_ptr<Context> &actx)
        : topics(atopics), ctx(actx)
    {
    }
    ~SubscriberPrivate()
    {
        if (socketId > 0)
            ctx->d()->removeSub(socketId);
    }

    void topicCallback(zmq::message_t &topicMsg)
    {
        RpcTopic rpcTopic;
        if (!rpcTopic.deserialize(topicMsg)) {
            std::cout << "Invalid topic message." << std::endl;
            return;
        }

        const auto iter = std::find(topics.cbegin(), topics.cend(), rpcTopic.topic);
        if (iter != topics.cend() && topicCb) {
            topicCb(rpcTopic.topic, rpcTopic.data);
        }
    }

    std::shared_ptr<Context> ctx;
    uint64_t socketId{0};
    std::vector<std::string> topics;
    Subscriber::TopicCallback topicCb;
};

Subscriber::Subscriber(const std::vector<std::string> &topics, const std::shared_ptr<Context> &ctx)
    : _d(new SubscriberPrivate(topics, ctx))
{
}

Subscriber::~Subscriber()
{
    delete _d;
}

void Subscriber::setCallback(const TopicCallback &cb)
{
    _d->topicCb = cb;
}

void Subscriber::connect(const std::string &addr)
{
    const auto socketId = _d->ctx->d()->addSub(addr, [this](zmq::message_t &topicMsg){
        _d->topicCallback(topicMsg);
    });
    _d->socketId = socketId;
}
}
