#include <algorithm>
#include <iostream>
#include <mutex>

#include "utils.h"
#include "Message.h"
#include "Event.h"
#include "ContextPrivate.h"
#include "Context.h"
#include "PubSub.h"

namespace zrpc {
class PublisherPrivate
{
public:
    PublisherPrivate(const std::shared_ptr<Context> &actx)
        : ctx(actx), dealer(detail::ContextAccess::get(actx)->createFrontendSocket())
    {
    }
    ~PublisherPrivate()
    {
        if (socketId > 0)
            detail::ContextAccess::get(ctx)->removePub(socketId);

        if (dealer)
            dealer->close();
    }

    void pubTopic(const std::string &topic, Payload &&data)
    {
        auto *event = new PubTopicEvent;
        event->socketId = socketId;
        event->message.header = takeMessage(std::string(topic));
        event->message.parts = takePayload(std::move(data));

        std::lock_guard<std::mutex> locker(_mtxForDealer);
        DealerWriter(*dealer).writePtr(event, 0);
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
    _d->socketId = detail::ContextAccess::get(_d->ctx)->addPub(addr);
}

void Publisher::pubTopic(const std::string &topic, Payload &&data)
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
            detail::ContextAccess::get(ctx)->removeSub(socketId);
    }

    void topicCallback(RpcMessage &msg)
    {
        if (!msg.valid) {
            std::cout << "Invalid topic message." << std::endl;
            return;
        }

        const auto topic = viewMessage(msg.header);
        const auto iter = std::find(topics.cbegin(), topics.cend(), topic);
        if (iter != topics.cend() && topicCb) {
            topicCb(topic, viewPayload(msg));
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
    const auto socketId = detail::ContextAccess::get(_d->ctx)->addSub(addr, [this](RpcMessage &topicMsg){
        _d->topicCallback(topicMsg);
    });
    _d->socketId = socketId;
}
}
