#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <spdlog/spdlog.h>

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
        close();
        if (dealer)
            dealer->close();
    }

    void bind(const std::string &addr)
    {
        if (socketId > 0)
            throw std::runtime_error("already bound");

        auto *event = new AddPubEvent;
        event->serverAddr = addr;
        DealerSender(*dealer).sendPtr(event, 0);
        socketId = recvSocketEventAck(*dealer, "bind");
        spdlog::info("publisher bound, addr={} socketId={}", addr, socketId);
    }

    void close()
    {
        if (socketId == 0)
            return;

        auto *event = new RemovePubEvent;
        event->socketId = socketId;
        DealerSender(*dealer).sendPtr(event, 0);
        recvEventAck(*dealer);
        socketId = 0;
    }

    void pubTopic(const std::string &topic, Payload &&data)
    {
        if (socketId == 0)
            throw std::runtime_error("not bound");

        auto *event = new SendTopicEvent;
        event->socketId = socketId;
        event->message.header = takeMessage(std::string(topic));
        event->message.parts = takePayload(std::move(data));

        std::lock_guard<std::mutex> locker(_dealerMtx);
        DealerSender(*dealer).sendPtr(event, 0);
    }

    std::shared_ptr<Context> ctx;
    uint64_t socketId{0};
    std::unique_ptr<zmq::socket_t> dealer;
    std::mutex _dealerMtx;
};

Publisher::Publisher(const std::shared_ptr<Context> &ctx)
    : _d(std::make_unique<PublisherPrivate>(ctx))
{
}

Publisher::~Publisher() = default;

void Publisher::bind(const std::string &addr)
{
    _d->bind(addr);
}

void Publisher::close()
{
    _d->close();
}

void Publisher::pubTopic(const std::string &topic, Payload &&data)
{
    _d->pubTopic(topic, std::move(data));
}

class SubscriberPrivate
{
public:
    SubscriberPrivate(const std::vector<std::string> &atopics, const std::shared_ptr<Context> &actx)
        : ctx(actx)
        , dealer(detail::ContextAccess::get(actx)->createFrontendSocket())
        , topics(atopics)
    {
    }
    ~SubscriberPrivate()
    {
        disconnect();
        wait();
        if (dealer)
            dealer->close();
    }

    void connect(const std::string &addr)
    {
        if (connected)
            throw std::runtime_error("already connected");

        wait();

        auto *event = new AddSubEvent;
        event->serverAddr = addr;
        event->topics = topics;
        event->subFunc = [this](RpcMessage &topicMsg) {
            topicCallback(topicMsg);
        };
        DealerSender(*dealer).sendPtr(event, 0);
        socketId = recvSocketEventAck(*dealer, "connect");
        connected = true;
        pendingWait = false;
        spdlog::info("subscriber connected, addr={} socketId={} topics={}",
                     addr, socketId, topics.size());
    }

    void disconnect()
    {
        if (!connected)
            return;

        auto *event = new RemoveSubEvent;
        event->socketId = socketId;
        DealerSender(*dealer).sendPtr(event, 0);
        recvEventAck(*dealer);
        connected = false;
        pendingWait = true;
    }

    void wait()
    {
        if (connected)
            disconnect();
        if (!pendingWait)
            return;

        recvEventAck(*dealer);
        pendingWait = false;
    }

    void topicCallback(RpcMessage &msg)
    {
        if (!msg.valid) {
            spdlog::warn("invalid topic message");
            return;
        }

        const auto topic = viewMessage(msg.header);
        /// ZMQ matches prefixes; exact equality avoids "ab" matching "abc".
        const auto iter = std::find(topics.cbegin(), topics.cend(), topic);
        if (iter != topics.cend() && topicCb) {
            topicCb(topic, viewPayload(msg));
        }
    }

    std::shared_ptr<Context> ctx;
    std::unique_ptr<zmq::socket_t> dealer;
    std::vector<std::string> topics;
    Subscriber::TopicCallback topicCb;
    uint64_t socketId{0};
    bool connected{false};
    bool pendingWait{false};
};

Subscriber::Subscriber(const std::vector<std::string> &topics, const std::shared_ptr<Context> &ctx)
    : _d(std::make_unique<SubscriberPrivate>(topics, ctx))
{
}

Subscriber::~Subscriber() = default;

void Subscriber::setCallback(const TopicCallback &cb)
{
    _d->topicCb = cb;
}

void Subscriber::connect(const std::string &addr)
{
    _d->connect(addr);
}

void Subscriber::disconnect()
{
    _d->disconnect();
}

void Subscriber::wait()
{
    _d->wait();
}
}
