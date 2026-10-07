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

/**
 * @brief Best-effort topic publisher.
 *
 * Slow or late subscribers may miss messages. Control-plane commands belong on
 * RPC. The PUB socket is not registered with the poller.
 */
class ZRPC_EXPORT Publisher final
{
public:
    Publisher(const std::shared_ptr<Context> &ctx);
    ~Publisher();

    void bind(const std::string &addr);
    void close();

    /**
     * Publish @p data on @p topic. Thread-safe after @ref bind.
     *
     * @throws std::runtime_error if not bound. Send failures are dropped (no error return).
     */
    void pubTopic(const std::string &topic, Payload &&data);

private:
    std::unique_ptr<PublisherPrivate> _d;
};

class SubscriberPrivate;

/**
 * @brief Topic subscriber.
 *
 * The destructor runs @ref disconnect then @ref wait. Callbacks must not throw,
 * and must not call @ref wait or destroy this subscriber.
 */
class ZRPC_EXPORT Subscriber final
{
public:
    /**
     * @param topics Non-empty names are passed to @c ZMQ_SUBSCRIBE on @ref connect.
     *               Delivery still matches the topic string exactly (ZMQ is prefix-based).
     */
    Subscriber(const std::vector<std::string> &topics, const std::shared_ptr<Context> &ctx);
    ~Subscriber();

    using TopicCallback = std::function<void(std::string_view topic, PayloadView payload)>;

    /**
     * Install the delivery callback. Call only before @ref connect.
     *
     * Changing it afterwards, or concurrently with a worker, is a data race.
     */
    void setCallback(const TopicCallback &cb);

    void connect(const std::string &addr);
    void disconnect();
    void wait();

private:
    std::unique_ptr<SubscriberPrivate> _d;
};
}
