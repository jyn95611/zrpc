#pragma once

namespace zrpc {
class ContextPrivate;
class Context final
{
public:
    Context(int ioThrNum = 4, int workerThrNum = 8);
    ~Context();

    void quit();
    void wait();

private:
    friend class Server;
    friend class ServerPrivate;
    friend class Channel;
    friend class ChannelPrivate;
    friend class Stub;
    friend class StubPrivate;
    friend class Publisher;
    friend class PublisherPrivate;
    friend class Subscriber;
    friend class SubscriberPrivate;

private:
    ContextPrivate *d() { return _d; }

private:
    ContextPrivate *_d = nullptr;
};
}
