#pragma once

#include <zmq.hpp>

namespace zrpc {
enum class EventType {
    Quit,
    Ready,

    Connect,
    Disconnect,
    SendRequest,
    ProcessReply,
    Bind,
    Unbind,
    ProcessRequest,
    SendReply,

    AddPub,
    RemovePub,
    AddSub,
    RemoveSub,
    PubTopic,
    SubTopic
};

struct Event
{
    explicit Event(EventType atype) : type(atype) {}
    virtual ~Event() = default;

    const EventType type;
};

template <EventType cmdType>
struct EventTemplate : public Event
{
    EventTemplate() : Event(cmdType) {}
};

enum class RpcRequestStatus
{
    Done = 0,
    DeadlineExceeded = 1,
};

using ClientFunc = std::function<void(RpcRequestStatus, zmq::message_t&)>;
using ServerFunc = std::function<void(zmq::message_t&, zmq::message_t&)>;
using SubFunc = std::function<void(zmq::message_t&)>;

using QuitEvent = EventTemplate<EventType::Quit>;
using ReadyEvent = EventTemplate<EventType::Ready>;

struct ConnectEvent : public EventTemplate<EventType::Connect>
{
    std::string serverAddr;
};

struct DisconnectEvent : public EventTemplate<EventType::Disconnect>
{
    uint64_t socketId;
};

struct SendRequestEvent : public EventTemplate<EventType::SendRequest>
{
    uint64_t clientSocketId;
    int64_t timeoutMs{-1};
    ClientFunc clientFunc;
    zmq::message_t requestMsg;
};

struct ProcessReplyEvent : public EventTemplate<EventType::ProcessReply>
{
    RpcRequestStatus status;
    ClientFunc clientFunc;
    zmq::message_t replyMsg;
};

struct BindEvent : public EventTemplate<EventType::Bind>
{
    std::string serverAddr;
    ServerFunc serverFunc;
};

struct UnbindEvent : public EventTemplate<EventType::Unbind>
{
    uint64_t socketId;
};

struct ProcessRequestEvent : public EventTemplate<EventType::ProcessRequest>
{
    uint64_t requestId;
    uint64_t serverSocketId;
    ServerFunc serverFunc;
    std::string routerId;
    zmq::message_t requestMsg;
};

struct SendReplyEvent : public EventTemplate<EventType::SendReply>
{
    uint64_t requestId;
    uint64_t serverSocketId;
    std::string routerId;
    zmq::message_t replyMsg;
};

struct AddPubEvent : public EventTemplate<EventType::AddPub>
{
    std::string serverAddr;
};

struct RemovePubEvent : public EventTemplate<EventType::RemovePub>
{
    uint64_t socketId;
};

struct AddSubEvent : public EventTemplate<EventType::AddSub>
{
    std::string serverAddr;
    SubFunc subFunc;
};

struct RemoveSubEvent : public EventTemplate<EventType::RemoveSub>
{
    uint64_t socketId;
};

struct PubTopicEvent : public EventTemplate<EventType::PubTopic>
{
    uint64_t socketId;
    zmq::message_t topicMsg;
};

struct SubTopicEvent : public EventTemplate<EventType::SubTopic>
{
    SubFunc subFunc;
    zmq::message_t topicMsg;
};
}
