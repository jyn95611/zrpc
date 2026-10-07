#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "Message.h"

namespace zrpc {
/**
 * @brief In-process command types posted as @c Event* over inproc sockets.
 *
 * Pointers are valid only inside the owning @ref Context. The command bus must
 * stay @c inproc://, not @c tcp:// or @c ipc://.
 */
enum class EventType {
    Quit,
    Ready,

    AddServer,
    RemoveServer,
    AddClient,
    RemoveClient,    
    SendRequest,
    ProcessRequest,
    SendReply,
    ProcessReply, 

    AddPub,
    RemovePub,
    AddSub,
    RemoveSub,
    SendTopic,
    ProcessTopic,
    ProcessTopicDone
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

enum class RequestStatus
{
    Replied = 0,
    Timeout = 1,
    Disconnected = 2,
    TryAgain = 3,
};

using ServerFunc = std::function<void(RpcMessage& request, RpcMessage& reply)>;
using ClientFunc = std::function<void(RequestStatus, RpcMessage&)>;
using SubFunc = std::function<void(RpcMessage&)>;

using QuitEvent = EventTemplate<EventType::Quit>;
using ReadyEvent = EventTemplate<EventType::Ready>;

struct AddServerEvent : public EventTemplate<EventType::AddServer>
{
    std::string serverAddr;
    ServerFunc serverFunc;
};

struct RemoveServerEvent : public EventTemplate<EventType::RemoveServer>
{
    uint64_t socketId;
};

struct AddClientEvent : public EventTemplate<EventType::AddClient>
{
    std::string serverAddr;
};

struct RemoveClientEvent : public EventTemplate<EventType::RemoveClient>
{
    uint64_t socketId;
};

struct SendRequestEvent : public EventTemplate<EventType::SendRequest>
{
    uint64_t clientSocketId;
    int64_t timeoutMs{-1};
    ClientFunc clientFunc;
    RpcMessage request;
};

struct ProcessRequestEvent : public EventTemplate<EventType::ProcessRequest>
{
    uint64_t requestId;
    uint64_t serverSocketId;
    ServerFunc serverFunc;
    std::string routerId;
    RpcMessage request;
};

struct SendReplyEvent : public EventTemplate<EventType::SendReply>
{
    uint64_t requestId;
    uint64_t serverSocketId;
    std::string routerId;
    RpcMessage reply;
};

struct ProcessReplyEvent : public EventTemplate<EventType::ProcessReply>
{
    RequestStatus status;
    ClientFunc clientFunc;
    RpcMessage reply;
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
    std::vector<std::string> topics;
    SubFunc subFunc;
};

struct RemoveSubEvent : public EventTemplate<EventType::RemoveSub>
{
    uint64_t socketId;
};

struct SendTopicEvent : public EventTemplate<EventType::SendTopic>
{
    uint64_t socketId;
    RpcMessage message;
};

struct ProcessTopicEvent : public EventTemplate<EventType::ProcessTopic>
{
    uint64_t socketId;
    SubFunc subFunc;
    RpcMessage message;
};

struct ProcessTopicDoneEvent : public EventTemplate<EventType::ProcessTopicDone>
{
    uint64_t socketId;
};
}
