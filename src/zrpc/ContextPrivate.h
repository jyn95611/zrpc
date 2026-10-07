#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

#include "Event.h"
#include "Poller.h"

namespace zrpc {
class Context;
class ContextPrivate;

namespace detail {
struct ContextAccess
{
    static ContextPrivate *get(Context *ctx);
    static ContextPrivate *get(const std::shared_ptr<Context> &ctx) { return get(ctx.get()); }
};
}

/**
 * @brief Context implementation: poller, workers, and all socket sessions.
 */
class ContextPrivate
{
    struct ClientSession;

public:
    ContextPrivate(int ioThrNum, int workerThrNum);
    ~ContextPrivate();

    std::unique_ptr<zmq::socket_t> createFrontendSocket();

    uint64_t nextSocketId();
    uint64_t nextRequestId();

    void quit();
    void wait();

    void poll();
    void waitPollerReady();
    void worker();
    void waitAllWorkerReady();

    void handleBackendSocket();
    void handleClientSocket(uint64_t socketId);
    void handleClientMonitorSocket(uint64_t socketId);
    void handleServerSocket(uint64_t socketId);
    void handleSubSocket(uint64_t socketId);    

    void sendWorkerEvent(Event *event);

    void onAddServerEvent(AddServerEvent *event, const std::string &routerId);
    void onRemoveServerEvent(RemoveServerEvent *event, const std::string &routerId);
    void onAddClientEvent(AddClientEvent *event, const std::string &routerId);
    void onRemoveClientEvent(RemoveClientEvent *event, const std::string &routerId);    
    
    void onSendRequestEvent(SendRequestEvent *event);
    void onProcessReplyEvent(ProcessReplyEvent *event);       
    void completeClientRequest(uint64_t requestId, RequestStatus status,
        RpcMessage reply = {});
    void failClientPendingRequests(uint64_t socketId, RequestStatus status);
    bool sendClientRequest(ClientSession &session, uint64_t requestId);
    void flushClientUnsentRequests(ClientSession &session);
    void onProcessRequestEvent(ProcessRequestEvent *event);
    void onSendReplyEvent(SendReplyEvent *event);
    void releaseServerIfDrained(uint64_t socketId);
         
    void onAddPubEvent(AddPubEvent *event, const std::string &routerId);
    void onRemovePubEvent(RemovePubEvent *event, const std::string &routerId);
    void onAddSubEvent(AddSubEvent *event, const std::string &routerId);
    void onRemoveSubEvent(RemoveSubEvent *event, const std::string &routerId);

    void onSendTopicEvent(SendTopicEvent *event);
    void onProcessTopicEvent(ProcessTopicEvent *event);
    void onProcessTopicDoneEvent(ProcessTopicDoneEvent *event);
    void releaseSubIfDrained(uint64_t socketId);

    void onQuitEvent();

private:
    zmq::context_t _ctx;

    zmq::socket_t _backend;
    std::string _backendAddr;

    zmq::socket_t _frontend;
    std::string _frontendId;
    std::mutex _frontendMutex;

    std::unique_ptr<Poller> _poller;
    std::unique_ptr<std::thread> _pollerThr;
    
    int _currentWorker{0};
    std::vector<std::string> _workerIds;
    std::vector<std::thread> _workerThrs;

    /**
     * @brief Per-socket session state.
     *
     * Client starts in @ref Connecting, moves to @ref Ready on @c CONNECTED,
     * @ref PeerDown on disconnect / first-dial failure, and @ref Shutdown on
     * user close. Server and Subscriber enter the map already @ref Ready.
     */
    enum class SessionState {
        Connecting,
        Ready,
        PeerDown,  ///< Client only: new calls fail immediately; monitor still runs.
        Shutdown   ///< Inbound traffic in the same poll round is ignored.
    };

    struct ClientSession
    {
        std::unique_ptr<zmq::socket_t> socket;
        std::unique_ptr<zmq::socket_t> monitor;
        SessionState state{SessionState::Connecting};
        std::unordered_set<uint64_t> pendingRequests;
        /** Queued while @ref Connecting; flushed in order after @c CONNECTED. */
        std::vector<uint64_t> unsentRequests;
    };

    struct ClientRequest
    {
        uint64_t socketId{0};
        ClientFunc func;
        RpcMessage request;
        bool sent{false};
    };

    struct ServerSession
    {
        std::unique_ptr<zmq::socket_t> socket;
        ServerFunc func;
        int activeCalls{0};
        SessionState state{SessionState::Ready};
        std::string routerId;
    };

    std::atomic<uint64_t> _lastRequestId{0};
    uint64_t _lastSocketId{0};
    std::unordered_map<uint64_t, ServerSession> _serverSessions;
    std::unordered_map<uint64_t, ClientSession> _clientSessions;
    std::unordered_map<uint64_t, ClientRequest> _clientRequests;

    struct SubSession
    {
        std::unique_ptr<zmq::socket_t> socket;
        SubFunc func;
        int activeCalls{0};
        SessionState state{SessionState::Ready};
        std::string routerId;
    };

    std::unordered_map<uint64_t, std::unique_ptr<zmq::socket_t>> _pubSockets;
    std::unordered_map<uint64_t, SubSession> _subSessions;
};
}
