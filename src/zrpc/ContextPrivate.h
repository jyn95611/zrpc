#pragma once

#include <vector>
#include <unordered_map>
#include <memory>
#include <thread>
#include <mutex>
#include <zmq.hpp>

#include "Event.h"
#include "Poller.h"

namespace zrpc {
class ContextPrivate
{
public:
    ContextPrivate(int ioThrNum, int workerThrNum);
    ~ContextPrivate();

    std::unique_ptr<zmq::socket_t> createFrontendSocket();

    uint64_t addClient(const std::string &addr);
    void removeClient(uint64_t socketId);
    uint64_t addServer(const std::string &addr, const ServerFunc &func);
    void removeServer(uint64_t socketId);

    uint64_t addPub(const std::string &addr);
    void removePub(uint64_t socketId);
    uint64_t addSub(const std::string &addr, const SubFunc &func);
    void removeSub(uint64_t socketId);

    uint64_t generateSocketId();
    uint64_t generateRequestId();

    void quit();
    void wait();

    void poll();
    void waitPollerReady();
    void worker();
    void waitAllWorkerReady();

    void handleBackendSocket();
    void handleClientSocket(uint64_t socketId);
    void handleClientRequestTimeout(uint64_t requestId);
    void handleServerSocket(uint64_t socketId, const ServerFunc &serverFunc);
    void handleSubSocket(uint64_t socketId, const SubFunc &subFunc);

    void sendWorkerEvent(Event *event);

    void onConnectEvent(ConnectEvent *event, const std::string &routerId);
    void onDisconnectEvent(DisconnectEvent *event);
    void onSendRequestEvent(SendRequestEvent *event);
    void onProcessReplyEvent(ProcessReplyEvent *event);
    void onBindEvent(BindEvent *event, const std::string &routerId);
    void onUnbindEvent(UnbindEvent *event);
    void onProcessRequestEvent(ProcessRequestEvent *event);
    void onSendReplyEvent(SendReplyEvent *event);

    void onAddPubEvent(AddPubEvent *event, const std::string &routerId);
    void onRemovePubEvent(RemovePubEvent *event);
    void onAddSubEvent(AddSubEvent *event, const std::string &routerId);
    void onRemoveSubEvent(RemoveSubEvent *event);
    void onPubTopicEvent(PubTopicEvent *event);
    void onSubTopicEvent(SubTopicEvent *event);

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

    uint64_t _lastRequestId{0};
    uint64_t _lastSocketId{0};
    std::unordered_map<uint64_t, std::unique_ptr<zmq::socket_t>> _serverSockets;
    std::unordered_map<uint64_t, std::unique_ptr<zmq::socket_t>> _clientSockets;
    std::unordered_map<uint64_t, ClientFunc> _clientFuncs;

    std::unordered_map<uint64_t, std::unique_ptr<zmq::socket_t>> _pubSockets;
    std::unordered_map<uint64_t, std::unique_ptr<zmq::socket_t>> _subSockets;

    int _currentWorker{0};
    std::vector<std::string> _workerIds;
    std::vector<std::thread> _workerThrs;
};
}
