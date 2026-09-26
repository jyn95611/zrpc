#include <cstring>
#include <iostream>
#include <string>

#include "utils.h"
#include "ContextPrivate.h"
#include "Context.h"
#include "Message.h"

namespace zrpc {
namespace detail {
ContextPrivate *ContextAccess::get(Context *ctx)
{
    return ctx->_d;
}
}
ContextPrivate::ContextPrivate(int ioThrNum, int workerThrNum)
    : _ctx(ioThrNum),
      _backend(_ctx, zmq::socket_type::router),
      _frontend(_ctx, zmq::socket_type::dealer)
{
    _backendAddr = "inproc://context_backend";
    _backend.setsockopt(ZMQ_LINGER, 0);
    _backend.bind(_backendAddr);
    std::cout << "Backend socket bind addr: " << _backendAddr << std::endl;

    _frontendId = "context_frontend";
    _frontend.setsockopt(ZMQ_ROUTING_ID, _frontendId.c_str(), _frontendId.size());
    _frontend.setsockopt(ZMQ_LINGER, 0);
    _frontend.connect(_backendAddr);

    _workerThrs.reserve(workerThrNum);
    for (int i = 0; i < workerThrNum; ++i) {
        _workerThrs.emplace_back([this]{ worker(); });
    }

    _poller = std::make_unique<Poller>();
    _pollerThr = std::make_unique<std::thread>([this]{ poll(); });
    waitPollerReady();
}

ContextPrivate::~ContextPrivate()
{
    quit();
    wait();

    _frontend.close();
    _backend.close();
    _clientRequests.clear();
    _clientSessions.clear();
    _serverSockets.clear();
    _pubSockets.clear();
    _subSockets.clear();
}

std::unique_ptr<zmq::socket_t> ContextPrivate::createFrontendSocket()
{
    std::unique_ptr<zmq::socket_t> socket(new zmq::socket_t(_ctx, zmq::socket_type::dealer));
    socket->setsockopt(ZMQ_LINGER, 0);
    socket->connect(_backendAddr);
    return socket;
}

uint64_t ContextPrivate::addClient(const std::string &addr)
{
    std::cout << "Add client begin. addr: " << addr << std::endl;
    auto *event = new ConnectEvent;
    event->serverAddr = addr;
    std::lock_guard<std::mutex> locker(_frontendMutex);    
    DealerWriter(_frontend).writePtr(event, 0);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add client end. client socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removeClient(uint64_t socketId)
{
    auto *event = new DisconnectEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, 0);
}

uint64_t ContextPrivate::addServer(const std::string &addr, const ServerFunc &func)
{
    std::cout << "Add server begin. addr: " << addr << std::endl;
    auto *event = new BindEvent;
    event->serverAddr = addr;
    event->serverFunc = func;
    std::lock_guard<std::mutex> locker(_frontendMutex);    
    DealerWriter(_frontend).writePtr(event, 0);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add server end. server socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removeServer(uint64_t socketId)
{
    auto *event = new UnbindEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);    
    DealerWriter(_frontend).writePtr(event, 0);
}

uint64_t ContextPrivate::addPub(const std::string &addr)
{
    std::cout << "Add pub begin. addr: " << addr << std::endl;
    auto *event = new AddPubEvent;
    event->serverAddr = addr;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, 0);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add pub end. server socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removePub(uint64_t socketId)
{
    auto *event = new RemovePubEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, 0);
}

uint64_t ContextPrivate::addSub(const std::string &addr, const SubFunc &func)
{
    std::cout << "Add sub begin. addr: " << addr << std::endl;
    auto *event = new AddSubEvent;
    event->serverAddr = addr;
    event->subFunc = func;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, 0);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add sub end. server socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removeSub(uint64_t socketId)
{
    auto *event = new RemoveSubEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, 0);
}

uint64_t ContextPrivate::generateSocketId()
{
    return ++_lastSocketId;
}

uint64_t ContextPrivate::nextRequestId()
{
    return _lastRequestId.fetch_add(1, std::memory_order_relaxed) + 1;
}

void ContextPrivate::quit()
{
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(new QuitEvent, 0);
}

void ContextPrivate::wait()
{
    if (_pollerThr->joinable()) {
        _pollerThr->join();
    }

    for (auto &workerThr : _workerThrs) {
        if (workerThr.joinable()) {
            workerThr.join();
        }
    }
}

void ContextPrivate::poll()
{
    {
        waitAllWorkerReady();
        std::cout << "All worker already ready." << std::endl;
        RouterWriter(_backend, _frontendId).writePtr(new ReadyEvent, 0);
    }

    _poller->addSocket(&_backend, [this]{ handleBackendSocket(); });
    _poller->loop();
}

void ContextPrivate::waitPollerReady()
{
    auto event = DealerReader(_frontend).readPtr<Event>();
    if (event->type != EventType::Ready)
        std::cout << "Failed to recv poller ready event." << std::endl;
    std::cout << "Poller already ready." << std::endl;
}

void ContextPrivate::worker()
{
    zmq::socket_t dealer(_ctx, zmq::socket_type::dealer);
    dealer.connect(_backendAddr);
    DealerWriter(dealer).writePtr(new ReadyEvent, 0);

    bool quit{false};
    while (!quit) {
        auto event = DealerReader(dealer).readPtr<Event>();
        switch (event->type) {
        case EventType::ProcessRequest:
            onProcessRequestEvent(static_cast<ProcessRequestEvent*>(event.get()));
            break;
        case EventType::ProcessReply:
            onProcessReplyEvent(static_cast<ProcessReplyEvent*>(event.get()));
            break;
        case EventType::SubTopic:
            onSubTopicEvent(static_cast<SubTopicEvent*>(event.get()));
            break;
        case EventType::Quit:
            quit = true;
            break;
        default:
            break;
        }
    }

    dealer.close();
}

void ContextPrivate::waitAllWorkerReady()
{
    _workerIds.reserve(_workerThrs.size());
    for (int i = 0; i < _workerThrs.size(); ++i) {        
        RouterReader reader(_backend);
        auto event = reader.readPtr<Event>();
        if (event->type != EventType::Ready) {
            std::cout << "Failed to recv worker ready event. Num: " << i << std::endl;
            break;
        }
        _workerIds.emplace_back(std::move(reader.routerId));
    }
}

void ContextPrivate::handleBackendSocket()
{
//    std::cout << "Backend socket on event begin." << std::endl;
    RouterReader reader(_backend);
    auto event = reader.readPtr<Event>();
//    std::cout << "Backend socket on event type: " << int(event->type) << std::endl;
    switch (event->type) {
    case EventType::Connect:
        onConnectEvent(static_cast<ConnectEvent*>(event.get()), reader.routerId);
        break;
    case EventType::Disconnect:
        onDisconnectEvent(static_cast<DisconnectEvent*>(event.get()));
        break;
    case EventType::SendRequest:
        onSendRequestEvent(static_cast<SendRequestEvent*>(event.get()));
        break;
    case EventType::Bind:
        onBindEvent(static_cast<BindEvent*>(event.get()), reader.routerId);
        break;
    case EventType::Unbind:
        onUnbindEvent(static_cast<UnbindEvent*>(event.get()));
        break;
    case EventType::SendReply:
        onSendReplyEvent(static_cast<SendReplyEvent*>(event.get()));
        break;        
    case EventType::AddPub:
        onAddPubEvent(static_cast<AddPubEvent*>(event.get()), reader.routerId);
        break;
    case EventType::RemovePub:
        onRemovePubEvent(static_cast<RemovePubEvent*>(event.get()));
        break;
    case EventType::AddSub:
        onAddSubEvent(static_cast<AddSubEvent*>(event.get()), reader.routerId);
        break;
    case EventType::RemoveSub:
        onRemoveSubEvent(static_cast<RemoveSubEvent*>(event.get()));
        break;
    case EventType::PubTopic:
        onPubTopicEvent(static_cast<PubTopicEvent*>(event.get()));
        break;
    case EventType::Quit:
        onQuitEvent();
        break;
    default:
        break;
    }
//    std::cout << "Backend socket on event end." << std::endl;
}

void ContextPrivate::handleClientSocket(uint64_t socketId)
{
    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter == _clientSessions.end() || !sessionIter->second.socket)
        return;

    DealerReader reader(*sessionIter->second.socket);
    if (reader.error())
        return;

    auto reply = readRpcMessage(reader);
    completeClientRequest(peekRequestId(reply.header), RequestStatus::Replied, std::move(reply));
}

void ContextPrivate::handleClientMonitor(uint64_t socketId)
{
    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter == _clientSessions.end() || !sessionIter->second.monitor)
        return;

    auto &session = sessionIter->second;
    zmq::message_t eventMsg;
    if (!session.monitor->recv(&eventMsg, 0))
        return;

    if (eventMsg.size() < sizeof(uint16_t) + sizeof(int32_t))
        return;

    uint16_t eventId = 0;
    memcpy(&eventId, eventMsg.data(), sizeof(uint16_t));

    if (session.monitor->getsockopt<int>(ZMQ_RCVMORE)) {
        zmq::message_t addrMsg;
        session.monitor->recv(&addrMsg, 0);
    }

    if (eventId == ZMQ_EVENT_CONNECTED) {
        session.connected = true;
        session.everConnected = true;
        return;
    }

    if (eventId == ZMQ_EVENT_DISCONNECTED) {
        session.connected = false;
        failSocketPending(socketId, RequestStatus::Disconnected);
    }
}

void ContextPrivate::completeClientRequest(uint64_t requestId, RequestStatus status,
                                           RpcMessage reply)
{
    auto requestIter = _clientRequests.find(requestId);
    if (requestIter == _clientRequests.end())
        return;

    auto *event = new ProcessReplyEvent;
    event->status = status;
    event->clientFunc = std::move(requestIter->second.func);
    event->reply = std::move(reply);
    sendWorkerEvent(event);

    const uint64_t socketId = requestIter->second.socketId;
    _clientRequests.erase(requestIter);

    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter != _clientSessions.end())
        sessionIter->second.pendingRequests.erase(requestId);
}

void ContextPrivate::failSocketPending(uint64_t socketId, RequestStatus status)
{
    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter == _clientSessions.end())
        return;

    const auto requestIds = sessionIter->second.pendingRequests;
    for (auto requestId : requestIds)
        completeClientRequest(requestId, status);
}

void ContextPrivate::handleClientRequestTimeout(uint64_t requestId)
{
    completeClientRequest(requestId, RequestStatus::Timeout);
}

void ContextPrivate::handleServerSocket(uint64_t socketId, const ServerFunc &serverFunc)
{
    RouterReader reader(*_serverSockets[socketId]);
    if (reader.error()) {
        std::cout << "Server socket recv invalid message." << std::endl;
        return;
    }

    auto routerId = reader.routerId;
    auto request = readRpcMessage(reader);

    auto *event = new ProcessRequestEvent;
    event->requestId = peekRequestId(request.header);
    event->serverSocketId = socketId;
    event->serverFunc = serverFunc;
    event->routerId = std::move(routerId);
    event->request = std::move(request);
    sendWorkerEvent(event);
}

void ContextPrivate::handleSubSocket(uint64_t socketId, const SubFunc &subFunc)
{
    SocketdReader reader(*_subSockets[socketId]);

    auto *event = new SubTopicEvent;
    event->message = readRpcMessage(reader);
    event->subFunc = subFunc;
    sendWorkerEvent(event);
}

void ContextPrivate::sendWorkerEvent(Event *event)
{
    RouterWriter(_backend, _workerIds[_currentWorker]).writePtr(event, 0);
    ++_currentWorker;
    if (_currentWorker == _workerIds.size()) {
        _currentWorker = 0;
    }
}

void ContextPrivate::onConnectEvent(ConnectEvent *event, const std::string &routerId)
{
    auto socket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::dealer);
    socket->setsockopt(ZMQ_LINGER, 0);

    const uint64_t socketId = generateSocketId();
    const auto monitorAddr = "inproc://zrpc-client-monitor-" + std::to_string(socketId);
    zmq_socket_monitor(static_cast<void *>(*socket), monitorAddr.c_str(),
                       ZMQ_EVENT_CONNECTED | ZMQ_EVENT_DISCONNECTED);

    auto monitor = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::pair);
    monitor->setsockopt(ZMQ_LINGER, 0);
    monitor->connect(monitorAddr);

    socket->connect(event->serverAddr);

    _poller->addSocket(socket.get(), [this, socketId]{
        handleClientSocket(socketId);
    });
    _poller->addSocket(monitor.get(), [this, socketId]{
        handleClientMonitor(socketId);
    });

    ClientSession session;
    session.socket = std::move(socket);
    session.monitor = std::move(monitor);
    _clientSessions.emplace(socketId, std::move(session));

    RouterWriter(_backend, routerId).write(socketId, 0);
}

void ContextPrivate::onDisconnectEvent(DisconnectEvent *event)
{
    _poller->postCallback([this, socketId = event->socketId]{
        failSocketPending(socketId, RequestStatus::Disconnected);

        auto sessionIter = _clientSessions.find(socketId);
        if (sessionIter == _clientSessions.end())
            return;

        auto session = std::move(sessionIter->second);
        _clientSessions.erase(sessionIter);

        if (session.socket) {
            zmq_socket_monitor(static_cast<void *>(*session.socket), nullptr, 0);
            _poller->removeSocket(session.socket.get());
        }
        if (session.monitor)
            _poller->removeSocket(session.monitor.get());
    });
}

void ContextPrivate::onSendRequestEvent(SendRequestEvent *event)
{
    auto sessionIter = _clientSessions.find(event->clientSocketId);
    const bool disconnected = sessionIter != _clientSessions.end()
        && sessionIter->second.everConnected
        && !sessionIter->second.connected;
    if (disconnected || sessionIter == _clientSessions.end() || !sessionIter->second.socket) {
        auto *reply = new ProcessReplyEvent;
        reply->status = RequestStatus::Disconnected;
        reply->clientFunc = std::move(event->clientFunc);
        sendWorkerEvent(reply);
        return;
    }

    auto &session = sessionIter->second;
    const uint64_t requestId = peekRequestId(event->request.header);
    _clientRequests.emplace(requestId, ClientRequest{event->clientSocketId, std::move(event->clientFunc)});
    session.pendingRequests.insert(requestId);

    if (event->timeoutMs > 0) {
        _poller->runCallbackAfter(event->timeoutMs, [this, requestId]{
            handleClientRequestTimeout(requestId);
        });
    }

    DealerWriter writer(*session.socket);
    writeRpcMessage(writer, event->request);
}

void ContextPrivate::onProcessReplyEvent(ProcessReplyEvent *event)
{
    event->clientFunc(event->status, event->reply);
}

void ContextPrivate::onBindEvent(BindEvent *event, const std::string &routerId)
{
    auto socket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::router);
    socket->bind(event->serverAddr);

    const uint64_t socketId = generateSocketId();
    _poller->addSocket(socket.get(), [this, socketId, serverFunc = event->serverFunc]{
        handleServerSocket(socketId, serverFunc);
    });
    _serverSockets.emplace(socketId, std::move(socket));

    RouterWriter(_backend, routerId).write(socketId, 0);
}

void ContextPrivate::onUnbindEvent(UnbindEvent *event)
{
    _poller->postCallback([this, socketId = event->socketId]{
        auto socket = std::move(_serverSockets[socketId]);
        _serverSockets.erase(socketId);
        _poller->removeSocket(socket.get());
    });
}

void ContextPrivate::onProcessRequestEvent(ProcessRequestEvent *event)
{
    RpcMessage reply;
    event->serverFunc(event->request, reply);

    std::lock_guard<std::mutex> locker(_frontendMutex);
    auto *replyEvent = new SendReplyEvent;
    replyEvent->requestId = event->requestId;
    replyEvent->serverSocketId = event->serverSocketId;
    replyEvent->routerId = std::move(event->routerId);
    replyEvent->reply = std::move(reply);
    DealerWriter(_frontend).writePtr(replyEvent, 0);
}

void ContextPrivate::onSendReplyEvent(SendReplyEvent *event)
{
    auto socketIter = _serverSockets.find(event->serverSocketId);
    if (socketIter == _serverSockets.end() || !socketIter->second)
        return;

    RouterWriter writer(*socketIter->second, event->routerId);
    writeRpcMessage(writer, event->reply);
}

void ContextPrivate::onAddPubEvent(AddPubEvent *event, const std::string &routerId)
{
    auto pubSocket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::pub);
    pubSocket->bind(event->serverAddr);

    const auto socketId = generateSocketId();
    _pubSockets.emplace(socketId, std::move(pubSocket));

    RouterWriter(_backend, routerId).write(socketId, 0);
}

void ContextPrivate::onRemovePubEvent(RemovePubEvent *event)
{
    _poller->postCallback([this, socketId = event->socketId]{
        _pubSockets.erase(socketId);
    });
}

void ContextPrivate::onAddSubEvent(AddSubEvent *event, const std::string &routerId)
{
    auto subSocket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::sub);
    subSocket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
    subSocket->connect(event->serverAddr);

    const auto socketId = generateSocketId();
    _poller->addSocket(subSocket.get(), [this, socketId, subFunc = event->subFunc]{
        handleSubSocket(socketId, subFunc);
    });
    _subSockets.emplace(socketId, std::move(subSocket));

    RouterWriter(_backend, routerId).write(socketId, 0);
}

void ContextPrivate::onRemoveSubEvent(RemoveSubEvent *event)
{
    _poller->postCallback([this, socketId = event->socketId]{
        auto socket = std::move(_subSockets[socketId]);
        _subSockets.erase(socketId);
        _poller->removeSocket(socket.get());
    });
}

void ContextPrivate::onPubTopicEvent(PubTopicEvent *event)
{
    SocketWriter writer(*_pubSockets[event->socketId]);
    writeRpcMessage(writer, event->message);
}

void ContextPrivate::onSubTopicEvent(SubTopicEvent *event)
{
    if (event->subFunc)
        event->subFunc(event->message);
}

void ContextPrivate::onQuitEvent()
{
    for (auto &worker : _workerIds) {
        sendWorkerEvent(new QuitEvent);
    }
    _poller->quit();
}

Context::Context(int ioThrNum, int workerThrNum)
    : _d(new ContextPrivate(ioThrNum <= 0 ? 1 : ioThrNum, workerThrNum <= 0 ? 1 : workerThrNum))
{

}

Context::~Context()
{
    delete _d;
}

void Context::quit()
{
    _d->quit();
}

void Context::wait()
{
    _d->wait();
}
}
