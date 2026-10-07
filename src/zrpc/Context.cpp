#include <algorithm>
#include <cstring>
#include <string>
#include <spdlog/spdlog.h>

#include "utils.h"
#include "Message.h"
#include "ContextPrivate.h"
#include "Context.h"

namespace zrpc {
namespace detail {
ContextPrivate *ContextAccess::get(Context *ctx)
{
    return ctx->_d.get();
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
    spdlog::info("backend bound to {}", _backendAddr);

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
    _serverSessions.clear();
    _pubSockets.clear();
    _subSessions.clear();
}

std::unique_ptr<zmq::socket_t> ContextPrivate::createFrontendSocket()
{
    std::unique_ptr<zmq::socket_t> socket(new zmq::socket_t(_ctx, zmq::socket_type::dealer));
    socket->setsockopt(ZMQ_LINGER, 0);
    socket->connect(_backendAddr);
    return socket;
}

uint64_t ContextPrivate::nextSocketId()
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
    DealerSender(_frontend).sendPtr(new QuitEvent, 0);
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
        spdlog::info("all workers ready");
        RouterSender(_backend, _frontendId).sendPtr(new ReadyEvent, 0);
    }

    _poller->addSocket(&_backend, [this]{ handleBackendSocket(); });
    _poller->loop();
}

void ContextPrivate::waitPollerReady()
{
    auto event = DealerReceiver(_frontend).recvPtr<Event>();
    if (event->type != EventType::Ready)
        spdlog::error("failed to recv poller ready event");
    else
        spdlog::info("poller ready");
}

void ContextPrivate::worker()
{
    zmq::socket_t dealer(_ctx, zmq::socket_type::dealer);
    dealer.connect(_backendAddr);
    DealerSender(dealer).sendPtr(new ReadyEvent, 0);

    bool quit{false};
    while (!quit) {
        auto event = DealerReceiver(dealer).recvPtr<Event>();
        switch (event->type) {
        case EventType::ProcessRequest:
            onProcessRequestEvent(static_cast<ProcessRequestEvent*>(event.get()));
            break;
        case EventType::ProcessReply:
            onProcessReplyEvent(static_cast<ProcessReplyEvent*>(event.get()));
            break;
        case EventType::ProcessTopic:
            onProcessTopicEvent(static_cast<ProcessTopicEvent*>(event.get()));
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
        RouterReceiver receiver(_backend);
        auto event = receiver.recvPtr<Event>();
        if (event->type != EventType::Ready) {
            spdlog::error("failed to recv worker ready event, index={}", i);
            break;
        }
        _workerIds.emplace_back(std::move(receiver.routerId));
    }
}

void ContextPrivate::handleBackendSocket()
{
    RouterReceiver receiver(_backend);
    auto event = receiver.recvPtr<Event>();
    switch (event->type) {
    case EventType::AddClient:
        onAddClientEvent(static_cast<AddClientEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::RemoveClient:
        onRemoveClientEvent(static_cast<RemoveClientEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::SendRequest:
        onSendRequestEvent(static_cast<SendRequestEvent*>(event.get()));
        break;
    case EventType::AddServer:
        onAddServerEvent(static_cast<AddServerEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::RemoveServer:
        onRemoveServerEvent(static_cast<RemoveServerEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::SendReply:
        onSendReplyEvent(static_cast<SendReplyEvent*>(event.get()));
        break;        
    case EventType::AddPub:
        onAddPubEvent(static_cast<AddPubEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::RemovePub:
        onRemovePubEvent(static_cast<RemovePubEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::AddSub:
        onAddSubEvent(static_cast<AddSubEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::RemoveSub:
        onRemoveSubEvent(static_cast<RemoveSubEvent*>(event.get()), receiver.routerId);
        break;
    case EventType::SendTopic:
        onSendTopicEvent(static_cast<SendTopicEvent*>(event.get()));
        break;
    case EventType::ProcessTopicDone:
        onProcessTopicDoneEvent(static_cast<ProcessTopicDoneEvent*>(event.get()));
        break;
    case EventType::Quit:
        onQuitEvent();
        break;
    default:
        break;
    }
}

void ContextPrivate::handleClientSocket(uint64_t socketId)
{
    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter == _clientSessions.end() || !sessionIter->second.socket
        || sessionIter->second.state == SessionState::Shutdown)
        return;

    DealerReceiver receiver(*sessionIter->second.socket);
    if (receiver.error())
        return;

    auto reply = recvRpcMessage(receiver);
    const uint64_t requestId = peekRequestId(reply.header);
    /// Unmatched request ids are dropped inside completeClientRequest.
    completeClientRequest(requestId, RequestStatus::Replied, std::move(reply));
}

void ContextPrivate::handleClientMonitorSocket(uint64_t socketId)
{
    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter == _clientSessions.end() || !sessionIter->second.monitor
        || sessionIter->second.state == SessionState::Shutdown)
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

    /// First RPC and any queued unsent requests are flushed in enqueue order.
    if (eventId == ZMQ_EVENT_CONNECTED) {
        session.state = SessionState::Ready;
        flushClientUnsentRequests(session);
        return;
    }

    /// Fail in-flight calls; already-sent requests are not retried. ZMQ reconnects on its own.
    if (eventId == ZMQ_EVENT_DISCONNECTED) {
        session.state = SessionState::PeerDown;
        failClientPendingRequests(socketId, RequestStatus::Disconnected);
        return;
    }

    /// First dial only: fail-fast so the request never hits TCP. Later CONNECT_RETRIED events are ignored.
    if (eventId == ZMQ_EVENT_CONNECT_RETRIED
        && session.state == SessionState::Connecting) {
        session.state = SessionState::PeerDown;
        failClientPendingRequests(socketId, RequestStatus::Disconnected);
    }
}

void ContextPrivate::handleServerSocket(uint64_t socketId)
{
    auto sessionIter = _serverSessions.find(socketId);
    if (sessionIter == _serverSessions.end() || !sessionIter->second.socket
        || sessionIter->second.state == SessionState::Shutdown)
        return;

    auto &session = sessionIter->second;
    RouterReceiver receiver(*session.socket);
    if (receiver.error()) {
        spdlog::warn("server socket recv invalid message, socketId={}", socketId);
        return;
    }

    auto routerId = receiver.routerId;
    auto request = recvRpcMessage(receiver);

    auto *event = new ProcessRequestEvent;
    event->requestId = peekRequestId(request.header);
    event->serverSocketId = socketId;
    event->serverFunc = session.func;
    event->routerId = std::move(routerId);
    event->request = std::move(request);
    ++session.activeCalls;
    sendWorkerEvent(event);
}

void ContextPrivate::handleSubSocket(uint64_t socketId)
{
    auto sessionIter = _subSessions.find(socketId);
    if (sessionIter == _subSessions.end() || !sessionIter->second.socket
        || sessionIter->second.state == SessionState::Shutdown)
        return;

    auto &session = sessionIter->second;
    FrameReceiver receiver(*session.socket);
    auto message = recvRpcMessage(receiver);
    if (!message.valid) {
        spdlog::warn("sub socket recv invalid message, socketId={}", socketId);
        return; // Do not enqueue or bump activeCalls.
    }

    auto *event = new ProcessTopicEvent;
    event->socketId = socketId;
    event->message = std::move(message);
    event->subFunc = session.func;
    ++session.activeCalls;
    sendWorkerEvent(event);
}

void ContextPrivate::sendWorkerEvent(Event *event)
{
    /// Round-robin by count, not by duration or socket. Methods must be reentrant.
    RouterSender(_backend, _workerIds[_currentWorker]).sendPtr(event, 0);
    ++_currentWorker;
    if (_currentWorker == _workerIds.size()) {
        _currentWorker = 0;
    }
}

void ContextPrivate::onAddServerEvent(AddServerEvent *event, const std::string &routerId)
{
    uint64_t socketId = 0;
    std::string error;
    try {
        auto socket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::router);
        socket->setsockopt(ZMQ_LINGER, 0);
        socket->setsockopt(ZMQ_SNDTIMEO, 0); // Non-blocking send; failed replies are dropped.
        socket->bind(normalizeZmqAddr(event->serverAddr));

        const uint64_t id = nextSocketId();
        ServerSession session;
        session.socket = std::move(socket);
        session.func = event->serverFunc;
        session.routerId = routerId;
        auto &stored = _serverSessions.emplace(id, std::move(session)).first->second;
        _poller->addSocket(stored.socket.get(), [this, id]{
            handleServerSocket(id);
        });
        socketId = id;
    } catch (...) {
        error = currentExceptionMessage();
        socketId = 0;
    }
    sendSocketEventAck(_backend, routerId, socketId, error);
}

void ContextPrivate::onRemoveServerEvent(RemoveServerEvent *event, const std::string &routerId)
{
    auto sessionIter = _serverSessions.find(event->socketId);
    if (sessionIter != _serverSessions.end())
        sessionIter->second.state = SessionState::Shutdown;

    _poller->postCallback([this, socketId = event->socketId, routerId]{
        auto sessionIter = _serverSessions.find(socketId);
        if (sessionIter == _serverSessions.end()) {
            sendSocketEventAck(_backend, routerId, socketId, {});
            return;
        }

        auto &session = sessionIter->second;
        if (session.socket) {
            _poller->removeSocket(session.socket.get());
            session.socket.reset();
        }
        sendSocketEventAck(_backend, routerId, socketId, {});
        releaseServerIfDrained(socketId);
    });
}

void ContextPrivate::onAddClientEvent(AddClientEvent *event, const std::string &routerId)
{
    uint64_t socketId = 0;
    std::string error;
    try {
        auto socket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::dealer);
        socket->setsockopt(ZMQ_LINGER, 0);
        socket->setsockopt(ZMQ_SNDTIMEO, 0);

        const uint64_t id = nextSocketId();
        const auto monitorAddr = "inproc://zrpc-client-monitor-" + std::to_string(id);
        /// Attach the monitor before connect(), or a local listener can skip CONNECTED.
        if (zmq_socket_monitor(static_cast<void *>(*socket), monitorAddr.c_str(),
                               ZMQ_EVENT_CONNECTED | ZMQ_EVENT_DISCONNECTED
                                   | ZMQ_EVENT_CONNECT_RETRIED) != 0) {
            throw zmq::error_t();
        }

        auto monitor = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::pair);
        monitor->setsockopt(ZMQ_LINGER, 0);
        monitor->connect(monitorAddr);

        socket->connect(normalizeZmqAddr(event->serverAddr));

        ClientSession session;
        session.socket = std::move(socket);
        session.monitor = std::move(monitor);
        auto &stored = _clientSessions.emplace(id, std::move(session)).first->second;

        _poller->addSocket(stored.socket.get(), [this, id]{
            handleClientSocket(id);
        });
        _poller->addSocket(stored.monitor.get(), [this, id]{
            handleClientMonitorSocket(id);
        });
        socketId = id;
    } catch (...) {
        error = currentExceptionMessage();
        socketId = 0;
    }
    sendSocketEventAck(_backend, routerId, socketId, error);
}

void ContextPrivate::onRemoveClientEvent(RemoveClientEvent *event, const std::string &routerId)
{
    auto sessionIter = _clientSessions.find(event->socketId);
    if (sessionIter != _clientSessions.end())
        sessionIter->second.state = SessionState::Shutdown;

    _poller->postCallback([this, socketId = event->socketId, routerId]{
        failClientPendingRequests(socketId, RequestStatus::Disconnected);

        auto sessionIter = _clientSessions.find(socketId);
        if (sessionIter != _clientSessions.end()) {
            auto session = std::move(sessionIter->second);
            _clientSessions.erase(sessionIter);

            if (session.socket) {
                zmq_socket_monitor(static_cast<void *>(*session.socket), nullptr, 0);
                _poller->removeSocket(session.socket.get());
            }
            if (session.monitor)
                _poller->removeSocket(session.monitor.get());
        }
        sendSocketEventAck(_backend, routerId, socketId, {});
    });
}

void ContextPrivate::onSendRequestEvent(SendRequestEvent *event)
{
    /// PeerDown / Shutdown fail immediately; calls are not queued (not wait-for-ready).
    auto sessionIter = _clientSessions.find(event->clientSocketId);
    if (sessionIter == _clientSessions.end() || !sessionIter->second.socket
        || sessionIter->second.state == SessionState::PeerDown
        || sessionIter->second.state == SessionState::Shutdown) {
        auto *reply = new ProcessReplyEvent;
        reply->status = RequestStatus::Disconnected;
        reply->clientFunc = std::move(event->clientFunc);
        sendWorkerEvent(reply);
        return;
    }

    auto &session = sessionIter->second;
    const uint64_t requestId = peekRequestId(event->request.header);
    ClientRequest request;
    request.socketId = event->clientSocketId;
    request.func = std::move(event->clientFunc);
    request.request = std::move(event->request);
    _clientRequests.emplace(requestId, std::move(request));
    session.pendingRequests.insert(requestId);

    if (event->timeoutMs > 0) {
        _poller->runCallbackAfter(event->timeoutMs, [this, requestId]{
            completeClientRequest(requestId, RequestStatus::Timeout);
        });
    }

    /// Hold until this dial completes; pipelined calls share it and flush in order.
    if (session.state == SessionState::Connecting) {
        session.unsentRequests.push_back(requestId);
        return;
    }

    sendClientRequest(session, requestId);
}

void ContextPrivate::onProcessReplyEvent(ProcessReplyEvent *event)
{
    event->clientFunc(event->status, event->reply);
}

void ContextPrivate::completeClientRequest(uint64_t requestId, RequestStatus status,
                                           RpcMessage reply)
{
    auto requestIter = _clientRequests.find(requestId);
    if (requestIter == _clientRequests.end())
        return; // Late or duplicate reply: drop, do not complete twice.

    auto *event = new ProcessReplyEvent;
    event->status = status;
    event->clientFunc = std::move(requestIter->second.func);
    event->reply = std::move(reply);
    sendWorkerEvent(event);

    const uint64_t socketId = requestIter->second.socketId;
    _clientRequests.erase(requestIter);

    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter != _clientSessions.end()) {
        sessionIter->second.pendingRequests.erase(requestId);
        auto &unsent = sessionIter->second.unsentRequests;
        if (!unsent.empty()) {
            unsent.erase(std::remove(unsent.begin(), unsent.end(), requestId),
                         unsent.end());
        }
    }
}

void ContextPrivate::failClientPendingRequests(uint64_t socketId, RequestStatus status)
{
    auto sessionIter = _clientSessions.find(socketId);
    if (sessionIter == _clientSessions.end())
        return;

    sessionIter->second.unsentRequests.clear();
    const auto requestIds = sessionIter->second.pendingRequests;
    for (auto requestId : requestIds)
        completeClientRequest(requestId, status);
}

bool ContextPrivate::sendClientRequest(ClientSession &session, uint64_t requestId)
{
    auto requestIter = _clientRequests.find(requestId);
    if (!session.socket || requestIter == _clientRequests.end() || requestIter->second.sent)
        return true;

    DealerSender sender(*session.socket);
    if (!sendRpcMessage(sender, requestIter->second.request)) {
        completeClientRequest(requestId, RequestStatus::TryAgain); // Not written to ZMQ; caller may retry.
        return false;
    }

    requestIter->second.sent = true;
    return true;
}

void ContextPrivate::flushClientUnsentRequests(ClientSession &session)
{
    const auto requestIds = std::move(session.unsentRequests);
    session.unsentRequests.clear();
    for (auto requestId : requestIds)
        sendClientRequest(session, requestId);
}

void ContextPrivate::onProcessRequestEvent(ProcessRequestEvent *event)
{
    RpcMessage reply;
    event->serverFunc(event->request, reply);

    auto *replyEvent = new SendReplyEvent;
    replyEvent->requestId = event->requestId;
    replyEvent->serverSocketId = event->serverSocketId;
    replyEvent->routerId = std::move(event->routerId);
    replyEvent->reply = std::move(reply);
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerSender(_frontend).sendPtr(replyEvent, 0);
}

void ContextPrivate::onSendReplyEvent(SendReplyEvent *event)
{
    auto sessionIter = _serverSessions.find(event->serverSocketId);
    if (sessionIter == _serverSessions.end())
        return;

    auto &session = sessionIter->second;
    if (session.socket) {
        RouterSender sender(*session.socket, event->routerId);
        if (!sendRpcMessage(sender, event->reply)) {
            spdlog::warn("server reply send failed, socketId={} requestId={}",
                         event->serverSocketId, event->requestId);
        }
    }
    if (session.activeCalls > 0)
        --session.activeCalls;
    releaseServerIfDrained(event->serverSocketId);
}

void ContextPrivate::releaseServerIfDrained(uint64_t socketId)
{
    auto sessionIter = _serverSessions.find(socketId);
    if (sessionIter == _serverSessions.end())
        return;

    auto &session = sessionIter->second;
    if (session.state != SessionState::Shutdown || session.activeCalls > 0)
        return;

    sendSocketEventAck(_backend, session.routerId, socketId, {});
    _serverSessions.erase(sessionIter);
}

void ContextPrivate::onAddPubEvent(AddPubEvent *event, const std::string &routerId)
{
    uint64_t socketId = 0;
    std::string error;
    try {
        auto pubSocket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::pub);
        pubSocket->setsockopt(ZMQ_LINGER, 0);
        pubSocket->setsockopt(ZMQ_SNDTIMEO, 0);
        pubSocket->bind(normalizeZmqAddr(event->serverAddr));

        const uint64_t id = nextSocketId();
        _pubSockets.emplace(id, std::move(pubSocket));
        socketId = id;
    } catch (...) {
        error = currentExceptionMessage();
        socketId = 0;
    }
    sendSocketEventAck(_backend, routerId, socketId, error);
}

void ContextPrivate::onRemovePubEvent(RemovePubEvent *event, const std::string &routerId)
{
    _poller->postCallback([this, socketId = event->socketId, routerId]{
        _pubSockets.erase(socketId);
        sendSocketEventAck(_backend, routerId, socketId, {});
    });
}

void ContextPrivate::onAddSubEvent(AddSubEvent *event, const std::string &routerId)
{
    uint64_t socketId = 0;
    std::string error;
    try {
        auto subSocket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::sub);
        for (const auto &topic : event->topics) {
            if (topic.empty())
                continue;
            subSocket->setsockopt(ZMQ_SUBSCRIBE, topic.data(), topic.size());
        }
        subSocket->connect(normalizeZmqAddr(event->serverAddr));

        const uint64_t id = nextSocketId();
        SubSession session;
        session.socket = std::move(subSocket);
        session.func = event->subFunc;
        session.routerId = routerId;
        auto &stored = _subSessions.emplace(id, std::move(session)).first->second;
        _poller->addSocket(stored.socket.get(), [this, id]{
            handleSubSocket(id);
        });
        socketId = id;
    } catch (...) {
        error = currentExceptionMessage();
        socketId = 0;
    }
    sendSocketEventAck(_backend, routerId, socketId, error);
}

void ContextPrivate::onRemoveSubEvent(RemoveSubEvent *event, const std::string &routerId)
{
    auto sessionIter = _subSessions.find(event->socketId);
    if (sessionIter != _subSessions.end())
        sessionIter->second.state = SessionState::Shutdown;

    _poller->postCallback([this, socketId = event->socketId, routerId]{
        auto sessionIter = _subSessions.find(socketId);
        if (sessionIter == _subSessions.end()) {
            sendSocketEventAck(_backend, routerId, socketId, {});
            return;
        }

        auto &session = sessionIter->second;
        if (session.socket) {
            _poller->removeSocket(session.socket.get());
            session.socket.reset();
        }
        sendSocketEventAck(_backend, routerId, socketId, {});
        releaseSubIfDrained(socketId);
    });
}

void ContextPrivate::releaseSubIfDrained(uint64_t socketId)
{
    auto sessionIter = _subSessions.find(socketId);
    if (sessionIter == _subSessions.end())
        return;

    auto &session = sessionIter->second;
    if (session.state != SessionState::Shutdown || session.activeCalls > 0)
        return;
    sendSocketEventAck(_backend, session.routerId, socketId, {});
    _subSessions.erase(sessionIter);
}

void ContextPrivate::onSendTopicEvent(SendTopicEvent *event)
{
    auto socketIter = _pubSockets.find(event->socketId);
    if (socketIter == _pubSockets.end() || !socketIter->second)
        return;

    FrameSender sender(*socketIter->second);
    if (!sendRpcMessage(sender, event->message)) {
        spdlog::warn("pub topic send failed, socketId={}", event->socketId);
    }
}

void ContextPrivate::onProcessTopicEvent(ProcessTopicEvent *event)
{
    if (event->subFunc)
        event->subFunc(event->message);

    auto *done = new ProcessTopicDoneEvent;
    done->socketId = event->socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerSender(_frontend).sendPtr(done, 0);
}

void ContextPrivate::onProcessTopicDoneEvent(ProcessTopicDoneEvent *event)
{
    auto sessionIter = _subSessions.find(event->socketId);
    if (sessionIter == _subSessions.end())
        return;
    if (sessionIter->second.activeCalls > 0)
        --sessionIter->second.activeCalls;
    releaseSubIfDrained(event->socketId);
}

void ContextPrivate::onQuitEvent()
{
    for (auto &worker : _workerIds) {
        sendWorkerEvent(new QuitEvent);
    }
    _poller->quit();
}

Context::Context(int ioThrNum, int workerThrNum)
    : _d(std::make_unique<ContextPrivate>(ioThrNum <= 0 ? 1 : ioThrNum,
                                         workerThrNum <= 0 ? 1 : workerThrNum))
{

}

Context::~Context() = default;

void Context::quit()
{
    _d->quit();
}

void Context::wait()
{
    _d->wait();
}
}
