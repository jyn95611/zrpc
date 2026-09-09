#include <iostream>

#include "utils.h"
#include "ContextPrivate.h"
#include "Context.h"

namespace zrpc {
ContextPrivate::ContextPrivate(int ioThrNum, int workerThrNum)
    : _ctx(ioThrNum)
{
    _backendAddr = "inproc://context_backend";
    _backend = zmq::socket_t(_ctx, zmq::socket_type::router);
    _backend.set(zmq::sockopt::linger, 0);
    _backend.bind(_backendAddr);
    std::cout << "Backend socket bind addr: " << _backendAddr << std::endl;

    _frontend = zmq::socket_t(_ctx, zmq::socket_type::dealer);
    _frontendId = "context_frontend";
    _frontend.set(zmq::sockopt::routing_id, _frontendId.c_str());
    _frontend.set(zmq::sockopt::linger, 0);
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
    _clientFuncs.clear();
    _clientSockets.clear();
    _serverSockets.clear();
    _pubSockets.clear();
    _subSockets.clear();
}

std::unique_ptr<zmq::socket_t> ContextPrivate::createFrontendSocket()
{
    std::unique_ptr<zmq::socket_t> socket(new zmq::socket_t(_ctx, zmq::socket_type::dealer));
    socket->set(zmq::sockopt::linger, 0);
    socket->connect(_backendAddr);
    return socket;
}

uint64_t ContextPrivate::addClient(const std::string &addr)
{
    std::cout << "Add client begin. addr: " << addr << std::endl;
    auto *event = new ConnectEvent;
    event->serverAddr = addr;
    std::lock_guard<std::mutex> locker(_frontendMutex);    
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add client end. client socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removeClient(uint64_t socketId)
{
    auto *event = new DisconnectEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);
}

uint64_t ContextPrivate::addServer(const std::string &addr, const ServerFunc &func)
{
    std::cout << "Add server begin. addr: " << addr << std::endl;
    auto *event = new BindEvent;
    event->serverAddr = addr;
    event->serverFunc = func;
    std::lock_guard<std::mutex> locker(_frontendMutex);    
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add server end. server socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removeServer(uint64_t socketId)
{
    auto *event = new UnbindEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);    
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);
}

uint64_t ContextPrivate::addPub(const std::string &addr)
{
    std::cout << "Add pub begin. addr: " << addr << std::endl;
    auto *event = new AddPubEvent;
    event->serverAddr = addr;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add pub end. server socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removePub(uint64_t socketId)
{
    auto *event = new RemovePubEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);
}

uint64_t ContextPrivate::addSub(const std::string &addr, const SubFunc &func)
{
    std::cout << "Add sub begin. addr: " << addr << std::endl;
    auto *event = new AddSubEvent;
    event->serverAddr = addr;
    event->subFunc = func;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);

    const auto socketId = DealerReader(_frontend).read<uint64_t>();
    std::cout << "Add sub end. server socketId: " << socketId << std::endl;
    return socketId;
}

void ContextPrivate::removeSub(uint64_t socketId)
{
    auto *event = new RemoveSubEvent;
    event->socketId = socketId;
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(event, zmq::send_flags::none);
}

uint64_t ContextPrivate::generateSocketId()
{
    return ++_lastSocketId;
}

uint64_t ContextPrivate::generateRequestId()
{
    return ++_lastRequestId;
}

void ContextPrivate::quit()
{
    std::lock_guard<std::mutex> locker(_frontendMutex);
    DealerWriter(_frontend).writePtr(new QuitEvent, zmq::send_flags::none);
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
        RouterWriter(_backend, _frontendId).writePtr(new ReadyEvent, zmq::send_flags::none);
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
    DealerWriter(dealer).writePtr(new ReadyEvent, zmq::send_flags::none);

    bool quit{false};
    while (!quit) {
//        std::cout << "Worker on event begin." << std::endl;
        auto event = DealerReader(dealer).readPtr<Event>();
//        std::cout << "Worker on event type: " << int(event->type) << std::endl;
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
//            std::cout << "Worker on quit event." << std::endl;
            quit = true;
            break;
        default:
            break;
        }
//        std::cout << "Worker on event end." << std::endl;
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
    DealerReader reader(*_clientSockets[socketId]);
    if (reader.error())
        return;

    const auto requestId = reader.read<uint64_t>();
    const auto clientFuncIter = _clientFuncs.find(requestId);
    if (clientFuncIter == _clientFuncs.end())
        return;

    auto *event = new ProcessReplyEvent;
    event->status = RpcRequestStatus::DONE;
    event->clientFunc = clientFuncIter->second;
    event->replyMsg = reader.readMessage();
    sendWorkerEvent(event);
    _clientFuncs.erase(clientFuncIter);
}

void ContextPrivate::handleClientRequestTimeout(uint64_t requestId)
{
    const auto clientFuncIter = _clientFuncs.find(requestId);
    if (clientFuncIter == _clientFuncs.end())
        return;

    auto *event = new ProcessReplyEvent;
    event->status = RpcRequestStatus::DEADLINE_EXCEEDED;
    event->clientFunc = clientFuncIter->second;
    sendWorkerEvent(event);
    _clientFuncs.erase(clientFuncIter);
}

void ContextPrivate::handleServerSocket(uint64_t socketId, const ServerFunc &serverFunc)
{
    RouterReader reader(*_serverSockets[socketId]);
    if (reader.error()) {
        std::cout << "Server socket recv invalid message." << std::endl;
        return;
    }

    auto routerId = reader.routerId;
    auto requestId = reader.read<uint64_t>();
    auto requestMsg = std::move(reader.readMessage());
    if (reader.hasMore()) {
        std::cout << "Server socket recv invalid message." << std::endl;
        return;
    }

    auto *event = new ProcessRequestEvent;
    event->requestId = requestId;
    event->serverSocketId = socketId;
    event->serverFunc = serverFunc;
    event->routerId = std::move(routerId);
    event->requestMsg = std::move(requestMsg);
    sendWorkerEvent(event);
}

void ContextPrivate::handleSubSocket(uint64_t socketId, const SubFunc &subFunc)
{
    SocketdReader reader(*_subSockets[socketId]);

    auto *event = new SubTopicEvent;
    event->topicMsg = std::move(reader.readMessage());
    event->subFunc = subFunc;
    sendWorkerEvent(event);
}

void ContextPrivate::sendWorkerEvent(Event *event)
{
    RouterWriter(_backend, _workerIds[_currentWorker]).writePtr(event, zmq::send_flags::none);
    ++_currentWorker;
    if (_currentWorker == _workerIds.size()) {
        _currentWorker = 0;
    }
}

void ContextPrivate::onConnectEvent(ConnectEvent *event, const std::string &routerId)
{
    auto socket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::dealer);
    socket->connect(event->serverAddr);

    const uint64_t socketId = generateSocketId();
    _poller->addSocket(socket.get(), [this, socketId]{
        handleClientSocket(socketId);
    });
    _clientSockets.emplace(socketId, std::move(socket));

    RouterWriter(_backend, routerId).write(socketId, zmq::send_flags::none);
}

void ContextPrivate::onDisconnectEvent(DisconnectEvent *event)
{
    _poller->postCallback([this, socketId = event->socketId]{
        auto socket = std::move(_clientSockets[socketId]);
        _clientSockets.erase(socketId);
        _poller->removeSocket(socket.get());
    });
}

void ContextPrivate::onSendRequestEvent(SendRequestEvent *event)
{
    const uint64_t requestId = generateRequestId();
    _clientFuncs.emplace(requestId, event->clientFunc);

    if (event->timeoutMs > 0) {
        _poller->runCallbackAfter(event->timeoutMs, [this, requestId]{
            handleClientRequestTimeout(requestId);
        });
    }

    auto &clientSocket = _clientSockets[event->clinetSocketId];
    DealerWriter writer(*clientSocket);
    writer.write(requestId);
    writer.writeMessage(event->requestMsg, zmq::send_flags::none);
}

void ContextPrivate::onProcessReplyEvent(ProcessReplyEvent *event)
{
    event->clientFunc(event->status, event->replyMsg);
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

    RouterWriter(_backend, routerId).write(socketId, zmq::send_flags::none);
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
    zmq::message_t replyMsg;
    event->serverFunc(event->requestMsg, replyMsg);

    std::lock_guard<std::mutex> locker(_frontendMutex);
    auto *replyEvent = new SendReplyEvent;
    replyEvent->requestId = event->requestId;
    replyEvent->serverSocketId = event->serverSocketId;
    replyEvent->routerId = std::move(event->routerId);
    replyEvent->replyMsg = std::move(replyMsg);
    DealerWriter(_frontend).writePtr(replyEvent, zmq::send_flags::none);
}

void ContextPrivate::onSendReplyEvent(SendReplyEvent *event)
{
    auto &serverSocket = _serverSockets[event->serverSocketId];
    RouterWriter writer(*serverSocket, event->routerId);
    writer.write(event->requestId);
    writer.writeMessage(event->replyMsg, zmq::send_flags::none);
}

void ContextPrivate::onAddPubEvent(AddPubEvent *event, const std::string &routerId)
{
    auto pubSocket = std::make_unique<zmq::socket_t>(_ctx, zmq::socket_type::pub);
    pubSocket->bind(event->serverAddr);

    const auto socketId = generateSocketId();
    _pubSockets.emplace(socketId, std::move(pubSocket));

    RouterWriter(_backend, routerId).write(socketId, zmq::send_flags::none);
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
    subSocket->set(zmq::sockopt::subscribe, "");
    subSocket->connect(event->serverAddr);

    const auto socketId = generateSocketId();
    _poller->addSocket(subSocket.get(), [this, socketId, subFunc = event->subFunc]{
        handleSubSocket(socketId, subFunc);
    });
    _subSockets.emplace(socketId, std::move(subSocket));

    RouterWriter(_backend, routerId).write(socketId, zmq::send_flags::none);
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
    writer.writeMessage(event->topicMsg, zmq::send_flags::none);
}

void ContextPrivate::onSubTopicEvent(SubTopicEvent *event)
{
    if (event->subFunc)
        event->subFunc(event->topicMsg);
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
