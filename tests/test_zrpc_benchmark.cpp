#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "zrpc/Context.h"
#include "zrpc/Channel.h"
#include "zrpc/Server.h"

namespace {

constexpr const char *kServerAddr = "tcp://127.0.0.1:9989";
constexpr const char *kServiceName = "PerfService";
constexpr const char *kMethodName = "echo";

constexpr size_t kSmallPayloadSize = 64;
constexpr size_t kMediumPayloadSize = 1024 * 1024;       // 1 MB
constexpr size_t kLargePayloadSize = 10 * 1024 * 1024;     // 10 MB
constexpr size_t kXLargePayloadSize = 30 * 1024 * 1024;    // 30 MB

constexpr int kWarmupIterations = 5;

class StopWatch
{
public:
    void restart() { _start = std::chrono::steady_clock::now(); }

    int64_t elapsedUs() const
    {
        const auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now - _start).count();
    }

private:
    std::chrono::steady_clock::time_point _start{std::chrono::steady_clock::now()};
};

class PerfService : public zrpc::Service
{
public:
    PerfService() : zrpc::Service(kServiceName)
    {
        addMethod(kMethodName, [](const zrpc::PayloadView &request, zrpc::Payload &reply) {
            if (!request.views.empty())
                reply.emplace_back(request.views[0]);
        });
    }
};

struct BenchmarkConfig
{
    const char *label;
    size_t payloadSize;
    int iterations;
};

struct BenchmarkStats
{
    size_t sampleCount{0};
    int64_t minUs{0};
    int64_t maxUs{0};
    double avgUs{0.0};
    int64_t p50Us{0};
    int64_t p95Us{0};
    int64_t p99Us{0};
};

struct BenchmarkResult
{
    BenchmarkConfig config;
    BenchmarkStats stats;
    bool ok{false};
};

std::string formatPayloadSize(size_t bytes)
{
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (bytes < 1024) {
        oss << bytes << " B";
    } else if (bytes < 1024 * 1024) {
        oss << static_cast<double>(bytes) / 1024.0 << " KB";
    } else {
        oss << static_cast<double>(bytes) / (1024.0 * 1024.0) << " MB";
    }
    return oss.str();
}

std::string formatLatency(int64_t us)
{
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(us < 10000 ? 0 : 2);
    if (us < 1000) {
        oss << us << " us";
    } else if (us < 1000000) {
        oss << static_cast<double>(us) / 1000.0 << " ms";
    } else {
        oss << static_cast<double>(us) / 1000000.0 << " s";
    }
    return oss.str();
}

double throughputMbps(size_t payloadBytes, double avgUs)
{
    if (avgUs <= 0.0) {
        return 0.0;
    }
    // Round-trip transfers request + reply.
    const double bytesPerSec = payloadBytes * 2.0 * 1e6 / avgUs;
    return bytesPerSec / (1024.0 * 1024.0);
}

BenchmarkStats computeStats(std::vector<int64_t> samples)
{
    BenchmarkStats stats;
    if (samples.empty()) {
        return stats;
    }

    std::sort(samples.begin(), samples.end());

    stats.sampleCount = samples.size();
    stats.minUs = samples.front();
    stats.maxUs = samples.back();
    stats.avgUs = static_cast<double>(std::accumulate(samples.begin(), samples.end(), int64_t{0}))
                  / static_cast<double>(samples.size());

    auto percentile = [&](double p) {
        const auto idx = static_cast<size_t>(p * (samples.size() - 1));
        return samples[idx];
    };

    stats.p50Us = percentile(0.50);
    stats.p95Us = percentile(0.95);
    stats.p99Us = percentile(0.99);
    return stats;
}

void printReportHeader()
{
    std::cout << "========================================\n"
              << "  zrpc RPC latency benchmark\n"
              << "========================================\n"
              << "  method   : " << kServiceName << "." << kMethodName << "\n"
              << "  endpoint : " << kServerAddr << "\n"
              << "  warmup   : " << kWarmupIterations << " iterations\n"
              << std::endl;
}

void printSummaryTable(const std::vector<BenchmarkResult> &results)
{
    constexpr int kLabelW = 8;
    constexpr int kPayloadW = 10;
    constexpr int kCountW = 6;
    constexpr int kLatencyW = 10;
    constexpr int kThroughputW = 12;

    auto printRow = [&](const char *label, const char *payload, int iterations,
                        int64_t minUs, double avgUs, int64_t p50Us, int64_t p95Us, int64_t p99Us,
                        int64_t maxUs, double mbps) {
        std::cout << "  " << std::left << std::setw(kLabelW) << label
                  << std::setw(kPayloadW) << payload
                  << std::right << std::setw(kCountW) << iterations
                  << std::setw(kLatencyW) << formatLatency(minUs)
                  << std::setw(kLatencyW) << formatLatency(static_cast<int64_t>(avgUs))
                  << std::setw(kLatencyW) << formatLatency(p50Us)
                  << std::setw(kLatencyW) << formatLatency(p95Us)
                  << std::setw(kLatencyW) << formatLatency(p99Us)
                  << std::setw(kLatencyW) << formatLatency(maxUs)
                  << std::fixed << std::setprecision(1) << std::setw(kThroughputW) << mbps
                  << std::endl;
    };

    const int tableWidth = kLabelW + kPayloadW + kCountW + kLatencyW * 6 + kThroughputW + 2;

    std::cout << "  latency: round-trip time per RPC call\n"
              << "  throughput: request + reply / avg latency\n"
              << std::endl;
    std::cout << "  " << std::left << std::setw(kLabelW) << "Label"
              << std::setw(kPayloadW) << "Payload"
              << std::right << std::setw(kCountW) << "Count"
              << std::setw(kLatencyW) << "Min"
              << std::setw(kLatencyW) << "Avg"
              << std::setw(kLatencyW) << "P50"
              << std::setw(kLatencyW) << "P95"
              << std::setw(kLatencyW) << "P99"
              << std::setw(kLatencyW) << "Max"
              << std::setw(kThroughputW) << "MB/s"
              << std::endl;
    std::cout << "  " << std::string(tableWidth, '-') << std::endl;

    for (const auto &result : results) {
        const auto &config = result.config;
        const auto &stats = result.stats;
        const auto payload = formatPayloadSize(config.payloadSize);

        if (!result.ok || stats.sampleCount == 0) {
            std::cout << "  " << std::left << std::setw(kLabelW) << config.label
                      << std::setw(kPayloadW) << payload
                      << "  FAILED" << std::endl;
            continue;
        }

        printRow(config.label, payload.c_str(), static_cast<int>(stats.sampleCount),
                 stats.minUs, stats.avgUs, stats.p50Us, stats.p95Us, stats.p99Us, stats.maxUs,
                 throughputMbps(config.payloadSize, stats.avgUs));
    }

    std::cout << std::endl;
}

BenchmarkResult runBenchmark(zrpc::Stub &stub, const BenchmarkConfig &config)
{
    std::string payload(config.payloadSize, 'x');
    std::vector<int64_t> samples;
    samples.reserve(static_cast<size_t>(config.iterations));

    BenchmarkResult benchResult{config, {}, false};
    StopWatch watch;

    for (int i = 0; i < kWarmupIterations; ++i) {
        payload.assign(config.payloadSize, 'x');
        auto result = stub.callMethod(kServiceName, kMethodName, zrpc::Payload{std::move(payload)});
        if (!result.ok()) {
            std::cerr << "[" << config.label << "] warmup failed: "
                      << static_cast<int>(result.errorCode) << " " << result.errorMsg << std::endl;
            return benchResult;
        }
    }

    for (int i = 0; i < config.iterations; ++i) {
        payload.assign(config.payloadSize, 'x');
        watch.restart();
        auto result = stub.callMethod(kServiceName, kMethodName, zrpc::Payload{std::move(payload)});
        samples.push_back(watch.elapsedUs());

        if (!result.ok()) {
            std::cerr << "[" << config.label << "] benchmark failed at iteration " << i << ": "
                      << static_cast<int>(result.errorCode) << " " << result.errorMsg << std::endl;
            break;
        }
        const auto replySize = result.payload.views.empty() ? 0 : result.payload.views[0].size();
        if (replySize != config.payloadSize) {
            std::cerr << "[" << config.label << "] unexpected reply size: " << replySize
                      << ", expected: " << config.payloadSize << std::endl;
            break;
        }
    }

    benchResult.stats = computeStats(std::move(samples));
    benchResult.ok = benchResult.stats.sampleCount == static_cast<size_t>(config.iterations);
    return benchResult;
}

void runServer(std::shared_ptr<zrpc::Context> context)
{
    PerfService service;
    zrpc::Server server(context);
    server.registerService(&service);
    server.bind(kServerAddr);

    while (true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
    }
}

} // namespace

int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv;

    auto context = std::make_shared<zrpc::Context>();
    std::thread serverThread(runServer, context);
    serverThread.detach();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto channel = std::make_shared<zrpc::Channel>(context);
    channel->connect(kServerAddr);
    zrpc::Stub stub(channel);

    const BenchmarkConfig configs[] = {
        {"small", kSmallPayloadSize, 1000},
        {"medium", kMediumPayloadSize, 100},
        {"large", kLargePayloadSize, 20},
        {"large(30m)", kXLargePayloadSize, 10},
    };

    printReportHeader();

    std::vector<BenchmarkResult> results;
    results.reserve(std::size(configs));
    for (const auto &config : configs) {
        std::cout << "  running [" << config.label << "] "
                  << formatPayloadSize(config.payloadSize)
                  << ", " << config.iterations << " iterations..." << std::endl;
        results.push_back(runBenchmark(stub, config));
    }

    std::cout << std::endl;
    printSummaryTable(results);
    return 0;
}
