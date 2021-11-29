#pragma once
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

struct ZenFSMetrics {
public:
  ZenFSMetrics() {}
  virtual ~ZenFSMetrics() {}
public:
  virtual void AddReporter(uint32_t label, uint32_t type = 0) = 0;
  virtual void Report(uint32_t label, size_t value, uint32_t type_check = 0) = 0;
public: // sugar
  virtual void ReportQPS(uint32_t label, size_t qps) = 0;
  virtual void ReportThroughput(uint32_t label, size_t throughput) = 0;
  virtual void ReportLatency(uint32_t label, size_t latency) = 0;
  virtual void ReportGeneral(uint32_t label, size_t data) = 0;
  // and more
};

struct NoZenFSMetrics : public ZenFSMetrics {
  NoZenFSMetrics() : ZenFSMetrics() {}
  virtual ~NoZenFSMetrics() {}
public:
  virtual void AddReporter(uint32_t label, uint32_t type = 0) override {}
  virtual void Report(uint32_t label, size_t value, uint32_t type_check = 0) override {}
public:
  virtual void ReportQPS(uint32_t label, size_t qps) override {}
  virtual void ReportThroughput(uint32_t label, size_t throughput) override {}
  virtual void ReportLatency(uint32_t label, size_t latency) override {}
  virtual void ReportGeneral(uint32_t label, size_t data) override {}
};

struct ZenFSMetricsLatencyGuard {
  std::shared_ptr<ZenFSMetrics> metrics_;
  uint32_t label_;
  Env* env_;
  uint64_t begin_time_micro_;

  ZenFSMetricsLatencyGuard(std::shared_ptr<ZenFSMetrics> metrics, uint32_t label, Env* env)
  : metrics_(metrics), label_(label), env_(env), begin_time_micro_(GetTime()) {}

  virtual ~ZenFSMetricsLatencyGuard() {
    uint64_t end_time_micro_ = GetTime();
    assert(end_time_micro_ > begin_time_micro_);
    metrics_->ReportLatency(label_, Report(end_time_micro_ - begin_time_micro_)); 
  }
  
  virtual uint64_t GetTime() { return env_->NowMicros(); }
  virtual size_t Report(size_t time) { return time; }
};

}  // namespace ROCKSDB_NAMESPACE
