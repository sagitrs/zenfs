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

enum ZenFSMetricsHistograms : uint32_t {
  ZENFS_HISTOGRAM_ENUM_MIN, 

  ZENFS_FG_WRITE_LATENCY,
  ZENFS_BG_WRITE_LATENCY,
  
  ZENFS_READ_LATENCY,
  ZENFS_FG_SYNC_LATENCY,
  ZENFS_BG_SYNC_LATENCY,
  ZENFS_IO_ALLOC_WAL_LATENCY,
  ZENFS_IO_ALLOC_NON_WAL_LATENCY,
  ZENFS_IO_ALLOC_WAL_ACTUAL_LATENCY,
  ZENFS_IO_ALLOC_NON_WAL_ACTUAL_LATENCY,
  ZENFS_META_ALLOC_LATENCY,
  ZENFS_METADATA_SYNC_LATENCY,
  ZENFS_ROLL_LATENCY,

  ZENFS_WRITE_QPS,
  ZENFS_READ_QPS,
  ZENFS_SYNC_QPS,
  ZENFS_IO_ALLOC_QPS,
  ZENFS_META_ALLOC_QPS,
  ZENFS_ROLL_QPS,

  ZENFS_WRITE_THROUGHPUT,
  ZENFS_ROLL_THROUGHPUT,
  
  ZENFS_ACTIVE_ZONES,
  ZENFS_OPEN_ZONES,
  ZENFS_FREE_SPACE,
  ZENFS_USED_SPACE,
  ZENFS_RECLAIMABLE_SPACE,
  ZENFS_RESETABLE_ZONES,

  ZENFS_HISTOGRAM_ENUM_MAX,
};

enum ZenFSMetricsReporterType : uint32_t {
  ZENFS_REPORTER_TYPE_WITHOUT_CHECK = 0,
  ZENFS_REPORTER_TYPE_GENERAL,
  ZENFS_REPORTER_TYPE_LATENCY,    
  ZENFS_REPORTER_TYPE_QPS,
  ZENFS_REPORTER_TYPE_THROUGHPUT
};

//--------------old version-----------
class BytedanceMetrics {
 public:
  BytedanceMetrics(std::shared_ptr<MetricsReporterFactory> factory, std::string bytedance_tags,
                   std::shared_ptr<Logger> logger):
        bytedance_tags_(bytedance_tags),
        factory_(new CurriedMetricsReporterFactory(factory, logger.get(), Env::Default())),

        fg_write_latency_reporter_(*factory_->BuildHistReporter(fg_write_lat_label, bytedance_tags_)),
        bg_write_latency_reporter_(*factory_->BuildHistReporter(bg_write_lat_label, bytedance_tags_)),
        read_latency_reporter_(*factory_->BuildHistReporter(read_lat_label, bytedance_tags_)),
        fg_sync_latency_reporter_(*factory_->BuildHistReporter(fg_sync_lat_label, bytedance_tags_)),
        bg_sync_latency_reporter_(*factory_->BuildHistReporter(bg_sync_lat_label, bytedance_tags_)),
        meta_alloc_latency_reporter_(*factory_->BuildHistReporter(meta_alloc_lat_label, bytedance_tags_)),
        sync_metadata_reporter_(*factory_->BuildHistReporter(sync_metadata_lat_label, bytedance_tags_)),
        io_alloc_wal_latency_reporter_(*factory_->BuildHistReporter(io_alloc_wal_lat_label, bytedance_tags_)),
        io_alloc_wal_actual_latency_reporter_(
            *factory_->BuildHistReporter(io_alloc_wal_actual_lat_label, bytedance_tags_)),
        io_alloc_non_wal_latency_reporter_(
            *factory_->BuildHistReporter(io_alloc_non_wal_lat_label, bytedance_tags_)),
        io_alloc_non_wal_actual_latency_reporter_(
            *factory_->BuildHistReporter(io_alloc_non_wal_actual_lat_label, bytedance_tags_)),
        roll_latency_reporter_(*factory_->BuildHistReporter(roll_lat_label, bytedance_tags_)),
        write_qps_reporter_(*factory_->BuildCountReporter(write_qps_label, bytedance_tags_)),
        read_qps_reporter_(*factory_->BuildCountReporter(read_qps_label, bytedance_tags_)),
        sync_qps_reporter_(*factory_->BuildCountReporter(sync_qps_label, bytedance_tags_)),
        meta_alloc_qps_reporter_(*factory_->BuildCountReporter(meta_alloc_qps_label, bytedance_tags_)),
        io_alloc_qps_reporter_(*factory_->BuildCountReporter(io_alloc_qps_label, bytedance_tags_)),
        roll_qps_reporter_(*factory_->BuildCountReporter(roll_qps_label, bytedance_tags_)),
        write_throughput_reporter_(*factory_->BuildCountReporter(write_throughput_label, bytedance_tags_)),
        roll_throughput_reporter_(*factory_->BuildCountReporter(roll_throughput_label, bytedance_tags_)),
        active_zones_reporter_(*factory_->BuildHistReporter(active_zones_label, bytedance_tags_)),
        open_zones_reporter_(*factory_->BuildHistReporter(open_zones_label, bytedance_tags_)),
        zbd_free_space_reporter_(*factory_->BuildHistReporter(zbd_free_space_label, bytedance_tags_)),
        zbd_used_space_reporter_(*factory_->BuildHistReporter(zbd_used_space_label, bytedance_tags_)),
        zbd_reclaimable_space_reporter_(
            *factory_->BuildHistReporter(zbd_reclaimable_space_label, bytedance_tags_)),
        zbd_resetable_zones_reporter_(*factory_->BuildHistReporter(zbd_resetable_zones_label, bytedance_tags_)) {}

 public:
  std::string fg_write_lat_label = "zenfs_fg_write_latency";
  std::string bg_write_lat_label = "zenfs_bg_write_latency";

  std::string read_lat_label = "zenfs_read_latency";
  std::string fg_sync_lat_label = "fg_zenfs_sync_latency";
  std::string bg_sync_lat_label = "bg_zenfs_sync_latency";
  std::string io_alloc_wal_lat_label = "zenfs_io_alloc_wal_latency";
  std::string io_alloc_non_wal_lat_label = "zenfs_io_alloc_non_wal_latency";
  std::string io_alloc_wal_actual_lat_label = "zenfs_io_alloc_wal_actual_latency";
  std::string io_alloc_non_wal_actual_lat_label = "zenfs_io_alloc_non_wal_actual_latency";
  std::string meta_alloc_lat_label = "zenfs_meta_alloc_latency";
  std::string sync_metadata_lat_label = "zenfs_metadata_sync_latency";
  std::string roll_lat_label = "zenfs_roll_latency";

  std::string write_qps_label = "zenfs_write_qps";
  std::string read_qps_label = "zenfs_read_qps";
  std::string sync_qps_label = "zenfs_sync_qps";
  std::string io_alloc_qps_label = "zenfs_io_alloc_qps";
  std::string meta_alloc_qps_label = "zenfs_meta_alloc_qps";
  std::string roll_qps_label = "zenfs_roll_qps";

  std::string write_throughput_label = "zenfs_write_throughput";
  std::string roll_throughput_label = "zenfs_roll_throughput";

  std::string active_zones_label = "zenfs_active_zones";
  std::string open_zones_label = "zenfs_open_zones";
  std::string zbd_free_space_label = "zenfs_free_space";
  std::string zbd_used_space_label = "zenfs_used_space";
  std::string zbd_reclaimable_space_label = "zenfs_reclaimable_space";
  std::string zbd_resetable_zones_label = "zenfs_resetable_zones";

 public:
  std::string bytedance_tags_;
  std::shared_ptr<CurriedMetricsReporterFactory> factory_;

  using LatencyReporter = HistReporterHandle &;

  // All reporters
  LatencyReporter fg_write_latency_reporter_;
  LatencyReporter bg_write_latency_reporter_;

  LatencyReporter read_latency_reporter_;
  LatencyReporter fg_sync_latency_reporter_;
  LatencyReporter bg_sync_latency_reporter_;
  LatencyReporter meta_alloc_latency_reporter_;
  LatencyReporter sync_metadata_reporter_;
  LatencyReporter io_alloc_wal_latency_reporter_;
  LatencyReporter io_alloc_wal_actual_latency_reporter_;
  LatencyReporter io_alloc_non_wal_latency_reporter_;
  LatencyReporter io_alloc_non_wal_actual_latency_reporter_;
  LatencyReporter roll_latency_reporter_;

  using QPSReporter = CountReporterHandle &;
  QPSReporter write_qps_reporter_;
  QPSReporter read_qps_reporter_;
  QPSReporter sync_qps_reporter_;
  QPSReporter meta_alloc_qps_reporter_;
  QPSReporter io_alloc_qps_reporter_;
  QPSReporter roll_qps_reporter_;

  using ThroughputReporter = CountReporterHandle &;
  ThroughputReporter write_throughput_reporter_;
  ThroughputReporter roll_throughput_reporter_;

  using DataReporter = HistReporterHandle &;
  DataReporter active_zones_reporter_;
  DataReporter open_zones_reporter_;
  DataReporter zbd_free_space_reporter_;
  DataReporter zbd_used_space_reporter_;
  DataReporter zbd_reclaimable_space_reporter_;
  DataReporter zbd_resetable_zones_reporter_;
};

}  // namespace ROCKSDB_NAMESPACE
