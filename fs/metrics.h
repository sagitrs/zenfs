#pragma once

#include "rocksdb/env.h"
#include "utilities/trace/bytedance_metrics_reporter.h"
#include <map>

namespace ROCKSDB_NAMESPACE {

// We need to report all metrics here, don't initialize this in other places.
class OldBytedanceMetrics {
 public:
  OldBytedanceMetrics(std::shared_ptr<MetricsReporterFactory> factory, std::string bytedance_tags,
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

struct ZenFSMetrics {
public:
  ZenFSMetrics() {}
  virtual ~ZenFSMetrics() {}
public:
  void AddReporter(const std::string& label, const std::string& type = "") = 0;
  //MetricsReporter* GetReporter(const std::string& label) { return nullptr; }
  //size_t GetReporter_Debug(const std::string& label) = 0;
  void* GetReporter(const std::string& label) = 0;
  void DisposeReporter(const std::string& label) = 0;

public: // For Count Reporter:
  void AddCount(const std::string& label) = 0;
public: // For Hist Reporter:
  void AddRecord(const std::string& label, size_t size) = 0;
public:
  ZenFSMetrics(ZenFSMetrics&) = delete;
};

struct BDZenFSMetrics : public ZenFSMetrics {
public:
  enum TypeReporter { TypeHistReporter = 1, TypeCountReporter = 2, TypeNull = 3 };
  struct ReporterHandle {
    void* handle_;
    TypeReporter type_;
    ReporterHandle(HistReporterHandle* handle, TypeReporter type) : handle_(handle), type_(type) {}
    ReporterHandle(CountReporterHandle* handle, TypeReporter type) : handle_(handle), type_(type) {}
  };
public:
  std::string bytedance_tags_;
  std::shared_ptr<CurriedMetricsReporterFactory> factory_;
  std::map<std::string, ReporterHandle> reporter_map_;
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

  void AddReporter(const std::string& label, ReporterHandle* reporter) {
    if (reporter_map_.find(label) != reporter_map_.end()) {
      DisposeReporter(reporter_map_[label]);
      reporter_map_.erase(label);
    }
    reporter_map_[label] = reporter;
  }
  void AddReporter(const std::string& label, TypeReporter type) override {
    switch(type) {
    case TypeHistReporter: {
      AddReporter(label, ReporterHandle(*factory_->BuildHistReporter(fg_write_lat_label, bytedance_tags_), type);
    } break;
    case TypeCountReporter: {
      AddReporter(label, ReporterHandle(*factory_->BuildCountReporter(meta_alloc_qps_label, bytedance_tags_), type);
    } break;
    default:
      AddReporter(nullptr, TypeNull);
    }
  }
  void* GetReporter(const std::string& label) override {
    return reporter_map_[label].handle_;
  }
  void DisposeReporter(const std::string& label) override {
    // TODO : nothing.
  }
  virtual ~BDZenFSMetrics() {
    for (auto reporter : reporter_map_) 
      DisposeReporter(reporter);
  }

 public:
  BDZenFSMetrics(std::shared_ptr<MetricsReporterFactory> factory, std::string bytedance_tags, std::shared_ptr<Logger> logger):
    bytedance_tags_(bytedance_tags),
    factory_(new CurriedMetricsReporterFactory(factory, logger.get(), Env::Default())) {
      AddReporter(fg_write_lat_label, TypeHistReporter);
      AddReporter(bg_write_lat_label, TypeHistReporter);
      AddReporter(read_lat_label, TypeHistReporter);
      AddReporter(fg_sync_lat_label, TypeHistReporter);
      AddReporter(bg_sync_lat_label, TypeHistReporter);
      AddReporter(meta_alloc_lat_label, TypeHistReporter);
      AddReporter(sync_metadata_lat_label, TypeHistReporter);
      AddReporter(io_alloc_wal_lat_label, TypeHistReporter);
      AddReporter(io_alloc_wal_actual_lat_label, TypeHistReporter);
      AddReporter(io_alloc_non_wal_lat_label, TypeHistReporter);
      AddReporter(io_alloc_non_wal_actual_lat_label, TypeHistReporter);
      AddReporter(roll_lat_label, TypeHistReporter);

      AddReporter(write_qps_label, TypeCountReporter);
      AddReporter(read_qps_label, TypeCountReporter);
      AddReporter(sync_qps_label, TypeCountReporter);
      AddReporter(meta_alloc_qps_label, TypeCountReporter);
      AddReporter(io_alloc_qps_label, TypeCountReporter);
      AddReporter(roll_qps_label, TypeCountReporter);
      AddReporter(write_throughput_label, TypeCountReporter);
      AddReporter(roll_throughput_label, TypeCountReporter);

      AddReporter(active_zones_label, TypeHistReporter);
      AddReporter(open_zones_label, TypeHistReporter);
      AddReporter(zbd_free_space_label, TypeHistReporter);
      AddReporter(zbd_used_space_label, TypeHistReporter);
      AddReporter(zbd_reclaimable_space_label, TypeHistReporter);
      AddReporter(zbd_resetable_zones_label, TypeHistReporter);
    }

};

}  // namespace ROCKSDB_NAMESPACE
