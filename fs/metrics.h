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
  virtual void AddReporter(const std::string& label, const std::string& type = "") = 0;
public: // For Record Reporter:
  virtual void AddRecord(const std::string& label, size_t value) = 0;
  virtual void* GetRecord(const std::string& label) = 0;
};

struct NoZenFSMetrics : public ZenFSMetrics {
  NoZenFSMetrics() : ZenFSMetrics() {}
public:
  virtual void AddReporter(const std::string& label, const std::string& type = "") override {
    // Do nothing.
  }
  virtual void AddRecord(const std::string& label, size_t value) override {
    // Do nothing.
  }
  virtual void* GetRecord(const std::string& label) override {
    return nullptr;
    // Do nothing.
  }
};

struct BDZenFSMetrics : public ZenFSMetrics {
public:
  struct Reporter {
    void* handle_;
    std::string type_;
    Reporter(void* handle = nullptr, const std::string& type = "") 
      : handle_(handle), type_(type) {}
  };
public:
  std::string bytedance_tags_;
  std::shared_ptr<CurriedMetricsReporterFactory> factory_;
  std::map<std::string, Reporter> reporter_map_;
public:
  virtual void AddReporter(const std::string& label, const Reporter& reporter) {
    assert(reporter_map_.find(label) == reporter_map_.end());
    reporter_map_[label] = reporter;
  }
  virtual void AddReporter(const std::string& label, const std::string& type_string = "") override {
    if (type_string == type_hist_reporter) {
      AddReporter(label, Reporter(factory_->BuildHistReporter(fg_write_lat_label, bytedance_tags_), type_string));
    } else if (type_string == type_count_reporter) {
      AddReporter(label, Reporter(factory_->BuildCountReporter(meta_alloc_qps_label, bytedance_tags_), type_string));
    } else {
      assert(false);
      AddReporter(label, Reporter(nullptr, ""));
    }
  }

public: // For Record Reporter:
  virtual void AddRecord(const std::string& label, size_t size) override {
    assert(reporter_map_.find(label) != reporter_map_.end());
    Reporter rep = reporter_map_[label];
    if (rep.type_ == type_hist_reporter) {
      reinterpret_cast<HistReporterHandle*>(rep.handle_)->AddRecord(size);
    } else if (rep.type_ == type_count_reporter) {
      reinterpret_cast<CountReporterHandle*>(rep.handle_)->AddCount(size);
    } else {
      assert(false);
    }
  }
  virtual void* GetRecord(const std::string& label) override {
    assert(false);
    return reporter_map_[label].handle_;
  }
    
  virtual ~BDZenFSMetrics() {}

 public:
  BDZenFSMetrics(std::shared_ptr<MetricsReporterFactory> factory, std::string bytedance_tags, std::shared_ptr<Logger> logger):
    ZenFSMetrics(),
    bytedance_tags_(bytedance_tags),
    factory_(new CurriedMetricsReporterFactory(factory, logger.get(), Env::Default())) {
      AddReporter(fg_write_lat_label, type_hist_reporter);
      AddReporter(bg_write_lat_label, type_hist_reporter);
      AddReporter(read_lat_label, type_hist_reporter);
      AddReporter(fg_sync_lat_label, type_hist_reporter);
      AddReporter(bg_sync_lat_label, type_hist_reporter);
      AddReporter(meta_alloc_lat_label, type_hist_reporter);
      AddReporter(sync_metadata_lat_label, type_hist_reporter);
      AddReporter(io_alloc_wal_lat_label, type_hist_reporter);
      AddReporter(io_alloc_wal_actual_lat_label, type_hist_reporter);
      AddReporter(io_alloc_non_wal_lat_label, type_hist_reporter);
      AddReporter(io_alloc_non_wal_actual_lat_label, type_hist_reporter);
      AddReporter(roll_lat_label, type_hist_reporter);

      AddReporter(write_qps_label, type_count_reporter);
      AddReporter(read_qps_label, type_count_reporter);
      AddReporter(sync_qps_label, type_count_reporter);
      AddReporter(meta_alloc_qps_label, type_count_reporter);
      AddReporter(io_alloc_qps_label, type_count_reporter);
      AddReporter(roll_qps_label, type_count_reporter);
      AddReporter(write_throughput_label, type_count_reporter);
      AddReporter(roll_throughput_label, type_count_reporter);

      AddReporter(active_zones_label, type_hist_reporter);
      AddReporter(open_zones_label, type_hist_reporter);
      AddReporter(zbd_free_space_label, type_hist_reporter);
      AddReporter(zbd_used_space_label, type_hist_reporter);
      AddReporter(zbd_reclaimable_space_label, type_hist_reporter);
      AddReporter(zbd_resetable_zones_label, type_hist_reporter);
    }

public:
  std::string type_hist_reporter = "zenfs_type_hist_reporter";
  std::string type_count_reporter = "zenfs_type_count_reporter";

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
};

}  // namespace ROCKSDB_NAMESPACE
