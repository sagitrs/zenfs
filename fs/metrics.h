#pragma once

#include "rocksdb/env.h"
#include "utilities/trace/bytedance_metrics_reporter.h"

namespace ROCKSDB_NAMESPACE {

// We need to report all metrics here, don't initialize this in other places.
class BytedanceMetrics {
 public:
  BytedanceMetrics(std::shared_ptr<MetricsReporterFactory> factory, std::string bytedance_tags,
                   std::shared_ptr<Logger> logger)
      :

        bytedance_tags_(bytedance_tags),
        factory_(new CurriedMetricsReporterFactory(factory, logger.get(), Env::Default())),
        write_latency_reporter_(*factory_->BuildHistReporter(write_latency_metric_name, bytedance_tags_)),
        read_latency_reporter_(*factory_->BuildHistReporter(read_latency_metric_name, bytedance_tags_)),
        fg_sync_latency_reporter_(*factory_->BuildHistReporter(fg_sync_latency_metric_name, bytedance_tags_)),
        bg_sync_latency_reporter_(*factory_->BuildHistReporter(bg_sync_latency_metric_name, bytedance_tags_)),
        meta_alloc_latency_reporter_(*factory_->BuildHistReporter(meta_alloc_latency_metric_name, bytedance_tags_)),
        io_alloc_wal_latency_reporter_(*factory_->BuildHistReporter(io_alloc_wal_latency_metric_name, bytedance_tags_)),
        io_alloc_non_wal_latency_reporter_(
            *factory_->BuildHistReporter(io_alloc_non_wal_latency_metric_name, bytedance_tags_)),
        io_alloc_wal_actual_latency_reporter_(
            *factory_->BuildHistReporter(io_alloc_wal_actual_latency_metric_name, bytedance_tags_)),
        io_alloc_non_wal_actual_latency_reporter_(
            *factory_->BuildHistReporter(io_alloc_non_wal_actual_latency_metric_name, bytedance_tags_)),
        roll_latency_reporter_(*factory_->BuildHistReporter(roll_latency_metric_name, bytedance_tags_)),
        write_qps_reporter_(*factory_->BuildCountReporter(write_qps_metric_name, bytedance_tags_)),
        read_qps_reporter_(*factory_->BuildCountReporter(read_qps_metric_name, bytedance_tags_)),
        sync_qps_reporter_(*factory_->BuildCountReporter(sync_qps_metric_name, bytedance_tags_)),
        meta_alloc_qps_reporter_(*factory_->BuildCountReporter(meta_alloc_qps_metric_name, bytedance_tags_)),
        io_alloc_qps_reporter_(*factory_->BuildCountReporter(io_alloc_qps_metric_name, bytedance_tags_)),
        roll_qps_reporter_(*factory_->BuildCountReporter(roll_qps_metric_name, bytedance_tags_)),
        write_throughput_reporter_(*factory_->BuildCountReporter(write_throughput_metric_name, bytedance_tags_)),
        roll_throughput_reporter_(*factory_->BuildCountReporter(roll_throughput_metric_name, bytedance_tags_)),
        active_zones_reporter_(*factory_->BuildHistReporter(active_zones_metric_name, bytedance_tags_)),
        open_zones_reporter_(*factory_->BuildHistReporter(open_zones_metric_name, bytedance_tags_)),
        zbd_free_space_reporter_(*factory_->BuildHistReporter(zbd_free_space_metric_name, bytedance_tags_)),
        zbd_used_space_reporter_(*factory_->BuildHistReporter(zbd_used_space_metric_name, bytedance_tags_)),
        zbd_reclaimable_space_reporter_(
            *factory_->BuildHistReporter(zbd_reclaimable_space_metric_name, bytedance_tags_)),
        zbd_total_extent_length_reporter_(
            *factory_->BuildHistReporter(zbd_total_extent_length_metric_name, bytedance_tags_)) {}

 public:
  std::string write_latency_metric_name = "zenfs_write_latency";
  std::string read_latency_metric_name = "zenfs_read_latency";
  std::string fg_sync_latency_metric_name = "fg_zenfs_sync_latency";
  std::string bg_sync_latency_metric_name = "bg_zenfs_sync_latency";
  std::string io_alloc_wal_latency_metric_name = "zenfs_io_alloc_wal_latency";
  std::string io_alloc_non_wal_latency_metric_name = "zenfs_io_alloc_non_wal_latency";
  std::string io_alloc_wal_actual_latency_metric_name = "zenfs_io_alloc_wal_actual_latency";
  std::string io_alloc_non_wal_actual_latency_metric_name = "zenfs_io_alloc_non_wal_actual_latency";
  std::string meta_alloc_latency_metric_name = "zenfs_meta_alloc_latency";
  std::string roll_latency_metric_name = "zenfs_roll_latency";

  std::string write_qps_metric_name = "zenfs_write_qps";
  std::string read_qps_metric_name = "zenfs_read_qps";
  std::string sync_qps_metric_name = "zenfs_sync_qps";
  std::string io_alloc_qps_metric_name = "zenfs_io_alloc_qps";
  std::string meta_alloc_qps_metric_name = "zenfs_meta_alloc_qps";
  std::string roll_qps_metric_name = "zenfs_roll_qps";

  std::string write_throughput_metric_name = "zenfs_write_throughput";
  std::string roll_throughput_metric_name = "zenfs_roll_throughput";

  std::string active_zones_metric_name = "zenfs_active_zones";
  std::string open_zones_metric_name = "zenfs_open_zones";
  std::string zbd_free_space_metric_name = "zenfs_free_space";
  std::string zbd_used_space_metric_name = "zenfs_used_space";
  std::string zbd_reclaimable_space_metric_name = "zenfs_reclaimable_space";
  std::string zbd_total_extent_length_metric_name = "zenfs_total_extent_length";

 public:
  std::string bytedance_tags_;
  std::shared_ptr<CurriedMetricsReporterFactory> factory_;

  using LatencyReporter = HistReporterHandle &;
  LatencyReporter write_latency_reporter_;
  LatencyReporter read_latency_reporter_;
  LatencyReporter fg_sync_latency_reporter_;
  LatencyReporter bg_sync_latency_reporter_;
  LatencyReporter meta_alloc_latency_reporter_;
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
  DataReporter zbd_total_extent_length_reporter_;
};

}  // namespace ROCKSDB_NAMESPACE
