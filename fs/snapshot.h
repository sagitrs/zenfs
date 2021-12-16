// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>

#include "io_zenfs.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

// These are three Snapshot classes to capture real-time information from ZenFS
// for use by the upper layer algorithms. The three Snapshots will capture
// information from Zone, ZoneFile, and ZoneExtent respectively. If you plan to
// modify the variables of these three classes, please make sure that they have
// a public interface for copying and that the interface of the Snapshot classes
// are still logically correct after modification.
struct ZenFSSnapshotOptions {
  struct ZBDSnapshotOptions {
    bool enabled_ = 1;
    bool get_free_space_ = 1;
    bool get_used_space_ = 1;
    bool get_reclaimable_space_ = 1;
  } zbd_;
  struct ZoneSnapshotOptions {
    bool enabled_ = 1;
    bool write_position_ = 1;
    bool start_position_ = 1;
    bool id_ = 1;
    bool remaining_capacity_ = 1;
    bool used_capacity_ = 1;
    bool max_capacity_ = 1;
    bool get_migrate_score_ = 1;
  } zone_;
  struct ZoneFileSnapshotOptions {
    bool enabled_ = 1;
    bool id_ = 1;
    bool filename_ = 1;
  } zone_file_;
  struct ZoneExtentSnapshotOptions {
    bool enabled_ = 1;
    bool start_ = 1;
    bool length_ = 1;
    bool zone_id_ = 1;
  } zone_extent_;

  bool trigger_report_ = 1;
  bool as_lock_free_as_possible_ = 1;
};

class ZBDSnapshot {
 private:
  uint64_t free_space_;
  uint64_t used_space_;
  uint64_t reclaimable_space_;

 public:
  ZBDSnapshot() = default;
  ZBDSnapshot(const ZBDSnapshot&) = default;
  ZBDSnapshot(ZonedBlockDevice& zbd, const ZenFSSnapshotOptions& options)
      : free_space_(), used_space_(), reclaimable_space_() {
    if (options.zbd_.enabled_) {
      if (options.zbd_.get_free_space_) free_space_ = zbd.GetFreeSpace();
      if (options.zbd_.get_used_space_) used_space_ = zbd.GetUsedSpace();
      if (options.zbd_.get_reclaimable_space_)
        reclaimable_space_ = zbd.GetReclaimableSpace();
    }
  }
  uint64_t GetFreeSpace() const { return free_space_; }
  uint64_t GetUsedSpace() const { return used_space_; }
  uint64_t GetReclaimableSpace() const { return reclaimable_space_; }
};

struct ZoneExtentSnapshot {
 private:
  uint64_t start_;
  uint64_t length_;
  uint64_t zone_start_;
 public:
  uint64_t Start() const { return start_; }
  uint64_t Length() const { return length_; }
  uint64_t ZoneID() const { return zone_start_; }

 public:
  ZoneExtentSnapshot(const ZoneExtent& extent,
                     const ZenFSSnapshotOptions& options)
      : start_(), length_(), zone_start_() {
    if (options.zone_extent_.enabled_) {
      if (options.zone_extent_.start_) start_ = extent.start_;
      if (options.zone_extent_.length_) length_ = extent.length_;
      if (options.zone_extent_.zone_id_) zone_start_ = extent.zone_->start_;
    }
  }

 private:
  bool zone_view_enabled_ = 0;
  uint64_t file_id_ = 0;
 public:
  void SetFileID(uint64_t file_id) { zone_view_enabled_ = 1; file_id_ = file_id; }
  uint64_t FileID() const { return file_id_; }
};

struct ZoneFileSnapshot {
 private:
  uint64_t file_id_;
  std::string filename_;
  std::vector<ZoneExtentSnapshot> extent_;

 public:
  ZoneFileSnapshot(ZoneFile& file, const ZenFSSnapshotOptions& options)
      : file_id_(), filename_(), extent_() {
    if (options.zone_file_.enabled_) {
      if (options.zone_file_.id_) file_id_ = file.GetID();
      if (options.zone_file_.filename_) filename_ = file.GetFilename();
    }
    if (options.zone_extent_.enabled_)
      for (ZoneExtent* const& extent : file.GetExtents())
        extent_.emplace_back(*extent, options);
  }

  uint64_t FileID() const { return file_id_; }
  const std::string& Filename() const { return filename_; }
  const std::vector<ZoneExtentSnapshot>& Extent() const { return extent_; }
  std::vector<ZoneExtentSnapshot>& EditableExtent() { return extent_; }
};

class ZoneSnapshot {
 private:
  uint64_t start_;
  uint64_t wp_;

  uint64_t capacity_;
  uint64_t used_capacity_;
  uint64_t max_capacity_;
 public:
  uint64_t ID() const { return start_; }
  uint64_t RemainingCapacity() const { return capacity_; }
  uint64_t UsedCapacity() const { return used_capacity_; }
  uint64_t MaxCapacity() const { return max_capacity_; }
  uint64_t ReclaimableCapacity() const { return capacity_ - used_capacity_ - max_capacity_; }
  uint64_t StartPosition() const { return start_; }
  uint64_t WritePosition() const { return wp_; }

 private: // support zone-view.
  bool zone_view_enabled_ = 0;
  bool score_calculated_ = 0;
  int64_t migrate_score_ = INT64_MIN;
  std::vector<ZoneExtentSnapshot> file_extent_;
 public: 
  void AddFileExtent(const ZoneExtentSnapshot& extent) {
    zone_view_enabled_ = 1;
    file_extent_.emplace_back(extent);
  }
  const std::vector<ZoneExtentSnapshot>& GetFileExtent() const { 
    if (zone_view_enabled_)
      return file_extent_;
    else
      return file_extent_;  // shall be an error. 
  }
  bool ScoreCalculated() const { return score_calculated_; }
  void SetScore(int64_t score) { 
    migrate_score_ = score; 
    score_calculated_ = 1; 
  }
  int64_t GetScore() const { return migrate_score_; }

 public:
  ZoneSnapshot(const Zone& zone, const ZenFSSnapshotOptions& options)
      : start_(), wp_(), capacity_(), used_capacity_(), max_capacity_() {
    if (options.zone_.enabled_) {
      if (options.zone_.id_ || options.zone_.write_position_) wp_ = zone.wp_;
      if (options.zone_.remaining_capacity_) capacity_ = zone.capacity_;
      if (options.zone_.used_capacity_) capacity_ = zone.used_capacity_;
      if (options.zone_.max_capacity_) max_capacity_ = zone.max_capacity_;
    }
  }

};

struct ZenFSSnapshot {
 public:
  ZBDSnapshot zbd_;
  std::vector<ZoneSnapshot> zones_;
  std::vector<ZoneFileSnapshot> zone_files_;

 private:
  bool zone_view_enabled_;
  bool sorted_by_score_;
  std::unordered_map<uint64_t, ZoneSnapshot*> zone_map_;
  std::unordered_map<uint64_t, ZoneFileSnapshot*> file_map_;

  int64_t CalculateZoneMigrateScore(ZoneSnapshot& zone) {
    assert(zone_view_enabled_);
    int64_t benifit = zone.ReclaimableCapacity();
    int64_t cost = 0;
    for (auto& extent : zone.GetFileExtent()) {
      cost += extent.Length() * 1;  
      // shall be multiplied by extent.migrate_cost_ in the future.
    }
    return (1 * benifit - 1 * cost);
  }
 public:
  void GenerateZoneView() {
    if (zone_view_enabled_) return;

    for (auto& zone : zones_)
      zone_map_[zone.ID()] = &zone;
    for (auto& file : zone_files_)
      file_map_[file.FileID()] = &file;

    for (auto& file : zone_files_)
      for (auto& extent : file.EditableExtent()) {
        uint64_t zone_id = extent.ZoneID();
        if (zone_map_.find(zone_id) == zone_map_.end()) continue;
        auto zone = zone_map_[zone_id];

        extent.SetFileID(file.FileID());
        zone->AddFileExtent(extent);
      }
    
    zone_view_enabled_ = 1;
  }

  void SortZonesByMigrateScore() {
    // Please make sure you have collected all necessery information.
    if (!zone_view_enabled_)
      GenerateZoneView();
    if (!sorted_by_score_) {
      for (auto& zone : zones_)
        if (!zone.ScoreCalculated())
          zone.SetScore(CalculateZoneMigrateScore(zone));

      std::sort(zones_.begin(), zones_.end(), [](const ZoneSnapshot &l, const ZoneSnapshot &r) {
        return l.GetScore() > r.GetScore();
      });
      sorted_by_score_ = 1;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
