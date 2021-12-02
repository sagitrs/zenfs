// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <string>
#include <vector>
#include "io_zenfs.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

// We provide three Snapshot classes to capture real-time information from ZenFS
// for use by the upper layer algorithms. The three Snapshots will capture
// information from Zone, ZoneFile, and ZoneExtent respectively.

// If you plan to modify the variables of these three classes, please make sure
// that they have a public interface for copying after modification and that the
// interface of the Snapshot is still logically correct.

class ZoneSnapshot {
 private:
  uint64_t start_;
  uint64_t max_capacity_;
  uint64_t wp_;

 public:
  ZoneSnapshot(const Zone& zone)
      : start_(zone.start_), max_capacity_(zone.max_capacity_), wp_(zone.wp_) {}

 public:
  uint64_t FakeID() const { return start_; }
  uint64_t TotalCapacity() const { return max_capacity_; }
  uint64_t WritePosition() const { return wp_; }
  uint64_t StartPosition() const { return start_; }
};

struct ZoneExtentSnapshot {
 private:
  uint64_t start_;
  uint64_t length_;
  uint64_t zone_fake_id_;

 public:
  ZoneExtentSnapshot(const ZoneExtent& extent)
      : start_(extent.start_),
        length_(extent.length_),
        zone_fake_id_(extent.zone_->start_) {}

 public:
  uint64_t Start() const { return start_; }
  uint64_t Length() const { return length_; }
  uint64_t ZoneFakeID() const { return zone_fake_id_; }
};

struct ZoneFileSnapshot {
 private:
  uint64_t file_id_;
  std::string filename_;
  std::vector<ZoneExtentSnapshot> extent_;

 public:
  ZoneFileSnapshot(ZoneFile& file)
      : file_id_(file.GetID()), filename_(file.GetFilename()), extent_() {
    for (ZoneExtent*& extent : file.GetExtents()) extent_.emplace_back(*extent);
  }

 public:
  uint64_t FileID() const { return file_id_; }
  const std::string& Filename() const { return filename_; }
  const std::vector<ZoneExtentSnapshot>& Extent() const { return extent_; }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)