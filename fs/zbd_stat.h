// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <string>
#include <vector>

namespace ROCKSDB_NAMESPACE {

class ZoneFileStat {
 public:
  uint64_t file_id;
  uint64_t size_in_zone;
  std::string filename;
};

class ZoneStat {
 public:
  uint64_t total_capacity;
  uint64_t write_position;
  uint64_t start_position;
  std::vector<ZoneFileStat> files;
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
  virtual ~NoZenFSMetrics() {}
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

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)