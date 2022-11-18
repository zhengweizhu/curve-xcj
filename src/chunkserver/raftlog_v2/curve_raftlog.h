/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef SRC_CHUNKSERVER_RAFTLOG_V2_CURVE_RAFTLOG_H_
#define SRC_CHUNKSERVER_RAFTLOG_V2_CURVE_RAFTLOG_H_

#include <braft/macros.h>
#include <braft/storage.h>
#include <butil/iobuf.h>
#include <butil/memory/ref_counted.h>
#include <gtest/gtest_prod.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace braft {
struct LogEntry;
class ConfigurationManager;
}  // namespace braft

namespace curve {
namespace chunkserver {

class FilePool;

namespace raftlog_v2 {

// raftlog_v2::CurveSegment and raftlog_v2::CurveSegmentLogStorage are
// request-aware wal storage.
//
// For raft internal requests(e.g., raft configuration), they're consist with
// braft::Segment and braft::SegmentLogStorage.
// For normal user IO requests, IO data is stored in a separate file which is
// preallocated when formatting file pool, request info
// (curve::chunkserver::ChunkRequest) and necessary IO data index info are store
// with braft::SegmentHeader in append-only manner.

class CurveSegment : public butil::RefCountedThreadSafe<CurveSegment> {
 public:
    struct EntryHeader;

    // Create an opening segment
    CurveSegment(const std::string& path,
                 int64_t first_index,
                 int checksum_type,
                 FilePool* wal_file_pool);

    // Create a closed segment
    CurveSegment(const std::string& path,
                 int64_t first_index,
                 int64_t last_index,
                 int checksum_type,
                 FilePool* wal_file_pool);

    int create();

    int close(bool will_sync = true);

    int unlink();

    int load(braft::ConfigurationManager* configuration_manager);

    int sync(bool will_sync);

    // Append a log entry.
    // Return values:
    //   0 : success
    //   ENOSPC : current segment is full or no enough space to store data
    //   other others
    int append(const braft::LogEntry* entry);

    bool is_open() const { return _is_open; }

    // int64_t bytes() const { return _data_bytes; }

    bool is_full() const { return _is_full; }

    int64_t first_index() const { return _first_index; }

    int64_t last_index() const {
        return _last_index.load(butil::memory_order_consume);
    }

    // Get log entry by index.
    // Return `nullptr` if failed.
    braft::LogEntry* get(int64_t index);

    int64_t get_term(int64_t index) const;

    // Truncate log after `last_index_kept`
    // Returns 0 on success.
    int truncate(int64_t last_index_kept);

 private:
    friend class butil::RefCountedThreadSafe<CurveSegment>;
    ~CurveSegment();

    struct LogMeta {
        off_t offset;
        size_t length;
        int64_t term;
    };

    // Handle user IO data, write IO data in data file.
    //
    // |data| is original user IO request, this procedure will decodes IO data
    // from it, and then stores data index info in it.
    // |data_offset| will stores corresponding offset in data file.
    //
    // Returns 0 on success, ENOSPC if data file is full
    int _handle_user_request_data(butil::IOBuf* data, int64_t* data_offset);

    int _get_meta(int64_t index, LogMeta* meta) const;

    int _load_entry(off_t offset,
                    EntryHeader* head,
                    butil::IOBuf* data,
                    size_t size_hint,
                    bool is_load,
                    int64_t* data_off = nullptr,
                    int64_t* data_sz = nullptr);

 private:
    mutable braft::raft_mutex_t _mutex;

    std::string _path;

    bool _is_open;
    bool _is_full;

    int _checksum_type;

    int _meta_fd = -1;
    int _data_fd = -1;

    int64_t _meta_bytes = 0;
    int64_t _data_bytes = 0;

    const int64_t _first_index;
    butil::atomic<int64_t> _last_index;

    FilePool* _data_file_pool;

    std::string _meta_path;
    std::string _data_path;

    std::vector<std::pair</*offset*/ int64_t, /*term*/ int64_t>>
            _offset_and_term;

    std::vector</*offset*/ int64_t> _data_offset;
};

class CurveSegmentLogStorage : public braft::LogStorage {
    FRIEND_TEST(LogStorageTest, garbage_wal_data);

 public:
    using SegmentMap = std::map<int64_t, scoped_refptr<CurveSegment>>;

    explicit CurveSegmentLogStorage(const std::string& path,
                                    bool enable_sync = true)
        : _first_log_index(1),
          _last_log_index(0),
          _path(path),
          _checksum_type(0),
          _enable_sync(enable_sync) {
        LOG(INFO) << "Created CurveSegmentLogStorage";
    }

    CurveSegmentLogStorage()
        : _first_log_index(1),
          _last_log_index(0),
          _checksum_type(0),
          _enable_sync(true) {
        LOG(INFO) << "Created CurveSegmentLogStorage";
    }

    CurveSegmentLogStorage(const CurveSegmentLogStorage&) = delete;
    CurveSegmentLogStorage& operator=(const CurveSegmentLogStorage&) = delete;

    ~CurveSegmentLogStorage() override = default;

    static void RegisterFilePool(std::shared_ptr<FilePool> file_pool) {
        _data_file_pool = std::move(file_pool);
    }

    static bool IsWALDataFile(const std::string& filename);

    // Init logstorage, check consistency and integrity
    int init(braft::ConfigurationManager* configuration_manager) override;

    // First log index in log
    int64_t first_log_index() override;

    // Last log index in log
    int64_t last_log_index() override;

    // Get logentry by index
    braft::LogEntry* get_entry(int64_t index) override;

    // Get logentry's term by index
    int64_t get_term(int64_t index) override;

    // Append entries to log
    int append_entry(const braft::LogEntry* entry) override;

    // Append entries to log.
    // Return number of entries successfully appended
    int append_entries(const std::vector<braft::LogEntry*>& entries) override;

    // Delete logs from storage's head, [first_log_index, first_index_kept) will
    // be discarded.
    int truncate_prefix(int64_t first_index_kept) override;

    // Delete uncommitted logs from storage's tail, (last_index_kept,
    // last_log_index] will be discarded.
    int truncate_suffix(int64_t last_index_kept) override;

    // Drop all the existing logs and reset next log index to `next_log_index`.
    // This function is called after installing snapshot from leader.
    int reset(int64_t next_log_index) override;

    // Create an instance of this kind of LogStorage with the parameters encoded
    // in `uri`.
    // Return the address referenced to the instance on success, nullptr
    // otherwise.
    braft::LogStorage* new_instance(const std::string& uri) const override;

    SegmentMap& TEST_segments() { return _segments; }

 private:
    // Get current segment or open a new one to append entries.
    // Return `nullptr` if failed.
    scoped_refptr<CurveSegment> open_segment();

    int get_segment(int64_t index, scoped_refptr<CurveSegment>* ptr);

    int load_meta();
    int save_meta(int64_t index);

    int list_segments(bool is_empty);
    int load_segments(braft::ConfigurationManager* configuration_manager);

    void pop_segments(int64_t fist_index_kept,
                      std::vector<scoped_refptr<CurveSegment>>* popped);
    void pop_segments_from_back(
            int64_t last_index_kept,
            std::vector<scoped_refptr<CurveSegment>>* popped,
            scoped_refptr<CurveSegment>* last_segment);

 private:
    braft::raft_mutex_t _mutex;
    butil::atomic<int64_t> _first_log_index;
    butil::atomic<int64_t> _last_log_index;
    std::string _path;
    SegmentMap _segments;
    scoped_refptr<CurveSegment> _open_segment;
    int _checksum_type;
    bool _enable_sync;

    static std::shared_ptr<FilePool> _data_file_pool;
};

void RegisterCurveSegmentLogStorageV2OrDie();

}  // namespace raftlog_v2
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTLOG_V2_CURVE_RAFTLOG_H_
