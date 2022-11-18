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

#include "src/chunkserver/raftlog_v2/curve_raftlog.h"

#include <braft/configuration_manager.h>
#include <braft/fsync.h>
#include <braft/local_storage.pb.h>
#include <braft/protobuf_file.h>
#include <braft/util.h>
#include <butil/fd_utility.h>
#include <butil/raw_pack.h>
#include <butil/sys_byteorder.h>
#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <mutex>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "src/chunkserver/datastore/file_pool.h"

namespace braft {
DECLARE_bool(raft_sync);
DECLARE_bool(raft_sync_segments);
}  // namespace braft

namespace curve {
namespace chunkserver {
namespace raftlog_v2 {

DEFINE_bool(raft_sync_meta_path, false, "");
DEFINE_bool(raft_sync_data_file, false, "");

#define CURVE_SEGMENT_META_PATH "log_meta"

#define CURVE_SEGMENT_DATA_FILE_PATTERN "curve_data_%020" PRId64
#define CURVE_SEGMENT_DATA_PREFIX "curve_data_"

#define CURVE_SEGMENT_META_PATH_OPEN_PATTERN "log_inprogress_%020" PRId64
#define CURVE_SEGMENT_META_PATH_CLOSE_PATTERN "log_%020" PRId64 "_%020" PRId64

#define RAFTLOG_V2_VLOG VLOG(100)

namespace {

// consist with braft
enum CheckSumType {
    CHECKSUM_MURMURHASH32 = 0,
    CHECKSUM_CRC32 = 1,
};

// consist with braft
constexpr size_t ENTRY_HEADER_SIZE = 24;

constexpr mode_t kOpenMode = 0644;
constexpr size_t kMetaDataSize = 12;
constexpr size_t kMetaFileHeaderSize = 4096;
constexpr size_t kDirectIOBufferAlignment = 4096;

inline uint32_t get_checksum(int checksum_type, const char* data, size_t len) {
    switch (checksum_type) {
        case CHECKSUM_MURMURHASH32:
            return braft::murmurhash32(data, static_cast<int>(len));
        case CHECKSUM_CRC32:
            return braft::crc32(data, static_cast<int>(len));
        default:
            CHECK(false) << "Unknown checksum_type=" << checksum_type;
            abort();
            return 0;
    }
}

inline uint32_t get_checksum(int checksum_type, const butil::IOBuf& data) {
    switch (checksum_type) {
        case CHECKSUM_MURMURHASH32:
            return braft::murmurhash32(data);
        case CHECKSUM_CRC32:
            return braft::crc32(data);
        default:
            CHECK(false) << "Unknown checksum_type=" << checksum_type;
            abort();
            return 0;
    }
}

inline bool verify_checksum(int checksum_type,
                            const char* data,
                            size_t len,
                            uint32_t value) {
    switch (checksum_type) {
        case CHECKSUM_MURMURHASH32:
            return (value == braft::murmurhash32(data, static_cast<int>(len)));
        case CHECKSUM_CRC32:
            return (value == braft::crc32(data, static_cast<int>(len)));
        default:
            LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
            return false;
    }
}

inline bool verify_checksum(int checksum_type,
                            const butil::IOBuf& data,
                            uint32_t value) {
    switch (checksum_type) {
        case CHECKSUM_MURMURHASH32:
            return (value == braft::murmurhash32(data));
        case CHECKSUM_CRC32:
            return (value == braft::crc32(data));
        default:
            LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
            return false;
    }
}

inline int ftruncate_uninterrupted(int fd, off_t length) {
    int rc = 0;
    do {
        rc = ftruncate(fd, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}

const char* get_file_pool_meta_page(uint32_t meta_page_size) {
    static const uint32_t page_size = meta_page_size;
    CHECK_EQ(page_size, meta_page_size);
    static std::once_flag once;
    static std::unique_ptr<char[]> meta_page(new char[page_size]);
    std::call_once(once, []() { memset(meta_page.get(), 0, page_size); });

    return meta_page.get();
}

void* run_unlink(void* arg) {
    std::string* file_path = (std::string*)arg;
    butil::Timer timer;
    timer.start();
    int ret = ::unlink(file_path->c_str());
    timer.stop();
    BRAFT_VLOG << "unlink " << *file_path << " ret " << ret
               << " time: " << timer.u_elapsed();
    delete file_path;

    return nullptr;
}

inline void iobuf_deleter(void* ptr) {
    free(ptr);
}

inline std::string path_join(const std::string& dir, const char* name) {
    return absl::StrCat(dir, "/", name);
}

inline std::string path_join(const std::string& dir, const std::string& name) {
    return absl::StrCat(dir, "/", name);
}

int recycle_data_file(FilePool* data_file_pool, const std::string& file) {
    struct stat st_buf;
    if (::stat(file.c_str(), &st_buf) != 0) {
        LOG(WARNING) << "Failed to stat `" << file << "', error: " << berror();
        return -1;
    }

    static const uint32_t data_file_size_in_pool =
            data_file_pool->GetFilePoolOpt().metaPageSize +
            data_file_pool->GetFilePoolOpt().fileSize;

    int rc = 0;
    if (data_file_size_in_pool != st_buf.st_size) {
        LOG(WARNING) << "Unexpected data file size, expected: "
                     << data_file_size_in_pool << ", actual: " << st_buf.st_size
                     << ", file: " << file;
        ::unlink(file.c_str());
    } else {
        rc = data_file_pool->RecycleFile(file);
        LOG_IF(WARNING, rc != 0)
                << "Failed to recycle data segment file, path: `" << file
                << "', error: " << berror();
    }

    LOG(WARNING) << "Recycle or unlink unused data segment, path: `" << file
                 << "'";
    return rc;
}

}  // namespace

// Format of Header, all fields are in network order
// | -------------------- term (64bits) -------------------------  |
// | entry-type (8bits) | checksum_type (8bits) | reserved(16bits) |
// | ------------------ data len (32bits) -----------------------  |
// | data_checksum (32bits) | header checksum (32bits)             |

// consist with braft
struct CurveSegment::EntryHeader {
    int64_t term;
    int type;
    int checksum_type;
    uint32_t data_len;
    uint32_t data_real_len;
    uint32_t data_checksum;

    friend std::ostream& operator<<(std::ostream& os, const EntryHeader& h) {
        os << "{term=" << h.term << ", type=" << h.type
           << ", data_len=" << h.data_len
           << ", checksum_type=" << h.checksum_type
           << ", data_checksum=" << h.data_checksum << '}';
        return os;
    }
};

CurveSegment::CurveSegment(const std::string& path,
                           int64_t first_index,
                           int checksum_type,
                           FilePool* wal_file_pool)
    : _path(path),
      _is_open(true),
      _is_full(false),
      _checksum_type(checksum_type),
      _first_index(first_index),
      _last_index(first_index - 1),
      _data_file_pool(wal_file_pool) {}

CurveSegment::CurveSegment(const std::string& path,
                           int64_t first_index,
                           int64_t last_index,
                           int checksum_type,
                           FilePool* wal_file_pool)
    : _path(path),
      _is_open(false),
      _is_full(true),
      _checksum_type(checksum_type),
      _first_index(first_index),
      _last_index(last_index),
      _data_file_pool(wal_file_pool) {}

CurveSegment::~CurveSegment() {
    if (_meta_fd >= 0) {
        LOG_IF(WARNING, ::close(_meta_fd) != 0)
                << "Failed to close segment, meta fd: " << _meta_fd
                << ", error: " << berror();
        _meta_fd = -1;
    }

    if (_data_fd >= 0) {
        LOG_IF(WARNING, ::close(_data_fd) != 0)
                << "Failed to close segment, data fd: " << _data_fd
                << ", error: " << berror();
        _data_fd = -1;
    }
}

int CurveSegment::create() {
    if (!_is_open) {
        CHECK(false) << "Create on closed segment at first_index: "
                     << _first_index << " in " << _path;
        return -1;
    }

    std::string data_path(_path);
    butil::string_appendf(&data_path, "/" CURVE_SEGMENT_DATA_FILE_PATTERN,
                          _first_index);
    // FIXME(wuhanqing): is a full path

    static const uint32_t meta_page_size =
            _data_file_pool->GetFilePoolOpt().metaPageSize;
    int rc = _data_file_pool->GetFile(data_path,
                                      get_file_pool_meta_page(meta_page_size));
    if (rc != 0) {
        LOG(ERROR) << "Failed to get segment from file pool, path: "
                   << data_path;
        return -1;
    }

    _data_fd =
            ::open(data_path.c_str(), O_RDWR | O_NOATIME | O_DIRECT, kOpenMode);
    if (_data_fd >= 0) {
        butil::make_close_on_exec(_data_fd);
    } else {
        LOG(ERROR) << "Failed to open `" << data_path
                   << "', error: " << berror();
        return -1;
    }

    bool need_cleanup_data_file = false;
    auto cleanup_data_file = absl::MakeCleanup([this,
                                                &need_cleanup_data_file]() {
        if (need_cleanup_data_file) {
            LOG_IF(WARNING, 0 != ::close(_data_fd))
                    << "Failed to close fd: " << _data_fd
                    << ", error: " << berror();
            LOG_IF(WARNING, 0 != _data_file_pool->RecycleFile(_data_path))
                    << "Failed to recycle data file: " << _data_path;

            _data_fd = -1;
        }
    });

    std::string meta_path(_path);
    butil::string_appendf(&meta_path, "/" CURVE_SEGMENT_META_PATH_OPEN_PATTERN,
                          _first_index);
    _meta_fd = ::open(meta_path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_NOATIME,
                      kOpenMode);
    if (_meta_fd >= 0) {
        butil::make_close_on_exec(_meta_fd);
    } else {
        LOG(ERROR) << "Failed to open '" << meta_path
                   << "', error: " << berror();
        need_cleanup_data_file = true;

        return -1;
    }

    bool need_cleanup_meta_path = false;
    auto cleanup_meta_path =
            absl::MakeCleanup([this, &need_cleanup_meta_path]() {
                if (need_cleanup_meta_path) {
                    LOG_IF(WARNING, 0 != ::close(_meta_fd))
                            << "Failed to close fd: " << _meta_fd
                            << ", error: " << berror();
                    LOG_IF(WARNING, ::unlink(_meta_path.c_str()))
                            << "Failed to unlink file: " << _meta_path
                            << ", error: " << berror();

                    _meta_fd = -1;
                }
            });

    // Record data file name in header, header length is 4096 bytes.
    // Format of meta file header
    // |-- length   (4 bytes) --|-- data_path (depends) --|
    // |-- checksum (4 bytes) --|-- padding --|
    char meta_file_header_buf[kMetaFileHeaderSize] = {0};
    uint32_t length = sizeof(uint32_t) + data_path.size() + sizeof(uint32_t);
    CHECK(length <= kMetaFileHeaderSize);
    length = butil::HostToNet32(length);
    ::memcpy(meta_file_header_buf, &length, sizeof(length));
    ::memcpy(meta_file_header_buf + sizeof(uint32_t), data_path.c_str(),
             data_path.size());

    uint32_t checksum = butil::HostToNet32(
            get_checksum(_checksum_type, meta_file_header_buf,
                         sizeof(uint32_t) + data_path.size()));
    ::memcpy(meta_file_header_buf + sizeof(uint32_t) + data_path.size(),
             &checksum, sizeof(checksum));

    const ssize_t written =
            write(_meta_fd, meta_file_header_buf, kMetaFileHeaderSize);
    if (written != kMetaFileHeaderSize) {
        LOG(ERROR) << "Failed to write meta file header, error: " << berror();
        need_cleanup_data_file = true;
        need_cleanup_meta_path = true;

        return -1;
    }

    _meta_path = std::move(meta_path);
    _data_path = std::move(data_path);

    _meta_bytes = kMetaFileHeaderSize;

    // overwrite metapage
    _data_bytes = 0;

    return 0;
}

int CurveSegment::close(bool will_sync) {
    CHECK(_is_open);

    std::string new_path(_path);
    butil::string_appendf(&new_path, "/" CURVE_SEGMENT_META_PATH_CLOSE_PATTERN,
                          _first_index, _last_index.load());

    // TODO: optimize index memory usage by reconstruct vector
    LOG(INFO) << "close a full segment. Current first_index: " << _first_index
              << " last_index: " << _last_index
              << " raft_sync_segments: " << braft::FLAGS_raft_sync_segments
              << " will_sync: " << will_sync << " path: `" << new_path << "'";

    int ret = 0;
    if (_last_index > _first_index) {
        if (braft::FLAGS_raft_sync_segments && will_sync) {
            ret = braft::raft_fsync(_meta_fd);
            if (ret == 0) {
                ret = braft::raft_fsync(_data_fd);
            } else {
                braft::raft_fsync(_data_fd);
            }
        }
    }

    if (ret == 0) {
        _is_open = false;
        const int rc = ::rename(_meta_path.c_str(), new_path.c_str());
        LOG_IF(INFO, rc == 0)
                << "Renamed `" << _meta_path << "' to `" << new_path << '\'';
        LOG_IF(ERROR, rc != 0) << "Failed to rename `" << _meta_path << "' to `"
                               << new_path << "\', " << berror();
        if (rc == 0) {
            _meta_path = std::move(new_path);
        }

        return rc;
    }

    return ret;
}

int CurveSegment::unlink() {
    int ret = 0;

    do {
        ret = _data_file_pool->RecycleFile(_data_path);
        if (ret != 0) {
            LOG(ERROR) << "Failed to recycle data file, error: " << berror();
            break;
        }

        LOG(INFO) << "Recycled data `" << _data_path << "'";

        std::string tmp_path = _meta_path + ".tmp";
        ret = ::rename(_meta_path.c_str(), tmp_path.c_str());
        if (ret != 0) {
            PLOG(ERROR) << "Failed to rename `" << _meta_path << "' to `"
                        << tmp_path << "', error: " << berror();
            break;
        }

        // start bthread to unlink
        // TODO unlink follow control
        std::string* file_path = new std::string(std::move(tmp_path));
        bthread_t tid;
        if (bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, run_unlink,
                                     file_path) != 0) {
            run_unlink(file_path);
        }

        LOG(INFO) << "Unlinked segment `" << _meta_path << '\'';
    } while (false);

    return ret;
}

int CurveSegment::load(braft::ConfigurationManager* configuration_manager) {
    int ret = 0;
    std::string path(_path);

    if (_is_open) {
        butil::string_appendf(&path, "/" CURVE_SEGMENT_META_PATH_OPEN_PATTERN,
                              _first_index);
    } else {
        butil::string_appendf(&path, "/" CURVE_SEGMENT_META_PATH_CLOSE_PATTERN,
                              _first_index, _last_index.load());
    }

    _meta_fd = ::open(path.c_str(), O_RDWR | O_NOATIME, kOpenMode);
    if (_meta_fd < 0) {
        LOG(ERROR) << "Failed to open " << path << ", " << berror();
        return -1;
    }

    bool need_cleanup_meta_path = true;
    auto cleanup_meta_path =
            absl::MakeCleanup([this, &need_cleanup_meta_path]() {
                if (need_cleanup_meta_path) {
                    LOG_IF(WARNING, 0 != ::close(_meta_fd))
                            << "Failed to close fd: " << _meta_fd
                            << ", error: " << berror();
                    _meta_fd = -1;
                }
            });

    butil::make_close_on_exec(_meta_fd);
    struct stat st_buf;
    if (fstat(_meta_fd, &st_buf) != 0) {
        LOG(ERROR) << "Failed to get the stat of " << path << ", " << berror();
        return -1;
    }

    // load meta file's header
    char user_meta_data_buf[kMetaFileHeaderSize] = {0};
    const ssize_t n =
            pread(_meta_fd, user_meta_data_buf, kMetaFileHeaderSize, 0);
    if (n != static_cast<ssize_t>(kMetaFileHeaderSize)) {
        LOG(ERROR) << "Failed to read meta file header, error: " << berror();
        return -1;
    }

    uint32_t data_file_name_len = 0;
    ::memcpy(&data_file_name_len, user_meta_data_buf,
             sizeof(data_file_name_len));
    data_file_name_len =
            butil::NetToHost32(data_file_name_len) - 2 * sizeof(uint32_t);
    const std::string data_file_name(user_meta_data_buf + sizeof(uint32_t),
                                     data_file_name_len);
    uint32_t checksum = 0;
    ::memcpy(&checksum,
             user_meta_data_buf + sizeof(uint32_t) + data_file_name_len,
             sizeof(checksum));
    checksum = butil::NetToHost32(checksum);

    if (!verify_checksum(_checksum_type, user_meta_data_buf,
                         sizeof(uint32_t) + data_file_name_len, checksum)) {
        LOG(ERROR) << "Found corrupted meta file header, file: " << path;
        return -1;
    }

    _data_fd = ::open(data_file_name.c_str(), O_RDWR | O_NOATIME | O_DIRECT,
                      kOpenMode);
    if (_data_fd < 0) {
        LOG(ERROR) << "Failed to open `" << data_file_name
                   << "', error: " << berror();
        return -1;
    }
    butil::make_close_on_exec(_data_fd);

    // load entry index
    int64_t file_size = st_buf.st_size;
    int64_t entry_off = kMetaFileHeaderSize;
    int64_t actual_last_index = _first_index - 1;
    int64_t data_off = 0;
    int64_t data_sz = 0;
    for (int64_t i = _first_index; entry_off < file_size; i++) {
        EntryHeader header;
        const int rc = _load_entry(entry_off, &header, NULL, ENTRY_HEADER_SIZE,
                                   /*is_loading=*/true, &data_off, &data_sz);
        if (rc > 0) {
            // The last log was not completely written, which should be
            // truncated
            break;
        }
        if (rc < 0) {
            ret = rc;
            break;
        }
        // rc == 0
        const int64_t skip_len = ENTRY_HEADER_SIZE + header.data_len;
        if (entry_off + skip_len > file_size) {
            // The last log was not completely written and it should be
            // truncated
            break;
        }
        if (header.type == braft::ENTRY_TYPE_CONFIGURATION) {
            butil::IOBuf data;
            // Header will be parsed again but it's fine as configuration
            // changing is rare
            if (_load_entry(entry_off, NULL, &data, skip_len,
                            /*is_loading=*/false) != 0) {
                break;
            }
            scoped_refptr<braft::LogEntry> entry = new braft::LogEntry();
            entry->id.index = i;
            entry->id.term = header.term;
            butil::Status status = parse_configuration_meta(data, entry);
            if (status.ok()) {
                braft::ConfigurationEntry conf_entry(*entry);
                configuration_manager->add(conf_entry);
            } else {
                LOG(ERROR) << "fail to parse configuration meta, path: "
                           << _path << " entry_off " << entry_off;
                ret = -1;
                break;
            }
        }
        _offset_and_term.push_back(std::make_pair(entry_off, header.term));
        _data_offset.push_back(data_off);
        CHECK_EQ(_offset_and_term.size(), _data_offset.size())
                << " offset_and_term size: " << _offset_and_term.size()
                << ", data_offset size: " << _data_offset.size();
        ++actual_last_index;
        entry_off += skip_len;
    }

    const int64_t last_index = _last_index.load(butil::memory_order_relaxed);
    if (ret == 0 && !_is_open) {
        if (actual_last_index < last_index) {
            LOG(ERROR) << "data lost in a full segment, path: " << _path
                       << " first_index: " << _first_index
                       << " expect_last_index: " << last_index
                       << " actual_last_index: " << actual_last_index;
            ret = -1;
        } else if (actual_last_index > last_index) {
            // FIXME(zhengpengfei): should we ignore garbage entries silently
            LOG(ERROR) << "found garbage in a full segment, path: " << _path
                       << " first_index: " << _first_index
                       << " expect_last_index: " << last_index
                       << " actual_last_index: " << actual_last_index;
            ret = -1;
        }
    }

    if (ret != 0) {
        return ret;
    }

    if (_is_open) {
        _last_index = actual_last_index;
    }

    // truncate last uncompleted entry
    if (entry_off != file_size) {
        LOG(INFO) << "truncate last uncompleted write entry, path: " << _path
                  << " first_index: " << _first_index
                  << " old_size: " << file_size << " new_size: " << entry_off;
        ret = ftruncate_uninterrupted(_meta_fd, entry_off);
    }

    // seek to end, for opening segment
    ::lseek(_meta_fd, entry_off, SEEK_SET);

    _meta_bytes = entry_off;
    _data_bytes = data_off + data_sz;

    _meta_path = std::move(path);
    _data_path = std::move(data_file_name);

    need_cleanup_meta_path = false;

    return ret;
}

int CurveSegment::_load_entry(off_t offset,
                              EntryHeader* head,
                              butil::IOBuf* data,
                              size_t size_hint,
                              bool is_loading,
                              int64_t* data_off,
                              int64_t* data_sz) {
    assert(is_loading == false ||
           (is_loading == true && data_off != nullptr && data_sz != nullptr));

    butil::IOPortal buf;
    size_t to_read = std::max(size_hint, ENTRY_HEADER_SIZE);
    ssize_t n = braft::file_pread(&buf, _meta_fd, offset, to_read);
    if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
    }

    char header_buf[ENTRY_HEADER_SIZE];
    const char* p = (const char*)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    butil::RawUnpacker(p)
            .unpack64((uint64_t&)term)
            .unpack32(meta_field)
            .unpack32(data_len)
            .unpack32(data_checksum)
            .unpack32(header_checksum);

    EntryHeader tmp;
    tmp.term = term;
    tmp.type = meta_field >> 24;
    tmp.checksum_type = (meta_field << 8) >> 24;
    tmp.data_len = data_len;
    tmp.data_checksum = data_checksum;
    if (!verify_checksum(tmp.checksum_type, p, ENTRY_HEADER_SIZE - 4,
                         header_checksum)) {
        LOG(ERROR) << "Found corrupted header at offset=" << offset
                   << ", header=" << tmp << ", path: " << _path;
        return -1;
    }

    if (head != nullptr) {
        *head = tmp;
    }

    if (data == nullptr && tmp.type != braft::ENTRY_TYPE_DATA) {
        if (is_loading) {
            // _data_offset.push_back(_data_bytes);
            *data_off = _data_bytes;
            *data_sz = 0;
        }
        return 0;
    }

    if (buf.length() < ENTRY_HEADER_SIZE + data_len) {
        const size_t to_read = ENTRY_HEADER_SIZE + data_len - buf.length();
        const ssize_t n = braft::file_pread(&buf, _meta_fd,
                                            offset + buf.length(), to_read);
        if (n != (ssize_t)to_read) {
            return n < 0 ? -1 : 1;
        }
    } else if (buf.length() > ENTRY_HEADER_SIZE + data_len) {
        buf.pop_back(buf.length() - ENTRY_HEADER_SIZE - data_len);
    }
    CHECK_EQ(buf.length(), ENTRY_HEADER_SIZE + data_len);

    buf.pop_front(ENTRY_HEADER_SIZE);
    if (!verify_checksum(tmp.checksum_type, buf, tmp.data_checksum)) {
        LOG(ERROR) << "Found corrupted data at offset="
                   << offset + ENTRY_HEADER_SIZE << " header=" << tmp
                   << " path: " << _path;
        // TODO: abort()?
        return -1;
    }

    if (tmp.type != braft::ENTRY_TYPE_DATA) {
        data->swap(buf);
        if (is_loading) {
            *data_off = _data_bytes;
            *data_sz = 0;
        }
        return 0;
    }

    // unpack
    uint32_t data_offset = 0;
    uint32_t data_size = 0;
    data_checksum = 0;
    char meta_data_buf[kMetaDataSize];
    p = (const char*)buf.fetch(meta_data_buf, kMetaDataSize);
    butil::RawUnpacker(p)
            .unpack32(data_offset)
            .unpack32(data_size)
            .unpack32(data_checksum);

    if (is_loading) {
        *data_off = data_offset;
        *data_sz = data_size;
    }

    if (data == nullptr) {
        return 0;
    }

    char* data_buf = static_cast<char*>(
            aligned_alloc(kDirectIOBufferAlignment, data_size));
    CHECK(data_buf != nullptr) << " error: " << berror();

    n = ::pread(_data_fd, data_buf, data_size, data_offset);
    if (n != static_cast<ssize_t>(data_size)) {
        free(data_buf);
        LOG(ERROR) << "Failed to read data from " << _data_fd
                   << ", error: " << berror();
        return -1;
    }

    if (!verify_checksum(_checksum_type, data_buf, data_size, data_checksum)) {
        free(data_buf);
        LOG(ERROR) << "Found corrupted data offset=" << data_offset;
        return -1;
    }

    buf.pop_front(kMetaDataSize);
    data->swap(buf);
    data->append_user_data(data_buf, data_size, iobuf_deleter);

    return 0;
}

int CurveSegment::sync(bool will_sync) {
    // FIXME: sync policy
    if (_last_index > _first_index) {
        if (braft::FLAGS_raft_sync && will_sync) {
            int rc1 =
                    FLAGS_raft_sync_meta_path ? braft::raft_fsync(_meta_fd) : 0;
            int rc2 =
                    FLAGS_raft_sync_data_file ? braft::raft_fsync(_data_fd) : 0;
            return rc1 == 0 ? rc2 : rc1;
        }
        return 0;
    }

    return 0;
}

int CurveSegment::append(const braft::LogEntry* entry) {
    if (BAIDU_UNLIKELY(entry == nullptr || !_is_open)) {
        return EINVAL;
    }
    if (BAIDU_UNLIKELY(_is_full)) {
        LOG(WARNING) << "segment is full";
        return ENOSPC;
    }
    if (entry->id.index != _last_index.load(butil::memory_order_consume) + 1) {
        CHECK(false) << "entry->index=" << entry->id.index
                     << " _last_index=" << _last_index
                     << " _first_index=" << _first_index;
        return ERANGE;
    }

    butil::IOBuf data;
    int64_t data_offset = 0;
    switch (entry->type) {
        case braft::ENTRY_TYPE_DATA: {
            data.append(entry->data);
            int rc = _handle_user_request_data(&data, &data_offset);
            if (rc != 0) {
                return rc;
            }
        } break;
        case braft::ENTRY_TYPE_NO_OP:
            data_offset = _data_bytes;
            break;
        case braft::ENTRY_TYPE_CONFIGURATION: {
            butil::Status status = serialize_configuration_meta(entry, data);
            if (!status.ok()) {
                LOG(ERROR) << "Fail to serialize ConfigurationPBMeta, path: "
                           << _path;
                return -1;
            }
            data_offset = _data_bytes;
        } break;
        default:
            LOG(FATAL) << "unknow entry type: " << entry->type
                       << ", path: " << _path;
            return -1;
    }

    static bvar::LatencyRecorder write_meta_latency("curve_segment_write_meta");
    static bvar::LatencyRecorder write_meta_size("curve_segment_write_size");
    auto start_us = butil::gettimeofday_us();

    CHECK_LE(data.length(), 1UL << 56);

    char header_buf[ENTRY_HEADER_SIZE];
    const uint32_t meta_field = (entry->type << 24) | (_checksum_type << 16);
    butil::RawPacker packer(header_buf);
    packer.pack64(entry->id.term)
            .pack32(meta_field)
            .pack32(data.size())
            .pack32(get_checksum(_checksum_type, data));
    packer.pack32(
            get_checksum(_checksum_type, header_buf, ENTRY_HEADER_SIZE - 4));

    butil::IOBuf header;
    header.append(header_buf, ENTRY_HEADER_SIZE);
    const size_t to_write = header.length() + data.length();
    butil::IOBuf* pieces[2] = {&header, &data};
    size_t start = 0;
    ssize_t written = 0;
    while (written < (ssize_t)to_write) {
        const ssize_t n = butil::IOBuf::cut_multiple_into_file_descriptor(
                _meta_fd, pieces + start, ARRAY_SIZE(pieces) - start);
        if (n < 0) {
            LOG(ERROR) << "Fail to write to meta fd: " << _meta_fd
                       << ", path: " << _path << berror();
            return -1;
        }
        written += n;
        for (; start < ARRAY_SIZE(pieces) && pieces[start]->empty(); ++start) {
        }
    }

    write_meta_latency << (butil::gettimeofday_us() - start_us);
    write_meta_size << to_write;

    BAIDU_SCOPED_LOCK(_mutex);
    _offset_and_term.push_back(std::make_pair(_meta_bytes, entry->id.term));
    _data_offset.push_back(data_offset);
    _last_index.fetch_add(1, butil::memory_order_relaxed);
    _meta_bytes += to_write;

    CHECK(_offset_and_term.size() == _data_offset.size())
            << " offset_and_term size: " << _offset_and_term.size()
            << ", data_offset size: " << _data_offset.size();

    RAFTLOG_V2_VLOG << "append entry to `" << _meta_path
                    << "', index: " << entry->id.index
                    << ", type: " << entry->type
                    << ", meta bytes: " << _meta_bytes
                    << ", data bytes: " << _data_bytes
                    << ", last data offset: " << _data_offset.back();

    return 0;
}

int CurveSegment::_handle_user_request_data(butil::IOBuf* data,
                                            int64_t* data_offset) {
    assert(data != nullptr);
    assert(data_offset != nullptr);

    static bvar::LatencyRecorder handle_request_data_latency(
            "curve_segment_handle_request_data");
    static bvar::LatencyRecorder write_data_latency("curve_segment_write_data");

    auto start_us = butil::gettimeofday_us();
    const size_t origin_data_size = data->size();

    // Decode request size from data
    char raw_req_size_buf[sizeof(uint32_t)];
    uint32_t req_size = 0;
    const void* p = data->fetch(raw_req_size_buf, sizeof(raw_req_size_buf));
    CHECK(p != nullptr);
    ::memcpy(&req_size, p, sizeof(req_size));
    req_size = butil::NetToHost32(req_size);

    // user IO data size
    const size_t user_data_size = data->size() - sizeof(uint32_t) - req_size;

    static const uint32_t max_data_file_size =
            _data_file_pool->GetFilePoolOpt().metaPageSize +
            _data_file_pool->GetFilePoolOpt().fileSize;
    CHECK(user_data_size <= max_data_file_size)
            << " user_data_size: " << user_data_size
            << ", max_data_file_size: " << max_data_file_size;
    if (user_data_size + _data_bytes > max_data_file_size) {
        _is_full = true;
        return ENOSPC;
    }

    // 写入data file的offset
    const int64_t user_data_offset = _data_bytes;
    char* data_buf = static_cast<char*>(
            aligned_alloc(kDirectIOBufferAlignment, user_data_size));
    CHECK(reinterpret_cast<size_t>(data_buf) % 4096 == 0);

    // TODO(wuhanqing): align data
    data->copy_to(data_buf, /*bytes=*/user_data_size,
                  /*pos=*/sizeof(uint32_t) + req_size);

    auto pwrite_start_us = butil::gettimeofday_us();
    ssize_t written =
            pwrite(_data_fd, data_buf, user_data_size, user_data_offset);
    write_data_latency << (butil::gettimeofday_us() - pwrite_start_us);

    CHECK(written == (ssize_t)user_data_size)
            << " written: " << written << ", error: " << berror();
    _data_bytes += written;

    // user meta data
    // Format
    // |-- user data offset  (4 bytes) --|
    // |-- user data size    (4 bytes) --|
    // |-- user data checksum(4 bytes) --|
    char user_meta_data_buf[kMetaDataSize];
    const auto cksum = get_checksum(_checksum_type, data_buf, user_data_size);
    butil::RawPacker(user_meta_data_buf)
            .pack32((uint32_t)user_data_offset)
            .pack32((uint32_t)user_data_size)
            .pack32(cksum);

    RAFTLOG_V2_VLOG << "write IO data to `" << _data_path
                    << "', user data, offset: " << user_data_offset
                    << ", size: " << user_data_size << ", cksum: " << cksum;

    free(data_buf);

    butil::IOBuf user_meta_data;
    user_meta_data.append(user_meta_data_buf, kMetaDataSize);
    data->append_to(&user_meta_data, sizeof(uint32_t) + req_size);
    data->swap(user_meta_data);

    handle_request_data_latency << (butil::gettimeofday_us() - start_us);

    *data_offset = user_data_offset;
    return 0;
}

int CurveSegment::_get_meta(int64_t index, LogMeta* meta) const {
    BAIDU_SCOPED_LOCK(_mutex);
    if (index > _last_index.load(butil::memory_order_relaxed) ||
        index < _first_index) {
        // out of range
        BRAFT_VLOG << "_last_index="
                   << _last_index.load(butil::memory_order_relaxed)
                   << " _first_index=" << _first_index << ", index=" << index;
        return -1;
    } else if (_last_index == _first_index - 1) {
        BRAFT_VLOG << "_last_index="
                   << _last_index.load(butil::memory_order_relaxed)
                   << " _first_index=" << _first_index << ", index=" << index;
        // empty
        return -1;
    }

    int64_t meta_index = index - _first_index;
    int64_t entry_cursor = _offset_and_term[meta_index].first;
    int64_t next_cursor =
            (index < _last_index.load(butil::memory_order_relaxed))
                    ? _offset_and_term[meta_index + 1].first
                    : _meta_bytes;
    DCHECK_LT(entry_cursor, next_cursor);
    meta->offset = entry_cursor;
    meta->term = _offset_and_term[meta_index].second;
    meta->length = next_cursor - entry_cursor;
    return 0;
}

braft::LogEntry* CurveSegment::get(int64_t index) {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        LOG(WARNING) << "get meta failed, index: " << index;
        return nullptr;
    }

    bool ok = true;
    braft::LogEntry* entry = nullptr;
    do {
        braft::ConfigurationPBMeta configuration_meta;
        EntryHeader header;
        butil::IOBuf data;
        if (_load_entry(meta.offset, &header, &data, meta.length,
                        /*is_loading=*/false) != 0) {
            ok = false;
            break;
        }
        CHECK_EQ(meta.term, header.term);
        entry = new braft::LogEntry();
        entry->AddRef();
        switch (header.type) {
            case braft::ENTRY_TYPE_DATA:
                entry->data.swap(data);
                break;
            case braft::ENTRY_TYPE_NO_OP:
                CHECK(data.empty()) << "Data of NO_OP must be empty";
                break;
            case braft::ENTRY_TYPE_CONFIGURATION: {
                butil::Status status = parse_configuration_meta(data, entry);
                if (!status.ok()) {
                    LOG(WARNING) << "Fail to parse ConfigurationPBMeta, path: "
                                 << _path;
                    ok = false;
                    break;
                }
            } break;
            default:
                CHECK(false) << "Unknown entry type, path: " << _path;
                break;
        }

        if (!ok) {
            break;
        }
        entry->id.index = index;
        entry->id.term = header.term;
        entry->type = (braft::EntryType)header.type;
    } while (false);

    if (!ok && entry != nullptr) {
        entry->Release();
        entry = nullptr;
    }
    return entry;
}

int64_t CurveSegment::get_term(int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return 0;
    }
    return meta.term;
}

int CurveSegment::truncate(int64_t last_index_kept) {
    int64_t truncate_size = 0;
    int64_t data_bytes = 0;
    int64_t first_truncate_in_offset = 0;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    if (last_index_kept >= _last_index) {
        return 0;
    }

    first_truncate_in_offset = last_index_kept + 1 - _first_index;

    truncate_size = _offset_and_term[first_truncate_in_offset].first;
    data_bytes = _data_offset[first_truncate_in_offset];

    BRAFT_VLOG << "Truncating " << _path << " first_index: " << _first_index
               << " last_index from " << _last_index << " to "
               << last_index_kept << " truncate size to " << truncate_size;

    lck.unlock();

    // Truncate on a full segment need to rename back to inprogress segment
    // again, because the node may crash before truncate.
    if (!_is_open) {
        std::string old_path(_path);
        butil::string_appendf(&old_path,
                              "/" CURVE_SEGMENT_META_PATH_CLOSE_PATTERN,
                              _first_index, _last_index.load());

        std::string new_path(_path);
        butil::string_appendf(&new_path,
                              "/" CURVE_SEGMENT_META_PATH_OPEN_PATTERN,
                              _first_index);
        int ret = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, ret == 0)
                << "Renamed `" << old_path << "' to `" << new_path << '\'';
        LOG_IF(ERROR, ret != 0) << "Fail to rename `" << old_path << "' to `"
                                << new_path << "', " << berror();
        if (ret != 0) {
            return ret;
        }

        _meta_path = std::move(new_path);
        _is_open = true;
    }

    // truncate fd
    int ret = ftruncate_uninterrupted(_meta_fd, truncate_size);
    if (ret < 0) {
        return ret;
    }

    // seek fd
    off_t ret_off = ::lseek(_meta_fd, truncate_size, SEEK_SET);
    if (ret_off < 0) {
        PLOG(ERROR) << "Fail to lseek fd=" << _meta_fd
                    << " to size=" << truncate_size << " path: " << _path;
        return -1;
    }

    lck.lock();

    // update memory var
    _offset_and_term.resize(first_truncate_in_offset);
    _data_offset.resize(first_truncate_in_offset);
    _last_index.store(last_index_kept, butil::memory_order_relaxed);
    _meta_bytes = truncate_size;
    _data_bytes = data_bytes;
    _is_full = false;

    LOG(INFO) << "truncate `" << _meta_path
              << "', last_index_kept: " << last_index_kept
              << ", meta bytes: " << _meta_bytes
              << ", data bytes: " << _data_bytes
              << ", last data offset: " << [this]() {
                     if (_data_offset.empty()) {
                         return std::string{"empty"};
                     }

                     return std::to_string(_data_offset.back());
                 }();

    return ret;
}

std::shared_ptr<FilePool> CurveSegmentLogStorage::_data_file_pool;

int CurveSegmentLogStorage::init(
        braft::ConfigurationManager* configuration_manager) {
    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, braft::FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Failed to create " << dir_path.value() << " : " << e;
        return -1;
    }

    if (butil::crc32c::IsFastCrc32Supported()) {
        _checksum_type = CHECKSUM_CRC32;
        LOG_ONCE(INFO)
                << "Use crc32c as the checksum type of appending entries";
    } else {
        _checksum_type = CHECKSUM_MURMURHASH32;
        LOG_ONCE(INFO)
                << "Use murmurhash32 as the checksum type of appending entries";
    }

    int ret = 0;
    bool is_empty = false;
    do {
        ret = load_meta();
        if (ret != 0 && errno == ENOENT) {
            LOG(WARNING) << _path << " is empty";
            is_empty = true;
        } else if (ret != 0) {
            break;
        }

        ret = list_segments(is_empty);
        if (ret != 0) {
            break;
        }

        ret = load_segments(configuration_manager);
        if (ret != 0) {
            break;
        }
    } while (false);

    if (is_empty) {
        _first_log_index.store(1);
        _last_log_index.store(0);
        ret = save_meta(1);
    }

    return ret;
}

int64_t CurveSegmentLogStorage::first_log_index() {
    return _first_log_index.load(butil::memory_order_acquire);
}

int64_t CurveSegmentLogStorage::last_log_index() {
    return _last_log_index.load(butil::memory_order_acquire);
}

scoped_refptr<CurveSegment> CurveSegmentLogStorage::open_segment() {
    scoped_refptr<CurveSegment> prev_open_segment;

    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (!_open_segment) {
            _open_segment =
                    new CurveSegment(_path, last_log_index() + 1,
                                     _checksum_type, _data_file_pool.get());
            if (_open_segment->create() != 0) {
                _open_segment = nullptr;
                return nullptr;
            }
        }

        if (_open_segment->is_full()) {
            _segments[_open_segment->first_index()] = _open_segment;
            prev_open_segment.swap(_open_segment);
        }
    }

    do {
        if (prev_open_segment) {
            if (prev_open_segment->close(_enable_sync) == 0) {
                BAIDU_SCOPED_LOCK(_mutex);
                _open_segment =
                        new CurveSegment(_path, last_log_index() + 1,
                                         _checksum_type, _data_file_pool.get());
                if (_open_segment->create() == 0) {
                    // success
                    break;
                }
            }
            PLOG(ERROR) << "Fail to close old open_segment or create new"
                        << " open_segment path: " << _path;
            // Failed, revert former changes
            BAIDU_SCOPED_LOCK(_mutex);
            _segments.erase(prev_open_segment->first_index());
            _open_segment.swap(prev_open_segment);
            return NULL;
        }
    } while (false);

    return _open_segment;
}

int CurveSegmentLogStorage::append_entry(const braft::LogEntry* entry) {
    do {
        scoped_refptr<CurveSegment> segment = open_segment();
        if (nullptr == segment) {
            return EIO;
        }

        int ret = segment->append(entry);
        if (ret == 0) {
            _last_log_index.fetch_add(1, butil::memory_order_release);
            return segment->sync(_enable_sync);
        }

        if (ret == ENOSPC) {
            continue;
        }

        if (EEXIST == ret && entry->id.term != get_term(entry->id.index)) {
            return EINVAL;
        }

        return ret;
    } while (true);

    return false;
}

int CurveSegmentLogStorage::append_entries(
        const std::vector<braft::LogEntry*>& entries) {
    if (entries.empty()) {
        return 0;
    }

    const auto last_log_index =
            _last_log_index.load(butil::memory_order_relaxed);
    if (last_log_index + 1 != entries.front()->id.index) {
        LOG(FATAL) << "There's gap between appending entries and"
                   << " _last_log_index path: " << _path
                   << ", last log index: " << last_log_index
                   << ", new coming log index: " << entries.front()->id.index;
        return -1;
    }

    scoped_refptr<CurveSegment> last_segment;
    size_t i = 0;
    while (i < entries.size()) {
        braft::LogEntry* entry = entries[i];

        scoped_refptr<CurveSegment> segment = open_segment();
        if (nullptr == segment) {
            return i;
        }

        int ret = segment->append(entry);
        if (0 != ret) {
            if (ENOSPC == ret) {
                continue;
            }

            return i;
        }

        _last_log_index.fetch_add(1, butil::memory_order_release);
        last_segment = segment;
        ++i;
    }

    last_segment->sync(_enable_sync);
    return entries.size();
}

int CurveSegmentLogStorage::save_meta(int64_t index) {
    butil::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" CURVE_SEGMENT_META_PATH);

    braft::LogPBMeta meta;
    meta.set_first_log_index(index);
    braft::ProtoBufFile pb_file(meta_path);
    int ret = pb_file.save(&meta, braft::raft_sync_meta());

    timer.stop();
    PLOG_IF(ERROR, ret != 0) << "Fail to save meta to " << meta_path;
    LOG(INFO) << "log save_meta " << meta_path << " first_log_index: " << index
              << " time(us): " << timer.u_elapsed();
    return ret;
}

int CurveSegmentLogStorage::load_meta() {
    butil::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" CURVE_SEGMENT_META_PATH);

    braft::ProtoBufFile pb_file(meta_path);
    braft::LogPBMeta meta;
    if (0 != pb_file.load(&meta)) {
        PLOG_IF(ERROR, errno != ENOENT)
                << "Fail to load meta from " << meta_path;
        return -1;
    }

    _first_log_index.store(meta.first_log_index());

    timer.stop();
    LOG(INFO) << "log load_meta " << meta_path
              << " first_log_index: " << meta.first_log_index()
              << " time: " << timer.u_elapsed();
    return 0;
}

int CurveSegmentLogStorage::list_segments(bool is_empty) {
    butil::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION."
                     << " path: " << _path << ", error: " << berror();
        return -1;
    }

    std::map<int64_t, std::string> data_segments;

    // restore segment meta
    while (dir_reader.Next()) {
        // unlink unneeded segments and unfinished unlinked segments
        // 1. if is_empty == true, remove files starts with `log_`
        // 2. remove files that ends with `.tmp`
        if ((is_empty &&
             0 == strncmp(dir_reader.name(), "log_", strlen("log_"))) ||
            (0 == strncmp(dir_reader.name() +
                                  (strlen(dir_reader.name()) - strlen(".tmp")),
                          ".tmp", strlen(".tmp")))) {
            const std::string segment_path =
                    path_join(_path, dir_reader.name());
            ::unlink(segment_path.c_str());

            LOG(WARNING) << "unlink unused segment, path: " << segment_path;

            continue;
        }

        // recycle unused data segment file
        if (is_empty &&
            (0 == strncmp(dir_reader.name(), CURVE_SEGMENT_DATA_PREFIX,
                          strlen(CURVE_SEGMENT_DATA_PREFIX)))) {
            const std::string data_path = path_join(_path, dir_reader.name());
            if (recycle_data_file(_data_file_pool.get(), data_path) != 0) {
                LOG(WARNING) << "Failed to recycle data file, path `"
                             << data_path << "'";
                return -1;
            }

            continue;
        }

        int match = 0;
        int64_t first_index = 0;
        int64_t last_index = 0;
        match = sscanf(dir_reader.name(), CURVE_SEGMENT_META_PATH_CLOSE_PATTERN,
                       &first_index, &last_index);
        if (match == 2) {
            LOG(INFO) << "restore closed segment, path: `" << _path
                      << "' first_index: " << first_index
                      << " last_index: " << last_index;
            CurveSegment* segment =
                    new CurveSegment(_path, first_index, last_index,
                                     _checksum_type, _data_file_pool.get());
            _segments[first_index] = segment;
            continue;
        }

        match = sscanf(dir_reader.name(), CURVE_SEGMENT_META_PATH_OPEN_PATTERN,
                       &first_index);
        if (match == 1) {
            BRAFT_VLOG << "restore open segment, path: " << _path
                       << " first_index: " << first_index;
            if (!_open_segment) {
                _open_segment =
                        new CurveSegment(_path, first_index, _checksum_type,
                                         _data_file_pool.get());
                continue;
            } else {
                LOG(WARNING) << "open segment conflict, path: " << _path
                             << " first_index: " << first_index;
                return -1;
            }
        }

        match = sscanf(dir_reader.name(), CURVE_SEGMENT_DATA_FILE_PATTERN,
                       &first_index);
        if (match == 1) {
            BRAFT_VLOG << "found data segment, path: `" << _path
                       << "', first_index: " << first_index;
            data_segments.emplace(first_index, dir_reader.name());
        }
    }

    // check segment
    int64_t last_log_index = -1;
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end();) {
        CurveSegment* segment = it->second.get();
        if (segment->first_index() > segment->last_index()) {
            LOG(WARNING) << "closed segment is bad, path: " << _path
                         << " first_index: " << segment->first_index()
                         << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index != -1 &&
                   segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "closed segment not in order, path: " << _path
                         << " first_index: " << segment->first_index()
                         << " last_log_index: " << last_log_index;
            return -1;
        } else if (last_log_index == -1 &&
                   _first_log_index.load(butil::memory_order_acquire) <
                           segment->first_index()) {
            LOG(WARNING) << "closed segment has hole, path: " << _path
                         << " first_log_index: "
                         << _first_log_index.load(butil::memory_order_relaxed)
                         << " first_index: " << segment->first_index()
                         << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index == -1 &&
                   _first_log_index > segment->last_index()) {
            LOG(WARNING) << "closed segment need discard, path: " << _path
                         << " first_log_index: "
                         << _first_log_index.load(butil::memory_order_relaxed)
                         << " first_index: " << segment->first_index()
                         << " last_index: " << segment->last_index();
            segment->unlink();
            _segments.erase(it++);
            continue;
        }

        last_log_index = segment->last_index();
        ++it;
    }
    if (_open_segment) {
        if (last_log_index == -1 &&
            _first_log_index.load(butil::memory_order_relaxed) <
                    _open_segment->first_index()) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                         << " first_log_index: "
                         << _first_log_index.load(butil::memory_order_relaxed)
                         << " first_index: " << _open_segment->first_index();
        } else if (last_log_index != -1 &&
                   _open_segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                         << " first_log_index: "
                         << _first_log_index.load(butil::memory_order_relaxed)
                         << " first_index: " << _open_segment->first_index();
        }
        CHECK_LE(last_log_index, _open_segment->last_index());
    }

    // delete unpaired data segments
    for (auto it = data_segments.begin(); it != data_segments.end();) {
        if (_segments.count(it->first) != 0 ||
            (_open_segment != nullptr &&
             it->first == _open_segment->first_index())) {
            ++it;
            // LOG(INFO) << "found data segment: " << it->first;
            continue;
        }

        const std::string data_path = path_join(_path, it->second);
        if (recycle_data_file(_data_file_pool.get(), data_path) != 0) {
            LOG(WARNING) << "Failed to recycle data file, path `" << data_path
                         << "'";
            return -1;
        }

        it = data_segments.erase(it);
    }

    return 0;
}

int CurveSegmentLogStorage::load_segments(
        braft::ConfigurationManager* configuration_manager) {
    int ret = 0;

    // closed segments
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ++it) {
        CurveSegment* segment = it->second.get();
        LOG(INFO) << "load closed segment, path: `" << _path
                  << "' first_index: " << segment->first_index()
                  << " last_index: " << segment->last_index();
        ret = segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        _last_log_index.store(segment->last_index(),
                              butil::memory_order_release);
    }

    // open segment
    if (_open_segment) {
        LOG(INFO) << "load open segment, path: `" << _path
                  << "' first_index: " << _open_segment->first_index();
        ret = _open_segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        if (_first_log_index.load() > _open_segment->last_index()) {
            LOG(WARNING) << "open segment need discard, path: " << _path
                         << " first_log_index: " << _first_log_index.load()
                         << " first_index: " << _open_segment->first_index()
                         << " last_index: " << _open_segment->last_index();
            _open_segment->unlink();
            _open_segment = NULL;
        } else {
            _last_log_index.store(_open_segment->last_index(),
                                  butil::memory_order_release);
        }
    }
    if (_last_log_index == 0) {
        _last_log_index = _first_log_index - 1;
    }
    return 0;
}

braft::LogStorage* CurveSegmentLogStorage::new_instance(
        const std::string& uri) const {
    return new CurveSegmentLogStorage(uri, true);
}

void CurveSegmentLogStorage::pop_segments(
        int64_t first_index_kept,
        std::vector<scoped_refptr<CurveSegment>>* popped) {
    popped->clear();
    popped->reserve(32);
    BAIDU_SCOPED_LOCK(_mutex);
    _first_log_index.store(first_index_kept, butil::memory_order_release);
    for (SegmentMap::iterator it = _segments.begin(); it != _segments.end();) {
        scoped_refptr<CurveSegment>& segment = it->second;
        if (segment->last_index() < first_index_kept) {
            popped->push_back(segment);
            _segments.erase(it++);
        } else {
            return;
        }
    }
    if (_open_segment) {
        if (_open_segment->last_index() < first_index_kept) {
            popped->push_back(_open_segment);
            _open_segment = nullptr;
            // _log_storage is empty
            _last_log_index.store(first_index_kept - 1);
        } else {
            CHECK(_open_segment->first_index() <= first_index_kept);
        }
    } else {
        // _log_storage is empty
        _last_log_index.store(first_index_kept - 1);
    }
}

int CurveSegmentLogStorage::truncate_prefix(int64_t first_index_kept) {
    // segment files
    if (_first_log_index.load(butil::memory_order_acquire) >=
        first_index_kept) {
        BRAFT_VLOG << "Nothing is going to happen since _first_log_index="
                   << _first_log_index.load(butil::memory_order_relaxed)
                   << " >= first_index_kept=" << first_index_kept;
        return 0;
    }

    // NOTE: truncate_prefix is not important, as it has nothing to do with
    // consensus. We try to save meta on the disk first to make sure even if
    // the deleting fails or the process crashes (which is unlikely to happen).
    // The new process would see the latest `first_log_index'
    if (save_meta(first_index_kept) != 0) {  // NOTE
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    std::vector<scoped_refptr<CurveSegment>> popped;
    pop_segments(first_index_kept, &popped);
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = nullptr;
    }

    return 0;
}

void CurveSegmentLogStorage::pop_segments_from_back(
        int64_t last_index_kept,
        std::vector<scoped_refptr<CurveSegment>>* popped,
        scoped_refptr<CurveSegment>* last_segment) {
    popped->clear();
    popped->reserve(32);
    *last_segment = nullptr;
    BAIDU_SCOPED_LOCK(_mutex);
    _last_log_index.store(last_index_kept, butil::memory_order_release);
    if (_open_segment) {
        if (_open_segment->first_index() <= last_index_kept) {
            *last_segment = _open_segment;
            return;
        }
        popped->push_back(_open_segment);
        _open_segment = nullptr;
    }
    for (SegmentMap::reverse_iterator it = _segments.rbegin();
         it != _segments.rend(); ++it) {
        if (it->second->first_index() <= last_index_kept) {
            // Not return as we need to maintain _segments at the end of this
            // routine
            break;
        }
        popped->push_back(it->second);
        // XXX: C++03 not support erase reverse_iterator
    }
    for (size_t i = 0; i < popped->size(); i++) {
        _segments.erase((*popped)[i]->first_index());
    }
    if (_segments.rbegin() != _segments.rend()) {
        *last_segment = _segments.rbegin()->second;
    } else {
        // all the logs have been cleared, the we move _first_log_index to the
        // next index
        _first_log_index.store(last_index_kept + 1,
                               butil::memory_order_release);
    }
}

int CurveSegmentLogStorage::truncate_suffix(int64_t last_index_kept) {
    std::vector<scoped_refptr<CurveSegment>> popped;
    scoped_refptr<CurveSegment> last_segment;
    pop_segments_from_back(last_index_kept, &popped, &last_segment);
    bool truncate_last_segment = false;
    int ret = -1;

    if (last_segment) {
        if (_first_log_index.load(butil::memory_order_relaxed) <=
            _last_log_index.load(butil::memory_order_relaxed)) {
            truncate_last_segment = true;
        } else {
            // truncate_prefix() and truncate_suffix() to discard entire logs
            BAIDU_SCOPED_LOCK(_mutex);
            popped.push_back(last_segment);
            _segments.erase(last_segment->first_index());
            if (_open_segment) {
                CHECK(_open_segment.get() == last_segment.get());
                _open_segment = nullptr;
            }
        }
    }

    // The truncate suffix order is crucial to satisfy log matching property of
    // raft log must be truncated from back to front.
    for (size_t i = 0; i < popped.size(); ++i) {
        ret = popped[i]->unlink();
        if (ret != 0) {
            return ret;
        }
        popped[i] = nullptr;
    }
    if (truncate_last_segment) {
        bool closed = !last_segment->is_open();
        ret = last_segment->truncate(last_index_kept);
        if (ret == 0 && closed && last_segment->is_open()) {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK(!_open_segment);
            _open_segment.swap(last_segment);
        }
    }

    return ret;
}

int CurveSegmentLogStorage::reset(int64_t next_log_index) {
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index
                   << " path: " << _path;
        return EINVAL;
    }

    std::vector<scoped_refptr<CurveSegment>> popped;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    popped.reserve(_segments.size());
    for (SegmentMap::const_iterator it = _segments.begin();
         it != _segments.end(); ++it) {
        popped.push_back(it->second);
    }

    _segments.clear();
    if (_open_segment) {
        popped.push_back(_open_segment);
        _open_segment = nullptr;
    }

    _first_log_index.store(next_log_index, butil::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, butil::memory_order_relaxed);
    lck.unlock();

    // NOTE: see the comments in truncate_prefix
    if (save_meta(next_log_index) != 0) {
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = nullptr;
    }

    return 0;
}

braft::LogEntry* CurveSegmentLogStorage::get_entry(int64_t index) {
    scoped_refptr<CurveSegment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return nullptr;
    }

    return ptr->get(index);
}

int64_t CurveSegmentLogStorage::get_term(int64_t index) {
    scoped_refptr<CurveSegment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return 0;
    }
    return ptr->get_term(index);
}

int CurveSegmentLogStorage::get_segment(int64_t index,
                                        scoped_refptr<CurveSegment>* ptr) {
    BAIDU_SCOPED_LOCK(_mutex);
    int64_t first_index = first_log_index();
    int64_t last_index = last_log_index();
    if (first_index == last_index + 1) {
        return -1;
    }
    if (index < first_index || index > last_index + 1) {
        LOG_IF(WARNING, index > last_index)
                << "Attempted to access entry " << index << " outside of log, "
                << " first_log_index: " << first_index
                << " last_log_index: " << last_index;
        return -1;
    } else if (index == last_index + 1) {
        return -1;
    }

    if (_open_segment && index >= _open_segment->first_index()) {
        *ptr = _open_segment;
        CHECK(ptr->get() != nullptr);
    } else {
        CHECK(!_segments.empty());
        SegmentMap::iterator it = _segments.upper_bound(index);
        SegmentMap::iterator saved_it = it;
        --it;
        CHECK(it != saved_it);
        *ptr = it->second;
    }
    return 0;
}

bool CurveSegmentLogStorage::IsWALDataFile(const std::string& filename) {
    int match = 0;
    int first_index = 0;

    match = sscanf(filename.c_str(), CURVE_SEGMENT_DATA_FILE_PATTERN,
                   &first_index);
    return match == 1;
}

void RegisterCurveSegmentLogStorageV2OrDie() {
    static CurveSegmentLogStorage instance;
    braft::log_storage_extension()->RegisterOrDie("curve", &instance);
}

}  // namespace raftlog_v2
}  // namespace chunkserver
}  // namespace curve
