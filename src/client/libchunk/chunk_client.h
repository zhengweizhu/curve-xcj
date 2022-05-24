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

/*
 * Project: Curve
 *
 * History:
 *          2022/05/19  xuchaojie  Initial version
 */

#ifndef SRC_CLIENT_LIBCHUNK_CHUNK_CLIENT_H_
#define SRC_CLIENT_LIBCHUNK_CHUNK_CLIENT_H_

#include <brpc/channel.h>

#include "include/client/libchunk.h"
#include "src/client/libchunk/mds_client.h"

#include "proto/common.pb.h"
#include "proto/cli2.pb.h"
#include "proto/chunk.pb.h"

using ::google::protobuf::Closure;
using ::google::protobuf::Message;
using curve::chunkserver::ChunkRequest;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::ChunkService_Stub;
using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::GetLeaderRequest2;
using curve::chunkserver::GetLeaderResponse2;
using curve::chunkserver::CliService2_Stub;

namespace curve {
namespace client {

enum class TestMode {
    ChunkServer = 0,
    Copyset = 1,
};

struct ChunkReqInfo {
    uint64_t cid_ = 0;
    uint32_t cpid_ = 0;
    uint16_t lpid_ = 0;
    off_t offset = 0;
    size_t len = 0;
};

class ChunkClosure : public Closure {
 public:
    ChunkClosure(ChunkAioContext *aioCtx) : aioCtx_(aioCtx) {}

    virtual ~ChunkClosure() = default;

    void Run() override;

    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }

    void SetResponse(Message* response) {
        response_ = static_cast<ChunkResponse*>(response);
    }

 private:
    ChunkAioContext *aioCtx_;

    brpc::Controller *cntl_;
    ChunkResponse *response_;
};

class ChunkClient {
 public:
    ChunkClient() : mdsClient_(new MDSClient()) {}
    ChunkClient(MDSClient *mdsClient) : mdsClient_(mdsClient) {}

    ~ChunkClient() {
        Fini();
    }

    int Init(const ChunkOptions *ops);

    void Fini();

    int AioWrite(ChunkAioContext* aioCtx);

    int AioRead(ChunkAioContext* aioCtx);

 private:
    ChunkReqInfo CalcChunkReqInfo(ChunkAioContext* aioCtx);

    int GetLeader(uint64_t logicalPoolId,
        uint32_t copysetId,
        std::string *leader);

 private:
    brpc::Channel channel_;
    TestMode curTestMod_;
    const ChunkOptions *ops_;

    MDSClient* mdsClient_;
    std::vector<CopysetInfo> leaderCopysets;
};


}  // namespace client
}  // namespace curve



#endif  // SRC_CLIENT_LIBCHUNK_CHUNK_CLIENT_H_
