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


#include "src/client/libchunk/chunk_client.h"

#include <thread>  // NOLINT
#include <chrono>  // NOLINT

#include "src/common/string_util.h"
#include "src/common/net_common.h"

namespace curve {
namespace client {

void ChunkClosure::Run() {
    std::unique_ptr<ChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    if (cntl_->Failed()) {
        aioCtx_->ret = -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
    } else {
        int status = response_->status();
        switch(status) {
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            aioCtx_->ret = LIBCHUNK_ERROR::LIBCHUNK_OK;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED:
            aioCtx_->ret = -LIBCHUNK_ERROR::LIBCHUNK_REDIRECTED;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST:
            aioCtx_->ret = -LIBCHUNK_ERROR::LIBCHUNK_NOTEXIST;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD:
            aioCtx_->ret = -LIBCHUNK_ERROR::LIBCHUNK_OVERLOAD;
            break;
        default:
            aioCtx_->ret = -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
            break;
        }
        if (LIBCURVE_OP::LIBCURVE_OP_READ  == aioCtx_->op &&
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
            size_t len = cntl_->response_attachment().copy_to(
                aioCtx_->buf, aioCtx_->length);
            if (len != aioCtx_->length) {
                aioCtx_->ret = -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
            }
        }
    }
    aioCtx_->cb(aioCtx_);
    return;
}

int ChunkClient::GetLeader(uint64_t logicalPoolId,
    uint32_t copysetId,
    std::string *leader) {
    *leader = "";
    brpc::Controller cntl;
    cntl.set_timeout_ms(500);

    CliService2_Stub stub(&channel_);

    GetLeaderRequest2 request;
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);

    GetLeaderResponse2 response;
    uint32_t retry = 0;
    do {
        cntl.Reset();
        cntl.set_timeout_ms(ops_->rpcTimeoutMs);
        stub.GetLeader(&cntl,
            &request,
            &response,
            nullptr);
        LOG(INFO) << "Send GetLeader[log_id=" << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [GetLeaderRequest] "
                  << request.DebugString();
        if (cntl.Failed()) {
            LOG(WARNING) << "Send GetLeader error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(ops_->rpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < ops_->rpcMaxRetryTimes);

    if (cntl.Failed()) {
        LOG(ERROR) << "Send GetLeader error, retry fail,"
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
    } else {
        LOG(INFO) << "Received GetLeaderResponse[log_id="
                  << cntl.log_id()
                  << "] from " << cntl.remote_side()
                  << " to " << cntl.local_side()
                  << ". [GetLeaderResponse] "
                  << response.DebugString();
        std::string peerId = response.leader().address();
        std::vector<std::string> items;
        curve::common::SplitString(peerId, ":", &items);
        if (items.size() >= 2) {
            *leader = items[0] + items[1];
        } else {
            LOG(ERROR) << "Received GetLeaderResponse leaderPeer abnomal, "
                       << "peerId = " << peerId;
            return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
        }
    }
    return LIBCHUNK_ERROR::LIBCHUNK_OK;
}

int ChunkClient::Init(const ChunkOptions *ops) {
    ops_ = ops;
    int ret = mdsClient_->Init(ops_->mdsAddrs, ops);
    if (ret < 0) {
        LOG(ERROR) << "Init mdsClient_ failed, ret = " << ret;
        return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
    }

    if (channel_.Init(ops_->chunkserverAddr, NULL)) {
        LOG(ERROR) << "Fail to init channel to " << ops_->chunkserverAddr;
        return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
    }

    std::string leader = "";
    if (kEmptyLogicalPoolId == ops_->logicalpoolid ||
        kEmptyCopysetId == ops_->copysetid) {
        curTestMod_ = TestMode::ChunkServer;

        std::vector<CopysetInfo> copysets;
        mdsClient_->GetCopySetsInChunkServer(ops_->chunkserverAddr, &copysets);
        for (auto& cs : copysets) {
            ret = GetLeader(cs.logicalpoolid(), cs.copysetid(), &leader);
            if (ret < 0) {
                LOG(ERROR) << "GetLeader failed, ret = " << ret;
                return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
            }
            if (leader == ops_->chunkserverAddr) {
                leaderCopysets.emplace_back(cs);
            }
        }
        if (leaderCopysets.size() == 0) {
            LOG(ERROR) << "No leader found in current chunkserver";
            return -LIBCHUNK_ERROR::LIBCHUNK_REDIRECTED;
        }
    } else {
        curTestMod_ = TestMode::Copyset;
        ret = GetLeader(ops_->logicalpoolid, ops_->copysetid, &leader);
        if (ret < 0) {
            LOG(ERROR) << "GetLeader failed, ret = " << ret;
            return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
        }
        if (leader != ops_->chunkserverAddr) {
            LOG(ERROR) << "Current chunkserver is not the leader";
            return -LIBCHUNK_ERROR::LIBCHUNK_REDIRECTED;
        }
    }
}

void ChunkClient::Fini(){
    if (mdsClient_ != nullptr) {
        delete mdsClient_;
        mdsClient_ = nullptr;
    }
}

const uint64_t chunkSize_ = 16ul * 1024 * 1024;

ChunkReqInfo ChunkClient::CalcChunkReqInfo(ChunkAioContext* aioCtx) {
    uint64_t chunkIndex = aioCtx->offset / chunkSize_;

    ChunkReqInfo reqInfo;
    if (TestMode::Copyset == curTestMod_) {
        reqInfo.lpid_ = ops_->logicalpoolid;
        reqInfo.cpid_ = ops_->copysetid;
    } else {
        size_t leaderNum = leaderCopysets.size();
        reqInfo.lpid_ = leaderCopysets[0].logicalpoolid();
        reqInfo.cpid_ = leaderCopysets[chunkIndex % leaderNum].copysetid();
    }
    reqInfo.cid_ = (uint64_t(reqInfo.cpid_) << 32u) + chunkIndex;
    reqInfo.offset = aioCtx->offset % chunkSize_;
    reqInfo.len = aioCtx->length;
}

int ChunkClient::AioWrite(ChunkAioContext* aioCtx){
    ChunkClosure *done = new ChunkClosure(aioCtx);
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(ops_->rpcTimeoutMs);
    ChunkResponse *response = new ChunkResponse();
    done->SetCntl(cntl);
    done->SetResponse(response);

    ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_WRITE);

    ChunkReqInfo reqInfo = CalcChunkReqInfo(aioCtx);
    request.set_logicpoolid(reqInfo.lpid_);
    request.set_copysetid(reqInfo.cpid_);
    request.set_chunkid(reqInfo.cid_);
    request.set_sn(0);
    request.set_offset(reqInfo.offset);
    request.set_size(reqInfo.len);

    cntl->request_attachment().append(aioCtx->buf, aioCtx->length);

    ChunkService_Stub stub(&channel_);
    stub.WriteChunk(cntl, &request, response, doneGuard.release());

    return 0;
}

int ChunkClient::AioRead(ChunkAioContext* aioCtx){
    ChunkClosure *done = new ChunkClosure(aioCtx);
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(ops_->rpcTimeoutMs);
    ChunkResponse *response = new ChunkResponse();

    ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_READ);

    ChunkReqInfo reqInfo = CalcChunkReqInfo(aioCtx);
    request.set_logicpoolid(reqInfo.lpid_);
    request.set_copysetid(reqInfo.cpid_);
    request.set_chunkid(reqInfo.cid_);
    request.set_offset(reqInfo.offset);
    request.set_size(reqInfo.len);

    ChunkService_Stub stub(&channel_);
    stub.ReadChunk(cntl, &request, response, doneGuard.release());

    return 0;
}

}  // namespace client
}  // namespace curve
