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

#include "src/client/libchunk/mds_client.h"

#include "src/common/string_util.h"
#include "src/common/net_common.h"

namespace curve {
namespace client {

int MDSClient::Init(const std::string& mdsAddr, const ChunkOptions *ops) {
    ops_ = ops;
    // 初始化channel
    curve::common::SplitString(mdsAddr, ",", &mdsAddrVec_);
    if (mdsAddrVec_.empty()) {
        std::cout << "Split mds address fail!" << std::endl;
        return -1;
    }
    for (uint64_t i = 0; i < mdsAddrVec_.size(); ++i) {
        if (channel_.Init(mdsAddrVec_[i].c_str(), nullptr) != 0) {
            std::cout << "Init channel to " << mdsAddr << "fail!" << std::endl;
            continue;
        }
        // 寻找哪个mds存活
        curve::mds::topology::ListPhysicalPoolRequest request;
        curve::mds::topology::ListPhysicalPoolResponse response;
        curve::mds::topology::TopologyService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(ops_->rpcTimeoutMs);
        stub.ListPhysicalPool(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            continue;
        }
        currentMdsIndex_ = i;
        return 0;
    }
    std::cout << "Init channel to all mds fail!" << std::endl;
    return -1;
}

bool MDSClient::ChangeMDServer() {
    currentMdsIndex_++;
    if (currentMdsIndex_ > mdsAddrVec_.size() - 1) {
        currentMdsIndex_ = 0;
    }
    if (channel_.Init(mdsAddrVec_[currentMdsIndex_].c_str(),
                                                nullptr) != 0) {
        return false;
    }
    return true;
}

int MDSClient::GetCopySetsInChunkServer(const std::string& csAddr,
                                 std::vector<CopysetInfo>* copysets) {
    assert(copysets != nullptr);
    curve::mds::topology::GetCopySetsInChunkServerRequest request;
    curve::mds::topology::GetCopySetsInChunkServerResponse response;
    if (!curve::common::NetCommon::CheckAddressValid(csAddr)) {
        std::cout << "chunkserver address invalid!" << std::endl;
        return -1;
    }
    std::vector<std::string> strs;
    curve::common::SplitString(csAddr, ":", &strs);
    std::string ip = strs[0];
    uint64_t port;
    curve::common::StringToUll(strs[1], &port);
    request.set_hostip(ip);
    request.set_port(port);
    return GetCopySetsInChunkServer(&request, copysets);
}

int MDSClient::GetCopySetsInChunkServer(
                            GetCopySetsInChunkServerRequest* request,
                            std::vector<CopysetInfo>* copysets) {
    curve::mds::topology::GetCopySetsInChunkServerResponse response;
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    auto fp = &curve::mds::topology::TopologyService_Stub::GetCopySetsInChunkServer;  // NOLINT
    if (SendRpcToMds(request, &response, &stub, fp) != 0) {
        std::cout << "GetCopySetsInChunkServer from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == 0) {
        for (int i =0; i < response.copysetinfos_size(); ++i) {
            copysets->emplace_back(response.copysetinfos(i));
        }
        return 0;
    }
    std::cout << "GetCopySetsInChunkServer fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

template <typename T, typename Request, typename Response>
int MDSClient::SendRpcToMds(Request* request, Response* response, T* obp,
                void (T::*func)(google::protobuf::RpcController*,
                            const Request*, Response*,
                            google::protobuf::Closure*)) {
    int changeTimeLeft = mdsAddrVec_.size() - 1;
    while (changeTimeLeft >= 0) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(ops_->rpcTimeoutMs);
        (obp->*func)(&cntl, request, response, nullptr);
        if (!cntl.Failed()) {
            // 如果成功了，就返回0，对response的判断放到上一层
            return 0;
        }
        bool needRetry = (cntl.ErrorCode() != EHOSTDOWN &&
                          cntl.ErrorCode() != ETIMEDOUT &&
                          cntl.ErrorCode() != brpc::ELOGOFF);
        uint64_t retryTimes = 0;
        while (needRetry && retryTimes < ops_->rpcMaxRetryTimes) {
            cntl.Reset();
            (obp->*func)(&cntl, request, response, nullptr);
            if (cntl.Failed()) {
                retryTimes++;
                continue;
            }
            return 0;
        }
        // 对于需要重试的错误，重试次数用完了还没成功就返回错误不切换
        // ERPCTIMEDOUT比较特殊，这种情况下，mds可能切换了也可能没切换，所以
        // 需要重试并且重试次数用完后切换
        // 只有不需要重试的，也就是mds不在线的才会去切换mds
        if (needRetry && cntl.ErrorCode() != brpc::ERPCTIMEDOUT) {
            std::cout << "Send RPC to mds fail, error content: "
                      << cntl.ErrorText() << std::endl;
            return -1;
        }
        changeTimeLeft--;
        while (!ChangeMDServer() && changeTimeLeft > 0) {
            changeTimeLeft--;
        }
    }
    return -1;
}


}  // namespace client
}  // namespace curve
