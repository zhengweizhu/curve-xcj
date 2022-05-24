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

#ifndef SRC_CLIENT_LIBCHUNK_MDS_CLIENT_H_
#define SRC_CLIENT_LIBCHUNK_MDS_CLIENT_H_

#include <brpc/channel.h>

#include <string>
#include <vector>

#include "include/client/libchunk.h"
#include "proto/topology.pb.h"

using ::google::protobuf::Closure;
using ::google::protobuf::Message;
using curve::common::CopysetInfo;
using curve::mds::topology::GetCopySetsInChunkServerRequest;
using curve::mds::topology::GetCopySetsInChunkServerResponse;
using curve::mds::topology::ListPhysicalPoolRequest;
using curve::mds::topology::ListPhysicalPoolResponse;

namespace curve {
namespace client {

class MDSClient {
 public:
    int Init(const std::string& mdsAddr, const ChunkOptions *ops);

    int GetCopySetsInChunkServer(const std::string& csAddr,
                                 std::vector<CopysetInfo>* copysets);

    int GetCopySetsInChunkServer(
                            GetCopySetsInChunkServerRequest* request,
                            std::vector<CopysetInfo>* copysets);

 private:
    template <typename T, typename Request, typename Response>
    int SendRpcToMds(Request* request, Response* response, T* obp,
                void (T::*func)(google::protobuf::RpcController*,
                            const Request*, Response*,
                            google::protobuf::Closure*));

    bool ChangeMDServer();

 private:
    brpc::Channel channel_;

    std::vector<std::string> mdsAddrVec_;

    int currentMdsIndex_;

    const ChunkOptions *ops_;
};

}  // namespace client
}  // namespace curve


#endif  // SRC_CLIENT_LIBCHUNK_MDS_CLIENT_H_
