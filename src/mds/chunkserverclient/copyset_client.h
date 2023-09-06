/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
 */

#ifndef SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_
#define SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_

#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology.h"

#include "src/mds/chunkserverclient/chunkserver_client.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"
#include "src/common/channel_pool.h"

using ::curve::mds::topology::Topology;
using ::curve::mds::topology::CopySetInfo;

namespace curve {
namespace mds {
namespace chunkserverclient {

class CopysetClientClosure : public Closure {
 public:
    CopysetClientClosure() : err_(kMdsFail) {}
    virtual ~CopysetClientClosure() {}

    void SetErrCode(int ret) {
        err_ = ret;
    }

    int GetErrCode() {
        return err_;
    }

 private:
    int err_;
};

class CopysetClientInterface {
 public:
    virtual ~CopysetClientInterface() {}
    virtual int DeleteChunkSnapshotOrCorrectSn(LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t correctedSn) = 0;
    virtual int DeleteChunkSnapshot(
        uint64_t fileId,
        uint64_t originFileId,
        uint64_t chunkIndex,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t snapSn,
        const std::vector<uint64_t>& snaps) = 0;
    virtual int DeleteChunk(
        uint64_t fileId,
        uint64_t originFileId,
        uint64_t chunkIndex,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn) = 0;
    virtual int FlattenChunk(
        const std::shared_ptr<FlattenChunkContext> &ctx, 
        CopysetClientClosure* done) = 0;
};

class CopysetClient : public CopysetClientInterface {
 public:
    friend class ChunkServerFlattenChunkClosure;
 public:
     /**
      * @brief constructor
      *
      * @param topology
      */
    CopysetClient(std::shared_ptr<Topology> topo,
        const ChunkServerClientOption &option,
        std::shared_ptr<ChannelPool> channelPool)
        : topo_(topo),
          chunkserverClient_(
            std::make_shared<ChunkServerClient>(topo, option, channelPool)),
          updateLeaderRetryTimes_(option.updateLeaderRetryTimes),
          updateLeaderRetryIntervalMs_(option.updateLeaderRetryIntervalMs) {
    }

    void SetChunkServerClient(std::shared_ptr<ChunkServerClient> csClient) {
        chunkserverClient_ = csClient;
    }

    /**
     * @brief delete the snapshot generated during the dump or from history left
     *        over. If no snapshot is generated during the dump, modify the
     *        correctedSn of the chunk
     *
     * @param logicPoolId
     * @param copysetId
     * @param chunkId
     * @param correctedSn the version number that needs to be corrected when
     *                    there is no snapshot file for the chunk
     *
     * @return error code
     */
    int DeleteChunkSnapshotOrCorrectSn(LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t correctedSn) override;

    /**
     * @brief delete a specific snapshot from local multi-level snapshots
     *
     * @param logicPoolId
     * @param copysetId
     * @param chunkId
     * @param snapSn the snapshot sequence that needs to be deleted
     * @param snaps the existing snapshot sequence nums
     * @return error code
     */
    int DeleteChunkSnapshot(
        uint64_t fileId,
        uint64_t originFileId,
        uint64_t chunkIndex,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t snapSn,
        const std::vector<uint64_t>& snaps) override;

    /**
     * @brief delete chunk files that are not snapshot files
     *
     * @param logicPoolId
     * @param copysetId
     * @param chunkId
     * @param sn file version number
     *
     * @return error code
     */
    int DeleteChunk(
        uint64_t fileId,
        uint64_t originFileId,
        uint64_t chunkIndex,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn) override;


    int FlattenChunk(
        const std::shared_ptr<FlattenChunkContext> &ctx, 
        CopysetClientClosure* done) override;

 private:
    /**
     * @brief update leader
     *
     * @param[int][out] copyset
     *
     * @return error code
     */
    int UpdateLeader(CopySetInfo *copyset);

 private:
    std::shared_ptr<Topology> topo_;
    std::shared_ptr<ChunkServerClient> chunkserverClient_;

    uint32_t updateLeaderRetryTimes_;
    uint32_t updateLeaderRetryIntervalMs_;
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_
