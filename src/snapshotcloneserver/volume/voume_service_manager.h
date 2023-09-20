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
 * Created Date: 2023-09-25
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_VOLUME_VOUME_SERVICE_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_VOLUME_VOUME_SERVICE_MANAGER_H_


#include <memory>

#include "src/snapshotcloneserver/common/curvefs_client.h"

namespace curve {
namespace snapshotcloneserver {

class VolumeServiceManager {
 public:
     explicit VolumeServiceManager(
        const std::shared_ptr<CurveFsClient> &client)
       : client_(client) {}

    virtual ~VolumeServiceManager() {}

    virtual int CreateFile(const std::string &file,
        const std::string &user,
        uint64_t size,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string &poolset);

    virtual int DeleteFile(const std::string &file,
        const std::string &user);

 private:
    std::shared_ptr<CurveFsClient> client_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_VOLUME_VOUME_SERVICE_MANAGER_H_
