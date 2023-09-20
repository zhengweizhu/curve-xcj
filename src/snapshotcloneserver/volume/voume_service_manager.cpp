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

#include "src/snapshotcloneserver/volume/voume_service_manager.h"

#include <memory>

namespace curve {
namespace snapshotcloneserver {

int VolumeServiceManager::CreateFile(const std::string &file,
    const std::string &user,
    uint64_t size,
    uint64_t stripeUnit,
    uint64_t stripeCount,
    const std::string &poolset) {
    int ret = client_->CreateFile(file, user, size, stripeUnit, stripeCount,
        poolset);
    switch (ret) {
        case LIBCURVE_ERROR::OK:
        case -LIBCURVE_ERROR::EXISTS:
            break;
        case -LIBCURVE_ERROR::AUTHFAIL:
            LOG(ERROR) << "CreateFile by invalid user"
                       << ", file = " << file
                       << ", user = " << user
                       << ", size = " << size
                       << ", stripeUnit = " << stripeUnit
                       << ", stripeCount = " << stripeCount
                       << ", poolset = " << poolset;
            return kErrCodeInvalidUser;
        default:
            LOG(ERROR) << "Create fail, ret = " << ret
                       << ", file = " << file
                       << ", user = " << user
                       << ", size = " << size
                       << ", stripeUnit = " << stripeUnit
                       << ", stripeCount = " << stripeCount
                       << ", poolset = " << poolset;
            return kErrCodeInternalError;
    }
    LOG(INFO) << "CreateFile success, file = " << file
              << ", user = " << user
              << ", size = " << size
              << ", stripeUnit = " << stripeUnit
              << ", stripeCount = " << stripeCount
              << ", poolset = " << poolset;
    return kErrCodeSuccess;
}

int VolumeServiceManager::DeleteFile(const std::string &file,
    const std::string &user) {
    int ret = client_->DeleteFile(file, user);
    switch (ret) {
        case LIBCURVE_ERROR::OK:
        case -LIBCURVE_ERROR::NOTEXIST:
            break;
        case -LIBCURVE_ERROR::AUTHFAIL:
            LOG(ERROR) << "DeleteFile by invalid user"
                       << ", file = " << file
                       << ", user = " << user;
            return kErrCodeInvalidUser;
        default:
            LOG(ERROR) << "DeleteFile fail, ret = " << ret
                       << ", file = " << file
                       << ", user = " << user;
            return kErrCodeInternalError;
    }
    LOG(INFO) << "DeleteFile success, file = " << file
              << ", user = " << user;
    return kErrCodeSuccess;
}

}  // namespace snapshotcloneserver
}  // namespace curve
