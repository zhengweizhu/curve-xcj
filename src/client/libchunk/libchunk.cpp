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


#include "include/client/libchunk.h"

#include "src/client/libchunk/mds_client.h"
#include "src/client/libchunk/chunk_client.h"

curve::client::ChunkClient *globalclient = nullptr;

extern "C" {

int chunk_init(const ChunkOptions* options) {
    if (globalclient == nullptr) {
        globalclient = new curve::client::ChunkClient();
        int ret = globalclient->Init(options);
        if (ret != LIBCHUNK_ERROR::LIBCHUNK_OK) {
            delete globalclient;
            globalclient = nullptr;
            LOG(ERROR) << "Init chunk client failed, ret = " << ret;
            return ret;
        } else {
            LOG(INFO) << "Init chunk client success";
        }
    }
    return 0;
}

int chunk_fini(void) {
    if (globalclient != nullptr) {
        globalclient->Fini();
        delete globalclient;
        globalclient = nullptr;
        LOG(INFO) << "destory curve client success";
    }
    return 0;
}

int chunk_aio_pread(ChunkAioContext* context) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
    }
    return globalclient->AioRead(context);
}

int chunk_aio_pwrite(ChunkAioContext* context) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCHUNK_ERROR::LIBCHUNK_INTERNAL;
    }
    return globalclient->AioWrite(context);
}

}  // extern "C"

