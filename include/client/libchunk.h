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

#ifndef INCLUDE_CLIENT_LIBCHUNK_H_
#define INCLUDE_CLIENT_LIBCHUNK_H_

#include "libcurve_define.h"  // NOLINT

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>

enum LIBCHUNK_ERROR {
    LIBCHUNK_OK                      = 0,
    LIBCHUNK_INTERNAL                = 1,
    LIBCHUNK_REDIRECTED              = 2,
    LIBCHUNK_NOTEXIST                = 3,
    LIBCHUNK_OVERLOAD                = 4,
};

typedef void (*ChunkAioCallBack)(struct ChunkAioContext* context);

typedef struct ChunkAioContext {
    off_t               offset;
    size_t              length;
    int                 ret;
    LIBCURVE_OP         op;
    ChunkAioCallBack    cb;
    void*               buf;
} ChunkAioContext;

static int kEmptyLogicalPoolId = -1;
static int kEmptyCopysetId = -1;

typedef struct ChunkOptions {
    bool   inited;
    char*  chunkserverAddr;
    char*  mdsAddrs;
    int    logicalpoolid;
    int    copysetid;
    int    rpcTimeoutMs;
    int    rpcRetryIntervalMs;
    int    rpcMaxRetryTimes;
} ChunkOptions;

int chunk_init(const ChunkOptions* options);
int chunk_fini(void);

int chunk_aio_pread(ChunkAioContext* context);
int chunk_aio_pwrite(ChunkAioContext* context);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // INCLUDE_CLIENT_LIBCHUNK_H_
