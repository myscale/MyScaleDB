/*
 * Copyright (2024) MOQI SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <vector>

#include <Common/logger_useful.h>

#include "faiss/impl/io.h"

namespace VectorIndex
{

struct IndexReader : faiss::IOReader
{
    size_t read(void * ptr, size_t size, size_t nitems = 1) { return operator()(ptr, size, nitems); }
};

struct BufferIndexReader : IndexReader
{
    uint8_t * data;
    uint64_t total = 0;
    uint64_t rp = 0;

    size_t operator()(void * ptr, size_t size, size_t nitems) override;
};
}
