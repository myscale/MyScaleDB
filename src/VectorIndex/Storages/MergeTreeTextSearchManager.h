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

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <VectorIndex/Storages/HybridSearchResult.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/logger_useful.h>

#if USE_TANTIVY_SEARCH
#include <tantivy_search.h>
#endif

namespace DB
{

/// Text search manager, responsible for full-text search result precompute
class MergeTreeTextSearchManager : public MergeTreeBaseSearchManager
{
public:
    MergeTreeTextSearchManager(
        StorageMetadataPtr metadata_, TextSearchInfoPtr text_search_info_, ContextPtr context_)
        : MergeTreeBaseSearchManager{metadata_, context_}
        , text_search_info(text_search_info_)
    {
    }

    ~MergeTreeTextSearchManager() override = default;

    void executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part) override;

    void executeSearchWithFilter(
        const MergeTreeData::DataPartPtr & data_part,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter) override;

    void mergeResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter = nullptr,
        const ColumnUInt64 * part_offset = nullptr) override;

    bool preComputed() override
    {
        return text_search_result && text_search_result->computed;
    }

    CommonSearchResultPtr getSearchResult() override { return text_search_result; }

#if USE_TANTIVY_SEARCH
    void setBM25Stats(const Statistics & bm25_stats_in_table_)
    {
        bm25_stats_in_table = bm25_stats_in_table_;
    }
#endif

private:

    TextSearchInfoPtr text_search_info;

#if USE_TANTIVY_SEARCH
    Statistics bm25_stats_in_table; /// total bm25 info from all parts in a table
#endif

    /// lock text search result
    std::mutex mutex;
    TextSearchResultPtr text_search_result = nullptr;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeTextSearchManager");

    TextSearchResultPtr textSearch(
        const MergeTreeData::DataPartPtr & data_part = nullptr,
        const Search::DenseBitmapPtr filter = nullptr);
};

using MergeTreeTextSearchManagerPtr = std::shared_ptr<MergeTreeTextSearchManager>;

}
