
SET enable_brute_force_vector_search = 1;
SET allow_experimental_inverted_index = 1;

DROP TABLE IF EXISTS t_hybrid_search_rsf_all;
DROP TABLE IF EXISTS t_hybrid_search_rsf_local;
CREATE TABLE t_hybrid_search_rsf_all(
    id UInt32, vector_all Array(Float32), text_all String,
    CONSTRAINT check_length CHECK length(vector_all) = 3
) ENGINE=Distributed('test_shard_localhost', currentDatabase(), 't_hybrid_search_rsf_local');

CREATE TABLE t_hybrid_search_rsf_local(
    id UInt32, vector_all Array(Float32), text_all String,
    INDEX inv_idx(text_all) TYPE fts GRANULARITY 1,
    CONSTRAINT check_length CHECK length(vector_all) = 3
) ENGINE=MergeTree ORDER BY id;

SELECT 'rsf hybrid search on empty distributed table';
SELECT id, HybridSearch('fusion_type=rsf')(vector_all, text_all, [1.0,1,1], 'hybrid') AS score
FROM t_hybrid_search_rsf_all
ORDER BY score DESC
LIMIT 5;

SELECT id, TextSearch(text_all, 'hybrid') AS bm25_score
FROM t_hybrid_search_rsf_all ORDER BY bm25_score LIMIT 5; -- { serverError SYNTAX_ERROR }

DROP TABLE IF EXISTS t_hybrid_search_rsf_all;
DROP TABLE IF EXISTS t_hybrid_search_rsf_local;
