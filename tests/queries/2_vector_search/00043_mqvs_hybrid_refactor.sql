-- Tags: no-parallel

SET allow_experimental_inverted_index = true;

DROP TABLE IF EXISTS test_vector_invert_multi_parts;
CREATE TABLE test_vector_invert_multi_parts
(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=2;

INSERT INTO test_vector_invert_multi_parts VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.');
INSERT INTO test_vector_invert_multi_parts VALUES (10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

set enable_brute_force_vector_search=1;
SELECT 'hybrid search on two parts with relative score fusion';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score
FROM test_vector_invert_multi_parts
ORDER BY score DESC, id LIMIT 5;

SELECT 'hybrid search on two parts with rank fusion';
SELECT id, hybridsearch('fusion_type=rrf')(vector, doc, [1.0,1,1], 'Ancient') as score
FROM test_vector_invert_multi_parts
ORDER BY score DESC, id LIMIT 5;

SELECT 'hybrid search on two parts rsf with WHERE clause (0 is top 1 in text)';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score
FROM test_vector_invert_multi_parts where id < 10
ORDER BY score DESC, id LIMIT 5;

DROP TABLE test_vector_invert_multi_parts;

DROP TABLE IF EXISTS test_vector_inverted_two_stage SYNC;
CREATE TABLE test_vector_inverted_two_stage
(
    id    UInt32,
    vector  Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT check_length CHECK length(vector) = 16
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_to_build_vector_index=0;

ALTER TABLE test_vector_inverted_two_stage ADD VECTOR INDEX vec_ind vector TYPE MSTG;

INSERT INTO test_vector_inverted_two_stage SELECT number, arrayWithConstant(16, number), number FROM numbers(1001);
SYSTEM WAIT BUILDING VECTOR INDICES test_vector_inverted_two_stage;

SELECT 'hybrid search with two stage enabled';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, arrayWithConstant(16, 1.0), '3') as score
FROM test_vector_inverted_two_stage ORDER BY score DESC LIMIT 5 settings two_stage_search_option=2;

DROP TABLE IF EXISTS test_vector_inverted_two_stage_multi_parts SYNC;
CREATE TABLE test_vector_inverted_two_stage_multi_parts
(
    id    UInt32,
    vector  Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT check_length CHECK length(vector) = 16
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_to_build_vector_index=0;

ALTER TABLE test_vector_inverted_two_stage_multi_parts ADD VECTOR INDEX vec_ind vector TYPE MSTG;

INSERT INTO test_vector_inverted_two_stage_multi_parts SELECT number, arrayWithConstant(16, number), number FROM numbers(1001);
INSERT INTO test_vector_inverted_two_stage_multi_parts SELECT number, arrayWithConstant(16, number), number-1001 FROM numbers(1001,1001);
SYSTEM WAIT BUILDING VECTOR INDICES test_vector_inverted_two_stage_multi_parts;

SELECT 'hybrid search multi parts with two stage enabled';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, arrayWithConstant(16, 1.0), '3') as score
FROM test_vector_inverted_two_stage_multi_parts ORDER BY score DESC LIMIT 5 settings two_stage_search_option=2;

OPTIMIZE TABLE test_vector_inverted_two_stage_multi_parts FINAL;

SELECT 'optimized to one part, hybrid search with two stage enabled';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, arrayWithConstant(16, 1.0), '3') as score
FROM test_vector_inverted_two_stage_multi_parts ORDER BY score DESC LIMIT 5 settings two_stage_search_option=2;

DROP TABLE IF EXISTS test_vector_inverted_two_stage_multi_parts;
