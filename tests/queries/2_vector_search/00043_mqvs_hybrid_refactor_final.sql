-- Tags: no-parallel

set enable_brute_force_vector_search = 1;

DROP TABLE IF EXISTS test_vector_final;
CREATE TABLE test_vector_final(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine ReplacingMergeTree primary key id;
INSERT INTO test_vector_final SELECT number, arrayWithConstant(3, number) FROM numbers(20);

-- upsert data
INSERT INTO test_vector_final SELECT number, arrayWithConstant(3, number) FROM numbers(20);

SELECT 'Vector scan + final';
SELECT id, vector, distance(vector, arrayWithConstant(3, 5.0)) as dist FROM test_vector_final ORDER BY dist, id limit 5 SETTINGS final=1;

SELECT 'Vector scan + where + final';
SELECT id, vector, distance(vector, arrayWithConstant(3, 5.0)) as dist FROM test_vector_final WHERE id > 4 ORDER BY dist, id limit 5 SETTINGS final=1, optimize_move_to_prewhere_if_final=1;

DROP TABLE IF EXISTS test_inverted_final SYNC;
CREATE TABLE test_inverted_final
(
    id    UInt32,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = ReplacingMergeTree
ORDER BY id
settings index_granularity=2;

INSERT INTO test_inverted_final VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

-- upsert data
INSERT INTO test_inverted_final VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

SELECT 'Text search + final';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM test_inverted_final FINAL ORDER BY bm25 DESC LIMIT 5;

SELECT 'Hybrid + final';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM test_inverted_final FINAL ORDER BY score DESC, id LIMIT 5;

DROP TABLE IF EXISTS test_vector_inverted_two_stage_final SYNC;
CREATE TABLE test_vector_inverted_two_stage_final
(
    id    UInt32,
    vector  Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT check_length CHECK length(vector) = 16
)
engine = ReplacingMergeTree
ORDER BY id
SETTINGS min_bytes_to_build_vector_index=0;

ALTER TABLE test_vector_inverted_two_stage_final ADD VECTOR INDEX vec_ind vector TYPE MSTG;

INSERT INTO test_vector_inverted_two_stage_final SELECT number, arrayWithConstant(16, number), number FROM numbers(1001);

--upsert data
INSERT INTO test_vector_inverted_two_stage_final SELECT number, arrayWithConstant(16, number), number FROM numbers(1001);
SYSTEM WAIT BUILDING VECTOR INDICES test_vector_inverted_two_stage_final;

SELECT 'Two-stage vector scan + final';
SELECT id, distance(vector, arrayWithConstant(16, 1.0)) AS dist FROM test_vector_inverted_two_stage_final FINAL WHERE id < 11 ORDER BY dist, id LIMIT 5 SETTINGS two_stage_search_option=2;

SELECT 'Two-stage hybrid + final';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, arrayWithConstant(16, 1.0), '2') as score
FROM test_vector_inverted_two_stage_final FINAL
ORDER BY score DESC LIMIT 5 SETTINGS two_stage_search_option=2;

DROP TABLE test_vector_final;
DROP TABLE test_inverted_final;
DROP TABLE test_vector_inverted_two_stage_final;
