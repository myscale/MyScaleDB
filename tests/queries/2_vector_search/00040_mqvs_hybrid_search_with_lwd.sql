-- Tags: no-parallel

SET allow_experimental_inverted_index = true;

DROP TABLE IF EXISTS t_vector_invert_lwd;
CREATE TABLE t_vector_invert_lwd(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO t_vector_invert_lwd VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

SELECT 'text search before LWD';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert_lwd ORDER BY bm25 DESC LIMIT 1;

DELETE FROM t_vector_invert_lwd WHERE id=13;

SELECT 'text search after LWD';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert_lwd ORDER BY bm25 DESC LIMIT 1;

DROP TABLE t_vector_invert_lwd;

DROP TABLE IF EXISTS t_vector_invert_add_index;
CREATE TABLE t_vector_invert_add_index(
    id UInt64,
    vector Array(Float32),
    doc String,
    CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO t_vector_invert_add_index VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

ALTER TABLE t_vector_invert_add_index ADD INDEX inv_idx(doc) TYPE fts GRANULARITY 1;

SELECT 'text search on part w/o fts index';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert_add_index ORDER BY bm25 DESC LIMIT 2; -- {serverError QUERY_WAS_CANCELLED}

SELECT 'hybrid search rsf on part w/o fts index';
SET enable_brute_force_vector_search=1;
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert_add_index ORDER BY score DESC, id LIMIT 5; -- {serverError QUERY_WAS_CANCELLED}

ALTER TABLE t_vector_invert_add_index MATERIALIZE INDEX inv_idx;
DELETE FROM t_vector_invert_add_index WHERE id=13;

SELECT 'text search on part with index after LWD';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert_add_index ORDER BY bm25 DESC LIMIT 2;

SELECT 'hybrid search rsf on part with index after LWD';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert_add_index ORDER BY score DESC, id LIMIT 5;

DROP TABLE t_vector_invert_add_index;
