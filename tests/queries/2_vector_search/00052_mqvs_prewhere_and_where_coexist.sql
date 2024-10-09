-- Tags: no-parallel

DROP TABLE IF EXISTS test_where_and_prewhere;
CREATE TABLE test_where_and_prewhere(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    VECTOR INDEX vec_ind vector TYPE FLAT,
    CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO test_where_and_prewhere VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

SELECT 'Vector Search in WHERE';
SELECT id
FROM test_where_and_prewhere
PREWHERE id < 6
WHERE distance(vector, [5., 5, 5]) >= 0
LIMIT 5;

SELECT 'Multiple Vector Search in WHERE';
SELECT id
FROM test_where_and_prewhere
PREWHERE id < 6
WHERE distance(vector, [5., 5, 5]) >= 1 and distance(vector, [5., 5, 5]) < 20
LIMIT 5;

SELECT 'Vector Search in PREWHERE';
SELECT id
FROM test_where_and_prewhere
PREWHERE distance(vector, [5., 5, 5]) >= 0
WHERE id < 6
LIMIT 5;

SELECT 'Vector Search in WHERE and PREWHERE';
SELECT id
FROM test_where_and_prewhere
PREWHERE distance(vector, [1., 1, 1]) >= 0
WHERE distance(vector, [5., 5, 5]) >= 0
LIMIT 5;

SELECT 'Multiple Vector Search in WHERE and SELECT';
SELECT id, distance(vector, [1., 1, 1]) as dist1, distance(vector, [2., 2, 2]) as dist2
FROM test_where_and_prewhere
PREWHERE id < 6
WHERE distance(vector, [5., 5, 5]) >= 0
ORDER BY dist1 + dist2
LIMIT 5;

SELECT 'Text Search in WHERE';
SELECT id
FROM test_where_and_prewhere
PREWHERE id < 5
WHERE textsearch(doc, 'Ancient') >= 0
ORDER BY textsearch(doc, 'Ancient') DESC
LIMIT 5;


SELECT 'Hybrid Search in WHERE';
SELECT id
FROM test_where_and_prewhere
PREWHERE id < 5
WHERE hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') >= 0
ORDER BY hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') DESC
LIMIT 5;

DROP TABLE IF EXISTS test_where_and_prewhere;
