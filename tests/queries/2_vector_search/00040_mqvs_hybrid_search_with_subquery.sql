-- Tags: no-parallel

SET allow_experimental_inverted_index = true;
SET enable_brute_force_vector_search=1;

DROP TABLE IF EXISTS t_vector_invert_subquery;
CREATE TABLE t_vector_invert_subquery
(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree
ORDER BY id
settings index_granularity=2;

INSERT INTO t_vector_invert_subquery VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

ALTER TABLE t_vector_invert_subquery ADD VECTOR INDEX vec_ind vector TYPE FLAT;

SYSTEM WAIT BUILDING VECTOR INDICES t_vector_invert_subquery;

SELECT 'Scalar Subquery in text search function';
select id FROM (
select id, textsearch(doc, (
    select doc
    FROM t_vector_invert_subquery
    WHERE hasToken(doc, 'Ancient')
    LIMIT 1)
) as score
from t_vector_invert_subquery
order by score DESC
limit 5
)
limit 5;

SELECT 'Scalar Subquery in hybrid search function';
select id FROM (
select id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], (
    select doc
    FROM t_vector_invert_subquery
    WHERE hasToken(doc, 'Ancient')
    LIMIT 1)
) as score
from t_vector_invert_subquery
order by score DESC
limit 5
)
limit 5;

SELECT 'Scalar Subquery inside WITH clause in text search function';
WITH
(
    select doc
    FROM t_vector_invert_subquery
    WHERE hasToken(doc, 'Ancient')
    LIMIT 1
) AS target_keywords
select id FROM (
select id, textsearch(doc, target_keywords) as score
from t_vector_invert_subquery
order by score desc
limit 5
);

SELECT 'Scalar Subquery inside WITH clause in hybrid search function';
WITH
(
    select doc
    FROM t_vector_invert_subquery
    WHERE hasToken(doc, 'Ancient')
    LIMIT 1
) AS target_keywords
select id FROM (
select id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], target_keywords) as score
from t_vector_invert_subquery
order by score desc
limit 5
);

DROP TABLE IF EXISTS t_vector_invert_subquery;
