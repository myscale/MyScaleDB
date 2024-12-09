
SET allow_experimental_inverted_index = true;

DROP TABLE IF EXISTS t_vector_invert;
CREATE TABLE t_vector_invert(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO t_vector_invert VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

SELECT 'support only one hybrid/text/vector search function';
SELECT id, textsearch(doc, 'Ancient') as bm25, distance(vector, [1.0,1,1]) as dist FROM t_vector_invert ORDER BY bm25 DESC LIMIT 5; -- { serverError NOT_IMPLEMENTED }

set enable_brute_force_vector_search=true;
SELECT 'text search';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert ORDER BY bm25 DESC LIMIT 5;

SELECT 'text search with WHERE clause';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert WHERE id < 10 ORDER BY bm25 DESC LIMIT 5;

SELECT 'hybrid search with relative score fusion';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert ORDER BY score DESC, id LIMIT 5;

SELECT 'hybrid search with rank fusion';
SELECT id, hybridsearch('fusion_type=rrf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert ORDER BY score DESC, id LIMIT 5;

SELECT 'hybrid search rsf with WHERE clause';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert WHERE id < 10 ORDER BY score DESC, id LIMIT 5;

CREATE VECTOR INDEX i_vec ON t_vector_invert vector TYPE IVFFLAT;
SYSTEM WAIT BUILDING VECTOR INDICES t_vector_invert;
SELECT 'hybrid search with vector index';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert ORDER BY score DESC, id LIMIT 5;

SELECT 'hybrid search with vector scan parameters';
SELECT id, hybridsearch('dense_nprobe=128', 'fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert ORDER BY score DESC, id LIMIT 5;

SELECT id, hybridsearch('dense_alpha=3', 'fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert ORDER BY score DESC, id LIMIT 5; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_vector_invert;

CREATE TABLE t_vector_invert(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = MergeTree ORDER BY id settings index_granularity=2, enable_primary_key_cache=true;

INSERT INTO t_vector_invert VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.'),(10, [10,10,10],'Innovations in technology drive societal progress.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.');

SELECT 'text search with primary key cache';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert ORDER BY bm25 DESC LIMIT 5;

SELECT 'hybrid search with primary key cache';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'Ancient') as score FROM t_vector_invert ORDER BY score DESC, id DESC LIMIT 5;

DROP TABLE t_vector_invert;

DROP TABLE IF EXISTS t_vector_invert_array;
CREATE TABLE t_vector_invert_array(
    id UInt64,
    vector Array(Float32),
    docs Array(String),
    INDEX inv_idx(docs) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO t_vector_invert_array VALUES (0, [0,0,0], ['Ancient empires rise and fall, shaping history''s course.','Innovations in technology drive societal progress.', 'Military Strategy']),(1, [1,1,1], ['Artistic expressions reflect diverse cultural heritages.','Environmental conservation efforts protect Earth''s biodiversity.', 'Military Intelligence']),(2, [2,2,2], ['Social movements transform societies, forging new paths.','Diplomatic negotiations seek to resolve international conflicts.']),(3, [3,3,3], ['Economies fluctuate, reflecting the complex interplay of global forces.','Ancient philosophies provide wisdom for modern dilemmas.']),(4, [4,4,4], ['Strategic military campaigns alter the balance of power.','Economic theories debate the merits of market systems.']),(5, [5,5,5], ['Quantum leaps redefine understanding of physical laws.', 'Military strategies evolve with technological advancements.']),(6, [6,6,6], ['Chemical reactions unlock mysteries of nature.', 'Physics theories delve into the universe''s mysteries.']), (7, [7,7,7], ['Philosophical debates ponder the essence of existence.', 'Chemical compounds play crucial roles in medical breakthroughs.']),(8, [8,8,8], ['Marriages blend traditions, celebrating love''s union.', 'Philosophers debate ethics in the age of artificial intelligence.']),(9, [9,9,9], ['Explorers discover uncharted territories, expanding world maps.', 'Wedding ceremonies across cultures symbolize lifelong commitment.']);

SELECT 'text search on Array';
SELECT id, textsearch(docs, 'Military Strategy') as score FROM t_vector_invert_array ORDER BY score DESC LIMIT 5;

DROP TABLE IF EXISTS t_vector_invert_array;

DROP TABLE IF EXISTS t_vector_invert_map;
CREATE TABLE t_vector_invert_map(
    id UInt64,
    vector Array(Float32),
    doc_map Map(String,String),
    INDEX doc_map_idx(mapKeys(doc_map)) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO t_vector_invert_map VALUES (0, [0,0,0], {'Human History':'Exploring human history, culture from ancient to modern movements.'}),(1, [1,1,1], {'Social Sciences':'Studying societal structures, behaviors across psychology, economics.'}),(2, [2,2,2], {'Astronomy and Cosmos':'Delving into universe mysteries, black holes, nebulae, astrophysics.'}),(3, [3,3,3], {'Comic Art':'Vibrant world of comics, novels combining storytelling, visual artistry.'}),(4, [4,4,4], {'Campus Life':'Capturing essence of campus life, academic pursuits, student joys.'}),(5, [5,5,5], {'Cultural Studies':'Unraveling human culture, arts, thought across centuries, geographies.'}),(6, [6,6,6], {'Social Dynamics':'Analyzing dynamics of social interactions, institutions in evolving world.'}),(7, [7,7,7], {'Space Exploration':'Cosmic adventure to discover galaxy secrets, celestial phenomena.'}), (8, [8,8,8], {'Graphic Novels':'Comic universe journey, each page new adventure, fantasy to reality.'}),(9, [9,9,9], {'University Experiences':'Vibrancy of campus environments, melting pot of ideas, cultures.'}),(10, [10,10,10], {'Art and Philosophy':'Diving into realms of creativity, how art, literature shape world.'}),(11, [11,11,11], {'Human Behavior':'Study of social dynamics, cultural, economic, political impacts.'}),(12, [12,12,12], {'Cosmic Mysteries':'Charting course through universe wonders, seeking greatest mysteries.'}),(13, [13,13,13], {'Comics and Narratives':'Diverse world of comics, each panel unique narrative experiences.'}),(14, [14,14,14], {'Student Life':'Unique journey of campus life, academic rigor, extracurricular excitement.'}),(15, [15,15,15], {'Historical Insights':'Broad spectrum of human achievements from philosophers to artists.'}),(16, [16,16,16], {'Societal Structures':'Exploring web of societies, understanding cultural, economic impacts.'}),(17, [17,17,17], {'Universe Exploration':'Journeying across cosmos, exploring celestial bodies, universe physics.'}),(18, [18,18,18], {'Art of Storytelling':'Creative world of comics blending art, storytelling imaginatively.'}),(19, [19,19,19], {'Educational Innovations':'Dynamic, diverse campus life where education meets innovation.'});

SELECT 'text search on Map';
SELECT id, textsearch(mapKeys(doc_map), 'Comics') as score FROM t_vector_invert_map ORDER BY score DESC LIMIT 5;

DROP TABLE IF EXISTS t_vector_invert_map;

DROP TABLE IF EXISTS t_vector_invert_multi;
CREATE TABLE t_vector_invert_multi
(
    id UInt64,
    vector Array(Float32),
    doc String,
    doc2 String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    INDEX inv_idx2(doc2) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO t_vector_invert_multi VALUES (0, [0,0,0], 'Ancient empires rise and fall, shaping history''s course.', 'Exploring human history, culture from ancient to modern movements.'),(1,[1,1,1], 'Artistic expressions reflect diverse cultural heritages.', 'Studying societal structures, behaviors across psychology, economics.'),(2,[2,2,2], 'Social movements transform societies, forging new paths.', 'Delving into universe mysteries, black holes, nebulae, astrophysics.'),(3,[3,3,3], 'Economies fluctuate, reflecting the complex interplay of global forces.', 'Vibrant world of comics, novels combining storytelling, visual artistry.'),(4,[4,4,4], 'Strategic military campaigns alter the balance of power.','Capturing essence of campus life, academic pursuits, student joys.'),(5, [5,5,5], 'Quantum leaps redefine understanding of physical laws.', 'Unraveling human culture, arts, thought across centuries, geographies.'),(6, [6,6,6],'Chemical reactions unlock mysteries of nature.', 'Analyzing dynamics of social interactions, institutions in evolving world.'), (7,[7,7,7], 'Philosophical debates ponder the essence of existence.', 'Cosmic adventure to discover galaxy secrets, celestial phenomena.'),(8,[8,8,8], 'Marriages blend traditions, celebrating love''s union.', 'Comic universe journey, each page new adventure, fantasy to reality.'),(9,[9,9,9], 'Explorers discover uncharted territories, expanding world maps.', 'Vibrancy of campus environments, melting pot of ideas, cultures.'),(10, [10,10,10],'Innovations in technology drive societal progress.', 'Diving into realms of creativity, how art, literature shape world.'),(11,[11,11,11], 'Environmental conservation efforts protect Earth''s biodiversity.', 'Study of social dynamics, cultural, economic, political impacts.'),(12,[12,12,12], 'Diplomatic negotiations seek to resolve international conflicts.', 'Charting course through universe wonders, seeking greatest mysteries.'),(13,[13,13,13], 'Ancient philosophies provide wisdom for modern dilemmas.', 'Diverse world of comics, each panel unique narrative experiences.'),(14,[14,14,14], 'Economic theories debate the merits of market systems.', 'Unique journey of campus life, academic rigor, extracurricular excitement.'),(15,[15,15,15], 'Military strategies evolve with technological advancements.', 'Broad spectrum of human achievements from philosophers to artists.'),(16,[16,16,16], 'Physics theories delve into the universe''s mysteries.', 'Exploring web of societies, understanding cultural, economic impacts.'),(17,[17,17,17], 'Chemical compounds play crucial roles in medical breakthroughs.', 'Journeying across cosmos, exploring celestial bodies, universe physics.'),(18,[18,18,18], 'Philosophers debate ethics in the age of artificial intelligence.', 'Creative world of comics blending art, storytelling imaginatively.'),(19,[19,19,19], 'Wedding ceremonies across cultures symbolize lifelong commitment.', 'Dynamic, diverse campus life where education meets innovation.');

SELECT 'text search on doc';
SELECT id, textsearch(doc, 'Ancient') as bm25 FROM t_vector_invert_multi ORDER BY bm25 DESC LIMIT 5;

SELECT 'hybridsearch on doc2';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc2, [1.0,1,1], 'cultural') as score FROM t_vector_invert_multi ORDER BY score DESC, id LIMIT 5;

DROP TABLE t_vector_invert_multi;

DROP TABLE IF EXISTS t_vector_invert_binary;
CREATE TABLE t_vector_invert_binary
(
    id UInt64,
    vector FixedString(3),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO t_vector_invert_binary VALUES (0, char(0,0,0), 'Ancient empires rise and fall, shaping history''s course.'),(1, char(1,1,1), 'Artistic expressions reflect diverse cultural heritages.'),(2, char(2,2,2), 'Social movements transform societies, forging new paths.'),(3, char(3,3,3), 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4, char(4,4,4), 'Strategic military campaigns alter the balance of power.'),(5, char(5,5,5), 'Quantum leaps redefine understanding of physical laws.'),(6, char(6,6,6),'Chemical reactions unlock mysteries of nature.'), (7, char(7,7,7), 'Philosophical debates ponder the essence of existence.'),(8, char(8,8,8), 'Marriages blend traditions, celebrating love''s union.'),(9, char(9,9,9), 'Explorers discover uncharted territories, expanding world maps.'),(10, char(10,10,10),'Innovations in technology drive societal progress.'),(11, char(11,11,11), 'Environmental conservation efforts protect Earth''s biodiversity.'),(12, char(12,12,12), 'Diplomatic negotiations seek to resolve international conflicts.'),(13, char(13,13,13), 'Ancient philosophies provide wisdom for modern dilemmas.'),(14, char(14,14,14), 'Economic theories debate the merits of market systems.'),(15, char(15,15,15), 'Military strategies evolve with technological advancements.'),(16, char(16,16,16), 'Physics theories delve into the universe''s mysteries.'),(17, char(17,17,17), 'Chemical compounds play crucial roles in medical breakthroughs.'),(18, char(18,18,18), 'Philosophers debate ethics in the age of artificial intelligence.'),(19, char(19,19,19), 'Wedding ceremonies across cultures symbolize lifelong commitment.');

SET enable_brute_force_vector_search=true;
SELECT 'hybrid search with relative score fusion on binary vector';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, char(1,1,1), 'Ancient') as score FROM t_vector_invert_binary ORDER BY score DESC, id LIMIT 5;

SELECT 'hybrid search with rank fusion on binary vector';
SELECT id, hybridsearch('fusion_type=rrf')(vector, doc, char(1,1,1), 'Ancient') as score FROM t_vector_invert_binary ORDER BY score DESC, id LIMIT 5;

DROP TABLE t_vector_invert_binary;

set allow_experimental_json_type = 1;
DROP TABLE IF EXISTS t_vector_invert_json;
CREATE TABLE t_vector_invert_json
(
    id UInt64,
    vector Array(Float32),
    doc JSON,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree ORDER BY id settings index_granularity=2; -- { serverError INCORRECT_QUERY }
