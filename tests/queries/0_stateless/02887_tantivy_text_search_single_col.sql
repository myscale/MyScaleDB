-- Tags: no-tsan, no-msan

SET allow_experimental_inverted_index = 1;
SET log_queries = 1;
SET mutations_sync = 1;
SET enable_fts_index_for_string_functions = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts, index_granularity=2, index_granularity_bytes=10Mi, Compact Part <<<<<<<======';
DROP TABLE IF EXISTS simple_table sync;
CREATE TABLE simple_table(id UInt64, doc String, INDEX text_idx(doc) TYPE fts GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO simple_table VALUES (0, 'Ancient empires rise and fall, shaping history''s course.'),(1, 'Artistic expressions reflect diverse cultural heritages.'),(2, 'Social movements transform societies, forging new paths.'),(3, 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4, 'Strategic military campaigns alter the balance of power.'),(5, 'Quantum leaps redefine understanding of physical laws.'),(6, 'Chemical reactions unlock mysteries of nature.'), (7, 'Philosophical debates ponder the essence of existence.'),(8, 'Marriages blend traditions, celebrating love''s union.'),(9, 'Explorers discover uncharted territories, expanding world maps.'),(10, 'Innovations in technology drive societal progress.'),(11, 'Environmental conservation efforts protect Earth''s biodiversity.'),(12, 'Diplomatic negotiations seek to resolve international conflicts.'),(13, 'Ancient philosophies provide wisdom for modern dilemmas.'),(14, 'Economic theories debate the merits of market systems.'),(15, 'Military strategies evolve with technological advancements.'),(16, 'Physics theories delve into the universe''s mysteries.'),(17, 'Chemical compounds play crucial roles in medical breakthroughs.'),(18, 'Philosophers debate ethics in the age of artificial intelligence.'),(19, 'Wedding ceremonies across cultures symbolize lifelong commitment.');
-- check fts index was created
SELECT name, type FROM system.data_skipping_indices WHERE table =='simple_table' AND database = currentDatabase() LIMIT 1;
-- throw in a random consistency check
CHECK TABLE simple_table;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 1]: search fts index with ==
SELECT '[Test Case 1]: search fts index with ==';
SELECT count(*) FROM simple_table WHERE doc == 'Social movements transform societies, forging new paths.';
-- [Test Case 1] check the query only read 1 granules (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM simple_table WHERE doc == ''So')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 2]: search fts index with LIKE
SELECT '[Test Case 2]: search fts index with LIKE';
SELECT count(*) FROM simple_table WHERE doc LIKE '%seek%';
-- [Test Case 2]: check the query read smaller than 10 granules (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT count(*) FROM simple_table WHERE doc LIKE \'%seek%\';')
        AND type='QueryFinish'
        AND result_rows==1
    ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 3]: search fts index with hasToken
SELECT '[Test Case 3]: search fts index with hasToken';
SELECT count(*) FROM simple_table WHERE hasToken(doc, 'Ancient');
-- [Test Case 3]: check the query only read 2 granules (40 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT count(*) FROM simple_table WHERE hasToken(doc, \'Ancient\');')
        AND type='QueryFinish' 
        AND result_rows==1
    ORDER BY query_start_time DESC
    LIMIT 1;




DROP TABLE IF EXISTS simple_table sync;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts(), index_granularity=2, index_granularity_bytes=10Mi, Compact Part <<<<<<<======';
DROP TABLE IF EXISTS simple_table_x sync;
CREATE TABLE simple_table_x(id UInt64, doc String, INDEX text_idx(doc) TYPE fts() GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO simple_table_x VALUES (0, 'Ancient empires rise and fall, shaping history''s course.'),(1, 'Artistic expressions reflect diverse cultural heritages.'),(2, 'Social movements transform societies, forging new paths.'),(3, 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4, 'Strategic military campaigns alter the balance of power.'),(5, 'Quantum leaps redefine understanding of physical laws.'),(6, 'Chemical reactions unlock mysteries of nature.'), (7, 'Philosophical debates ponder the essence of existence.'),(8, 'Marriages blend traditions, celebrating love''s union.'),(9, 'Explorers discover uncharted territories, expanding world maps.'),(10, 'Innovations in technology drive societal progress.'),(11, 'Environmental conservation efforts protect Earth''s biodiversity.'),(12, 'Diplomatic negotiations seek to resolve international conflicts.'),(13, 'Ancient philosophies provide wisdom for modern dilemmas.'),(14, 'Economic theories debate the merits of market systems.'),(15, 'Military strategies evolve with technological advancements.'),(16, 'Physics theories delve into the universe''s mysteries.'),(17, 'Chemical compounds play crucial roles in medical breakthroughs.'),(18, 'Philosophers debate ethics in the age of artificial intelligence.'),(19, 'Wedding ceremonies across cultures symbolize lifelong commitment.'),(20, 'Military Strategy'),(21, 'Military Intelligence');
-- check fts() index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'simple_table_x' AND database = currentDatabase() LIMIT 1;
CHECK TABLE simple_table_x;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 4]: search fts() index with hasToken
SELECT '[Test Case 4]: search fts() index with hasToken';
SELECT count(*) FROM simple_table_x WHERE hasToken(doc, 'Ancient');
-- [Test Case 4]: check the query only read 2 granules (22 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT count(*) FROM simple_table_x WHERE hasToken(doc, \'Ancient\');')
        AND type='QueryFinish' 
        AND result_rows==1
    ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 5]: search fts() index with IN operator
SELECT '[Test Case 5]: search fts() index with IN operator';
SELECT count(*) FROM simple_table_x WHERE doc IN ('Military Strategy', 'Military Intelligence');
-- [Test Case 5]: check the query read smaller than 11 granules (22 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows<22 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM simple_table_x WHERE doc IN (''Milita')
        AND type='QueryFinish' 
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 6]: search fts() index with multiSearch
SELECT '[Test Case 6]: search fts() index with multiSearch';
SELECT count(*) FROM simple_table_x WHERE multiSearchAny(doc, ['Ancient', 'Military']);
-- [Test Case 6]: check the query read smaller than 11 granules (22 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows<22 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM simple_table_x WHERE multiSearchAny(doc, [''Anc') 
        AND type='QueryFinish' 
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;




-- DROP TABLE IF EXISTS simple_table_x sync;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts() on array column, index_granularity=2, index_granularity_bytes=10Mi, Compact Part <<<<<<<======';
DROP TABLE IF EXISTS simple_table_array sync;
CREATE TABLE simple_table_array (id UInt64, docs Array(String), INDEX text_idx(docs) TYPE fts()) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO simple_table_array SELECT rowNumberInBlock(), groupArray(doc) FROM simple_table_x GROUP BY id%10;
-- check fts() index on array was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'simple_table_array' AND database = currentDatabase() LIMIT 1;
CHECK TABLE simple_table_array;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 7]: search fts() index on array with has
SELECT '[Test Case 7]: search fts() index with has';
SELECT count(*) FROM simple_table_array WHERE has(docs, 'Military Strategy');
-- [Test Case 7]: check the query must read smaller than 5 granules (10 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows<10 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM simple_table_array WHERE has(docs, ''Military') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;




DROP TABLE IF EXISTS simple_table_array sync;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts on map column, index_granularity=2, index_granularity_bytes=10Mi, Compact Part <<<<<<<======';
DROP TABLE IF EXISTS simple_table_map sync;
CREATE TABLE simple_table_map (id UInt64, doc_map Map(String,String), INDEX doc_map_idx(mapKeys(doc_map)) TYPE fts) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO simple_table_map VALUES (0, {'Human History':'Exploring human history, culture from ancient to modern movements.'}),(1, {'Social Sciences':'Studying societal structures, behaviors across psychology, economics.'}),(2, {'Astronomy and Cosmos':'Delving into universe mysteries, black holes, nebulae, astrophysics.'}),(3, {'Comic Art':'Vibrant world of comics, novels combining storytelling, visual artistry.'}),(4, {'Campus Life':'Capturing essence of campus life, academic pursuits, student joys.'}),(5, {'Cultural Studies':'Unraveling human culture, arts, thought across centuries, geographies.'}),(6, {'Social Dynamics':'Analyzing dynamics of social interactions, institutions in evolving world.'}),(7, {'Space Exploration':'Cosmic adventure to discover galaxy secrets, celestial phenomena.'}), (8, {'Graphic Novels':'Comic universe journey, each page new adventure, fantasy to reality.'}),(9, {'University Experiences':'Vibrancy of campus environments, melting pot of ideas, cultures.'}),(10, {'Art and Philosophy':'Diving into realms of creativity, how art, literature shape world.'}),(11, {'Human Behavior':'Study of social dynamics, cultural, economic, political impacts.'}),(12, {'Cosmic Mysteries':'Charting course through universe wonders, seeking greatest mysteries.'}),(13, {'Comics and Narratives':'Diverse world of comics, each panel unique narrative experiences.'}),(14, {'Student Life':'Unique journey of campus life, academic rigor, extracurricular excitement.'}),(15, {'Historical Insights':'Broad spectrum of human achievements from philosophers to artists.'}),(16, {'Societal Structures':'Exploring web of societies, understanding cultural, economic impacts.'}),(17, {'Universe Exploration':'Journeying across cosmos, exploring celestial bodies, universe physics.'}),(18, {'Art of Storytelling':'Creative world of comics blending art, storytelling imaginatively.'}),(19, {'Educational Innovations':'Dynamic, diverse campus life where education meets innovation.'});
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'simple_table_map' AND database = currentDatabase() LIMIT 1;
CHECK TABLE simple_table_map;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 8]: search fts index on map with mapContains
SELECT '[Test Case 8]: search fts index on map with mapContains';
SELECT count(*) FROM simple_table_map WHERE mapContains(doc_map, 'Comics and Narratives') SETTINGS allow_experimental_analyzer=0;
-- [Test Case 8]: check the query must read smaller than 5 granules (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows<=8 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM simple_table_map WHERE mapContains(doc_map, ''Co') 
        AND type='QueryFinish' 
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

SELECT '[Test Case 9]: search fts index on map with map key';
-- [Test Case 9]: search fts index on map with map key; index has effects on map keys, not map values. 
SYSTEM FLUSH LOGS;
SELECT count(*) FROM simple_table_map WHERE doc_map['Comic Art'] like '%world%';
-- [Test Case 9]: check the query must read all 4 granules (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;

SELECT read_rows <= 8 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM simple_table_map WHERE doc_map[''Co') 
        AND type='QueryFinish' 
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;



DROP TABLE IF EXISTS simple_table_map sync;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts, index_granularity=2, index_granularity_bytes=10Mi, Compact Part, 4 DataParts <<<<<<<======';
DROP TABLE IF EXISTS simple_table_4_parts sync;
CREATE TABLE simple_table_4_parts (id UInt64, doc String, INDEX text_idx(doc) TYPE fts GRANULARITY 1) ENGINE = MergeTree() PARTITION BY id % 4 ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO simple_table_4_parts VALUES (0, 'Ancient empires rise and fall, shaping history''s course.'),(1, 'Artistic expressions reflect diverse cultural heritages.'),(2, 'Social movements transform societies, forging new paths.'),(3, 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4, 'Strategic military campaigns alter the balance of power.'),(5, 'Quantum leaps redefine understanding of physical laws.'),(6, 'Chemical reactions unlock mysteries of nature.'), (7, 'Philosophical debates ponder the essence of existence.'),(8, 'Marriages blend traditions, celebrating love''s union.'),(9, 'Explorers discover uncharted territories, expanding world maps.'),(10, 'Innovations in technology drive societal progress.'),(11, 'Environmental conservation efforts protect Earth''s biodiversity.'),(12, 'Diplomatic negotiations seek to resolve international conflicts.'),(13, 'Ancient philosophies provide wisdom for modern dilemmas.'),(14, 'Economic theories debate the merits of market systems.'),(15, 'Military strategies evolve with technological advancements.'),(16, 'Physics theories delve into the universe''s mysteries.'),(17, 'Chemical compounds play crucial roles in medical breakthroughs.'),(18, 'Philosophers debate ethics in the age of artificial intelligence.'),(19, 'Wedding ceremonies across cultures symbolize lifelong commitment.');
-- check fts index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'simple_table_4_parts' AND database = currentDatabase() LIMIT 1;
-- check data parts count (wanted 4)
SELECT count(*) FROM system.parts WHERE (table = 'simple_table_4_parts') AND (active = 1) AND database=currentDatabase();
-- check table
CHECK TABLE simple_table_4_parts;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 10]: search fts index with hasToken on 4 data parts
SELECT '[Test Case 10]: search fts index with hasToken on 4 data parts';
SELECT count(*) FROM simple_table_4_parts WHERE hasToken(doc, 'Ancient');
-- [Test Case 10]: check the query only read 2 granules (20 rows total; each granule has 2 rows)
-- [Test Case 10]: each part has 5 rows / 3 granules, [0,1];[2,3];[4,4], table has 12 granules.
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT count(*) FROM simple_table_4_parts WHERE hasToken(doc, \'Ancient\');')
        AND type='QueryFinish' 
        AND result_rows==1
    ORDER BY query_start_time DESC
    LIMIT 1;




DROP TABLE IF EXISTS simple_table_4_parts sync;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts(''{"doc":{"tokenizer":{"type":"chinese"}}}''), index_granularity=2, index_granularity_bytes=10Mi, Compact Part, language Chinese <<<<<<<======';
DROP TABLE IF EXISTS simple_table_chinese sync;
CREATE TABLE simple_table_chinese(id UInt64, doc String, INDEX text_idx(doc) TYPE fts('{"doc":{"tokenizer":{"type":"chinese"}}}')) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO simple_table_chinese VALUES (0, '古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭刻了时代的变迁与文明的发展。'),(1, '艺术的多样表达方式反映了不同文化的丰富遗产，展现了人类创造力的无限可能。'),(2, '社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。'),(3, '全球经济的波动复杂多变，如同镜子反映出世界各国之间错综复杂的力量关系。'),(4, '战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。'),(5, '量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。'),(6, '化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。'), (7, '哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。'),(8, '婚姻的融合不仅是情感的结合，更是不同传统和文化的交汇，彰显了爱的力量。'),(9, '探险，勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。'),(10, '科技创新的步伐从未停歇，它推动着社会的进步，引领着时代的前行。'),(11, '环保行动积极努力保护地球的生物多样性，为我们共同的家园筑起绿色的屏障。'),(12, '外交谈判在国际舞台上寻求和平解决冲突，致力于构建一个更加和谐的世界。'),(13, '古代哲学的智慧至今仍对现代社会的诸多难题提供启示和解答，影响深远。'),(14, '经济学理论围绕市场体系的优劣进行了深入的探讨与辩论，对经济发展有重要指导意义。'),(15, '随着科技的不断进步，军事战略也在不断演变，应对新时代的挑战和需求。'),(16, '现代物理学理论深入挖掘宇宙的奥秘，试图解开那些探索宇宙时的未知之谜。'),(17, '在医学领域，化学化合物的作用至关重要，它们在许多重大医疗突破中扮演了核心角色。'),(18, '当代哲学家在探讨人工智能时代的伦理道德问题，对机器与人类的关系进行深刻反思。'),(19, '不同文化背景下的婚礼仪式代表着一生的承诺与责任，象征着两颗心的永恒结合。');
-- check fts index with chinese tokenizer was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'simple_table_chinese' AND database = currentDatabase() LIMIT 1;
CHECK TABLE simple_table_chinese;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 11]: search fts index with chinese tokenizer
SELECT '[Test Case 11]: search fts(''{"doc":{"tokenizer":{"type":"chinese"}}}'') index with chinese tokenizer';
SELECT count(*) FROM simple_table_chinese WHERE hasToken(doc, '世界');
-- [Test Case 11]: check the query only read 4 granule (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_chinese WHERE hasToken(doc, ''世界'');') 
        AND type='QueryFinish'
        ORDER BY query_start_time DESC
    LIMIT 1;




DROP TABLE IF EXISTS simple_table_chinese sync;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts, index_granularity=2, index_granularity_bytes=10Mi, Wide Part, LWD->ALTER_DELETE->OPTIMIZE->INSERT <<<<<<<======';
DROP TABLE IF EXISTS simple_table_curd sync;
CREATE TABLE simple_table_curd(id UInt64, doc String, INDEX text_idx(doc) TYPE fts GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', min_bytes_for_wide_part=0, min_rows_for_wide_part=0;;
INSERT INTO simple_table_curd VALUES (0, 'Ancient empires rise and fall, shaping history''s course.'),(1, 'Artistic expressions reflect diverse cultural heritages.'),(2, 'Social movements transform societies, forging new paths.'),(3, 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4, 'Strategic military campaigns alter the balance of power.'),(5, 'Quantum leaps redefine understanding of physical laws.'),(6, 'Chemical reactions unlock mysteries of nature.'), (7, 'Philosophical debates ponder the essence of existence.'),(8, 'Marriages blend traditions, celebrating love''s union.'),(9, 'Explorers discover uncharted territories, expanding world maps.'),(10, 'Innovations in technology drive societal progress.'),(11, 'Environmental conservation efforts protect Earth''s biodiversity.'),(12, 'Diplomatic negotiations seek to resolve international conflicts.'),(13, 'Ancient philosophies provide wisdom for modern dilemmas.'),(14, 'Economic theories debate the merits of market systems.'),(15, 'Military strategies evolve with technological advancements.'),(16, 'Physics theories delve into the universe''s mysteries.'),(17, 'Chemical compounds play crucial roles in medical breakthroughs.'),(18, 'Philosophers debate ethics in the age of artificial intelligence.'),(19, 'Wedding ceremonies across cultures symbolize lifelong commitment.');
-- check fts index was created
SELECT name, type FROM system.data_skipping_indices WHERE table =='simple_table_curd' AND database = currentDatabase() LIMIT 1;
-- throw in a random consistency check
CHECK TABLE simple_table_curd;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 12]: search fts index before LWD operation
SELECT '[Test Case 12]: search fts index before LWD operation';
SELECT count(*) FROM simple_table_curd WHERE hasToken(doc, 'Ancient');
-- [Test Case 12]: check the query only read 2 granule (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd WHERE hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 13]: search fts index after LWD operation.
SELECT '[Test Case 13]: search fts index after LWD operation';
DELETE FROM simple_table_curd WHERE id=0;
SELECT count(*) FROM simple_table_curd WHERE hasToken(doc, 'Ancient');
-- [Test Case 13]: In Wide Part, LWD does not actually delete data within the index, so the index will still hit 2 granules.
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd WHERE hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 14]: 
SELECT '[Test Case 14]: search fts index after OPTIMIZE FINAL';
OPTIMIZE TABLE simple_table_curd FINAL;
SYSTEM FLUSH LOGS;
SELECT count(*) FROM simple_table_curd WHERE id!=-2 and hasToken(doc, 'Ancient');
-- [Test Case 14]: OPTIMIZE FINAL after LWD, it will make `_row_exists` apply to table, so the index will be rebuild.
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd WHERE id!=-2 and hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 15]: use ALTER DELETE to delete one row in current table.
SELECT '[Test Case 15]: search fts index after ALTER DELETE';
ALTER TABLE simple_table_curd DELETE WHERE id=13;
SELECT count(*) FROM simple_table_curd WHERE id>=0 and hasToken(doc, 'Ancient');

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 16]: use INSERT to add 2 rows in current Wide table.
SELECT '[Test Case 16]: search fts index after adding one row in current Wide table.';
INSERT INTO simple_table_curd VALUES (13, 'Ancient philosophies provide wisdom for modern dilemmas.'), (20, 'This sentence just wanna fill one granule');
SYSTEM FLUSH LOGS;
SELECT count(*) FROM simple_table_curd WHERE id<100 and hasToken(doc, 'Ancient');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd WHERE id<100 and hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;



DROP TABLE IF EXISTS simple_table_curd sync;

---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts, index_granularity=2, index_granularity_bytes=10Mi, Compact Part, LWD->ALTER_DELETE->OPTIMIZE->INSERT <<<<<<<======';
DROP TABLE IF EXISTS simple_table_curd1 sync;
CREATE TABLE simple_table_curd1(id UInt64, doc String, INDEX text_idx(doc) TYPE fts GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', min_bytes_for_wide_part=10485760, min_rows_for_wide_part=1000;
INSERT INTO simple_table_curd1 VALUES (0, 'Ancient empires rise and fall, shaping history''s course.'),(1, 'Artistic expressions reflect diverse cultural heritages.'),(2, 'Social movements transform societies, forging new paths.'),(3, 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4, 'Strategic military campaigns alter the balance of power.'),(5, 'Quantum leaps redefine understanding of physical laws.'),(6, 'Chemical reactions unlock mysteries of nature.'), (7, 'Philosophical debates ponder the essence of existence.'),(8, 'Marriages blend traditions, celebrating love''s union.'),(9, 'Explorers discover uncharted territories, expanding world maps.'),(10, 'Innovations in technology drive societal progress.'),(11, 'Environmental conservation efforts protect Earth''s biodiversity.'),(12, 'Diplomatic negotiations seek to resolve international conflicts.'),(13, 'Ancient philosophies provide wisdom for modern dilemmas.'),(14, 'Economic theories debate the merits of market systems.'),(15, 'Military strategies evolve with technological advancements.'),(16, 'Physics theories delve into the universe''s mysteries.'),(17, 'Chemical compounds play crucial roles in medical breakthroughs.'),(18, 'Philosophers debate ethics in the age of artificial intelligence.'),(19, 'Wedding ceremonies across cultures symbolize lifelong commitment.');
-- check fts index was created
SELECT name, type FROM system.data_skipping_indices WHERE table =='simple_table_curd1' AND database = currentDatabase() LIMIT 1;
-- throw in a random consistency check
CHECK TABLE simple_table_curd1;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- [Test Case 17]: search fts index on compact Part before LWD operation
SELECT '[Test Case 17]: search fts index on compact Part before LWD operation';
SELECT count(*) FROM simple_table_curd1 WHERE hasToken(doc, 'Ancient');
-- [Test Case 17]: check the query only read 2 granule (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd1 WHERE hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 18]: search fts index on compact Part after LWD operation.
SELECT '[Test Case 18]: search fts index on compact Part after LWD operation';
DELETE FROM simple_table_curd1 WHERE id=0;
SELECT count(*) FROM simple_table_curd1 WHERE hasToken(doc, 'Ancient');
-- [Test Case 18]: In Wide Part, LWD does not actually delete data within the index, so the index will still hit 2 granules.
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd1 WHERE hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 19]: 
SELECT '[Test Case 19]: search fts index on compact Part after OPTIMIZE FINAL';
OPTIMIZE TABLE simple_table_curd1 FINAL;
SYSTEM FLUSH LOGS;
SELECT count(*) FROM simple_table_curd1 WHERE id>=0 and hasToken(doc, 'Ancient');
-- [Test Case 19]: OPTIMIZE FINAL after LWD, it will make `_row_exists` apply to table, so the index will be rebuild.
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd1 WHERE id>=0 and hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 20]: use ALTER DELETE to delete one row in current table.
SELECT '[Test Case 20]: search fts index after ALTER DELETE';
ALTER TABLE simple_table_curd1 DELETE WHERE id=13;
SELECT count(*) FROM simple_table_curd1 WHERE id!=-2 and hasToken(doc, 'Ancient');
-- [Test Case 20]: Mutations triggered by the `ALTER DELETE` statement will rebuild the index.

--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-
--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-

-- [Test Case 21]: use INSERT to add 2 rows in current Wide table.
SELECT '[Test Case 21]: search fts index after adding one row in current Wide table.';
INSERT INTO simple_table_curd1 VALUES (13, 'Ancient philosophies provide wisdom for modern dilemmas.'), (20, 'This sentence just wanna fill one granule');
SYSTEM FLUSH LOGS;
SELECT count(*) FROM simple_table_curd1 WHERE id!=-3 and hasToken(doc, 'Ancient');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select' 
        AND current_database = currentDatabase()    
        AND endsWith(trimRight(query), 'simple_table_curd1 WHERE id!=-3 and hasToken(doc, ''Ancient'');') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;



DROP TABLE IF EXISTS simple_table_curd1 sync;





---------------------------------------------------------------------------------------------------------------------------------------------------------
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
SELECT '';
SELECT '======>>>>>>> Test fts with doc store, index_granularity=2, index_granularity_bytes=10Mi, Compact Part <<<<<<<======';
DROP TABLE IF EXISTS simple_table sync;
CREATE TABLE simple_table(id UInt64, doc String, INDEX text_idx(doc) TYPE fts('{}') GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO simple_table VALUES (0, 'Ancient empires rise and fall, shaping history''s course.'),(1, 'Artistic expressions reflect diverse cultural heritages.'),(2, 'Social movements transform societies, forging new paths.'),(3, 'Economies fluctuate, reflecting the complex interplay of global forces.'),(4, 'Strategic military campaigns alter the balance of power.'),(5, 'Quantum leaps redefine understanding of physical laws.'),(6, 'Chemical reactions unlock mysteries of nature.'), (7, 'Philosophical debates ponder the essence of existence.'),(8, 'Marriages blend traditions, celebrating love''s union.'),(9, 'Explorers discover uncharted territories, expanding world maps.'),(10, 'Innovations in technology drive societal progress.'),(11, 'Environmental conservation efforts protect Earth''s biodiversity.'),(12, 'Diplomatic negotiations seek to resolve international conflicts.'),(13, 'Ancient philosophies provide wisdom for modern dilemmas.'),(14, 'Economic theories debate the merits of market systems.'),(15, 'Military strategies evolve with technological advancements.'),(16, 'Physics theories delve into the universe''s mysteries.'),(17, 'Chemical compounds play crucial roles in medical breakthroughs.'),(18, 'Philosophers debate ethics in the age of artificial intelligence.'),(19, 'Wedding ceremonies across cultures symbolize lifelong commitment.');
-- check fts index was created
SELECT name, type FROM system.data_skipping_indices WHERE table =='simple_table' AND database = currentDatabase() LIMIT 1;
-- throw in a random consistency check
CHECK TABLE simple_table;
--/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-
---------------------------------------------------------------------------------------------------------------------------------------------------------


-- [Test Case 2]: search fts index with LIKE
SELECT '[Test Case 22]: search fts index with LIKE';
SELECT count(*) FROM simple_table WHERE doc LIKE '%seek%';
-- [Test Case 2]: check the query read smaller than 10 granules (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT count(*) FROM simple_table WHERE doc LIKE \'%seek%\';')
        AND type='QueryFinish'
        AND result_rows==1
    ORDER BY query_start_time DESC
    LIMIT 1;

DROP TABLE IF EXISTS simple_table sync;
