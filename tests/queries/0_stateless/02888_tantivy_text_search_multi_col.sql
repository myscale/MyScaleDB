-- Tags: no-tsan, no-msan

SET allow_experimental_inverted_index = 1;
SET log_queries = 1;
SET mutations_sync = 1;
SET enable_fts_index_for_string_functions = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;


DROP TABLE IF EXISTS tb sync;

CREATE TABLE tb(`id` UInt64,`col1` String,`col2` String,`col3` String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tb VALUES (0,'An apple a day keeps','The early bird catches worm','Actions speak louder than words'),(1,'Better late than never ever','Easy come easy go always','Strike while the iron hot'),(2,'Too many cooks spoil broth','A stitch in time saves','Dont count your chickens before'),(3,'Every dog has its day','Let sleeping dogs lie peacefully','When in Rome do as'),(4,'The grass is always greener','Honesty is the best policy','Practice makes perfect every time'),(5,'Dont put all your eggs','A picture paints 1000 words','Judge not by its cover'),(6,'All that glitters isnt gold','Every cloud has silver lining','Hope for best prepare worst'),(7,'The pen is mightier than','Rome wasnt built in day','Theres no place like home'),(8,'What is grease means oo','Birds of feather flock together','A watched pot never boils'),(9,'The squeaky wheel gets grease','Never put off till tomorrow','Absence makes the heart fonder');


-- Tokenizer configuration
-- {
--   "col1": {
--     "tokenizer": {
--       "type": "stem",
--       "stop_word_filters": [
--         "english",
--         "french"
--       ],
--       "stem_languages": [
--         "german",
--         "english"
--       ],
--       "length_limit": 60
--     }
--   },
--   "col2": {
--     "tokenizer": {
--       "type": "simple"
--     }
--   }
-- }
ALTER TABLE tb ADD INDEX multi_idx (col1, col2, col3) TYPE fts('{ "col1": { "tokenizer": { "type": "stem", "stop_word_filters": ["english", "french"], "stem_languages": ["german", "english"], "length_limit": 60} }, "col2": { "tokenizer": {"type": "simple"} } }') GRANULARITY 1;
ALTER TABLE tb MATERIALIZE INDEX multi_idx SETTINGS mutations_sync=2;

SELECT '[Test Case 1]: function equals / notEquals / == / !=';

SELECT count(*) FROM tb WHERE col1 == 'Dont put all your eggs';
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 == ''Dont')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col1 != 'Dont put all your eggs';
SYSTEM FLUSH LOGS;
SELECT read_rows==10 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 != ''Dont')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE equals(col2, 'A stitch in time saves');
SYSTEM FLUSH LOGS;
SELECT read_rows==6 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE equals(col2, ''A stitch in time saves')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE notEquals(col3, 'Hope for best prepare worst');
SYSTEM FLUSH LOGS;
SELECT read_rows==10 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE notEquals(col3, ''Hope for best prepare worst')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT '[Test Case 2]: function hasToken / hasTokenOrNull / NOT hasToken';

SELECT count(*) FROM tb WHERE hasToken(col1, 'grease');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasToken(col1, ''grease')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE not hasToken(col1, 'grease');
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE not hasToken(col1, ''grease')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE hasToken(col2, 'stitch');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasToken(col2, ''stitch')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, 'practice');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, ''practice')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, 'Practice');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, ''Practice')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT '[Test Case 3]: function like / not like';

ALTER TABLE tb DROP INDEX multi_idx;
ALTER TABLE tb ADD INDEX multi_idx (col1, col2, col3) TYPE fts('{ "col1": { "tokenizer": { "type": "raw" } }, "col2": { "tokenizer": {"type": "raw"} }, "col3": { "tokenizer": {"type": "raw"} } }') GRANULARITY 1;
ALTER TABLE tb MATERIALIZE INDEX multi_idx SETTINGS mutations_sync=2;

SELECT count(*) FROM tb WHERE col1 like '%ll%yo%';
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 like ''%ll%yo%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col2 like 'Honest%';
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col2 like ''Honest%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col3 like '%o____o%';
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col3 like ''%o____o%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col3 not like '%o____o%';
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col3 not like ''%o____o%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT '[Test Case 4]: function startsWith / endsWith';

SELECT count(*) FROM tb WHERE startsWith(col1, 'Better');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE startsWith(col1, ''Better')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE endsWith(col3, 'time');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE endsWith(col3, ''time')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT '[Test Case 5]: function multiSearchAny / not multiSearchAny';

ALTER TABLE tb DROP INDEX multi_idx;
ALTER TABLE tb ADD INDEX multi_idx (col1, col2, col3) TYPE fts GRANULARITY 1;
ALTER TABLE tb MATERIALIZE INDEX multi_idx SETTINGS mutations_sync=2;

SELECT count(*) FROM tb WHERE multiSearchAny(col1, ['grass', 'eggs']);
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE multiSearchAny(col1, [''grass'', ''eggs''])')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;


SELECT count(*) FROM tb WHERE multiSearchAny(col3, ['prepare', 'home', 'never', 'iron']);
SYSTEM FLUSH LOGS;
SELECT read_rows==6 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE multiSearchAny(col3, [''prepare''')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE not multiSearchAny(col1, ['grass', 'eggs']);
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE not multiSearchAny(col1, [''grass'', ''eggs''])')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

DROP TABLE IF EXISTS tb sync;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT '[Test Case 6]: function mapContains';

CREATE TABLE tbm (id UInt64, col1 Map(String,String), col2 String) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO tbm VALUES(0,{'fruit cat':'An apple a day keeps', 'idiom':'The early bird catches worm'}, 'Actions speak louder than words'),(1,{'time':'Better late than never ever', 'life':'Easy come easy go always'}, 'Strike while the iron hot'),(2,{'cooking':'Too many cooks spoil broth', 'sewing':'A stitch in time saves'}, 'Dont count your chickens before'),(3,{'animal life':'Every dog has its day', 'advice':'Let sleeping dogs lie peacefully'}, 'When in Rome do as'),(4,{'nature pic':'The grass is always greener', 'virtue':'Honesty is the best policy'}, 'Practice makes perfect every time'),(5,{'caution':'Dont put all your eggs', 'art':'A picture paints 1000 words'}, 'Judge not by its cover'),(6,{'appearance':'All that glitters isnt gold', 'optimism':'Every cloud has silver lining'}, 'Hope for best prepare worst'),(7,{'writing adder':'The pen is mightier than', 'patience':'Rome wasnt built in day'}, 'Theres no place like home'),(8,{'question':'What is grease means oo', 'relationship':'Birds of feather flock together'}, 'A watched pot never boils'),(9,{'persistence':'The squeaky wheel gets grease', 'time':'Never put off till tomorrow'}, 'Absence makes the heart fonder');

ALTER TABLE tbm ADD INDEX multi_idx (mapKeys(col1), col2) TYPE fts('{ "mapKeys(col1)": { "tokenizer": { "type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": { "tokenizer": {"type": "simple"} } }') GRANULARITY 1;
ALTER TABLE tbm MATERIALIZE INDEX multi_idx SETTINGS mutations_sync=2;

SELECT count(*) FROM tbm WHERE mapContains(col1, 'nature pic') SETTINGS allow_experimental_analyzer=0;
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tbm WHERE mapContains(col1, ''nature pic') 
        AND type='QueryFinish' 
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

SELECT count(*) FROM tbm WHERE hasToken(col2, 'Strike');
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tbm WHERE hasToken(col2, ''Strike')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time DESC
    LIMIT 1;

DROP TABLE IF EXISTS tbm sync;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT '[Test Case 7]: function has';

DROP TABLE IF EXISTS tba;
CREATE TABLE tba(`id` UInt64, `col1` Array(String), `col2` String) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tba VALUES(0, ['The squeaky wheel gets grease', 'Never put off till tomorrow'], 'Absence makes the heart fonder'),(1, ['An apple a day keeps', 'The early bird catches worm'], 'Actions speak louder than words'),(2, ['Better late than never ever', 'Easy come easy go always'], 'Strike while the iron hot'),(3, ['Too many cooks spoil broth', 'A stitch in time saves'], 'Dont count your chickens before'),(4, ['Every dog has its day', 'Let sleeping dogs lie peacefully'], 'When in Rome do as'),(5, ['The grass is always greener', 'Honesty is the best policy'], 'Practice makes perfect every time'),(6, ['Dont put all your eggs', 'A picture paints 1000 words'], 'Judge not by its cover'),(7, ['All that glitters isnt gold', 'Every cloud has silver lining'], 'Hope for best prepare worst'),(8, ['The pen is mightier than', 'Rome wasnt built in day'], 'Theres no place like home'),(9, ['What is grease means oo', 'Birds of feather flock together'], 'A watched pot never boils');

ALTER TABLE tba ADD INDEX multi_idx (col1, col2) TYPE fts('{ "col1": { "tokenizer": { "type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": { "tokenizer": {"type": "simple"} } }') GRANULARITY 1;
ALTER TABLE tba MATERIALIZE INDEX multi_idx SETTINGS mutations_sync=2;

SELECT count(*) FROM tba WHERE has(col1, 'An apple a day keeps');
SYSTEM FLUSH LOGS;
SELECT read_rows==6 from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tba WHERE has(col1, ''An apple a day keeps') 
        AND type='QueryFinish'
        AND result_rows==1
        ORDER BY query_start_time DESC
    LIMIT 1;

DROP TABLE IF EXISTS tba;
