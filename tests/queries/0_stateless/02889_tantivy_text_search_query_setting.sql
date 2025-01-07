-- Tags: no-tsan, no-msan

SET allow_experimental_inverted_index = 1;
SET log_queries = 1;
SET mutations_sync = 1;
SET enable_fts_index_for_string_functions = 1;


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
--       "length_limit": 60,
--       "case_sensitive": true
--     }
--   },
--   "col2": {
--     "tokenizer": {
--       "type": "simple",
--       "case_sensitive": false
--     }
--   }
-- }
ALTER TABLE tb ADD INDEX multi_idx (col1, col2, col3) TYPE fts('{ "col1": { "tokenizer": { "type": "stem", "stop_word_filters": ["english", "french"], "stem_languages": ["german", "english"], "length_limit": 60, "case_sensitive": true} }, "col2": { "tokenizer": {"type": "simple", "case_sensitive": false} } }') GRANULARITY 1;
ALTER TABLE tb MATERIALIZE INDEX multi_idx SETTINGS mutations_sync=2;

SELECT '[Test Case 1]: function equals / notEquals / == / !=';

SELECT count(*) FROM tb WHERE col1 == 'Dont put all your eggs';
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 == ''Dont')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col1 == 'Dont put all your eggs' SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND trimRight(query) ilike '%Dont%'
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col1 != 'Dont put all your eggs';
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 != ''Dont')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE equals(col2, 'A stitch in time saves');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE equals(col2, ''A stitch in time saves')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE equals(col2, 'A stitch in time saves') SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE equals(col2, ''A s')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE notEquals(col3, 'Hope for best prepare worst');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE notEquals(col3, ''Hope for best prepare worst')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE notEquals(col3, 'Hope for best prepare worst') SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE notEquals(col3, ''Hope')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT '[Test Case 2]: function hasToken / hasTokenOrNull / NOT hasToken';

SELECT count(*) FROM tb WHERE hasToken(col1, 'grease');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasToken(col1, ''grease')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE hasToken(col1, 'grease') SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasToken(col1, ''gr')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE not hasToken(col1, 'grease');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE not hasToken(col1, ''grease')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


SELECT count(*) FROM tb WHERE not hasToken(col1, 'grease') SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE not hasToken(col1, ''gr')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE hasToken(col2, 'stitch');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasToken(col2, ''stitch')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, 'practice');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, ''practice')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, 'Practice');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, ''Practice')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


SELECT count(*) FROM tb WHERE hasTokenOrNull(col3, 'Practice') SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE hasTokenOrNull(col3')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT '[Test Case 3]: function like; in case sensitive column;';

SELECT count(*) FROM tb WHERE col1 like '%Dont%';
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 like ''%Dont%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col1 like '%Dont%' SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 like ''%Dont%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


SELECT count(*) FROM tb WHERE col1 like '%dont%';
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 like ''%dont%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


SELECT count(*) FROM tb WHERE col1 like '%dont%' SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 like ''%dont%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT '[Test Case 4]: function not like; in case sensitive column;';

SELECT count(*) FROM tb WHERE col1 not like '%Dont%';
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 not like ''%Dont%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE col1 not like '%Dont%' SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 not')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


SELECT count(*) FROM tb WHERE col1 not like '%dont%';
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 not like ''%dont%')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


SELECT count(*) FROM tb WHERE col1 not like '%dont%' SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE col1 not')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT '[Test Case 5]: function startsWith / endsWith; in case insensitive column';

SELECT count(*) FROM tb WHERE startsWith(col2, 'Honesty');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE startsWith(col2, ''Honesty')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE startsWith(col2, 'Rome') SETTINGS enable_fts_index_for_string_functions=0;
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE startsWith(col2, ''Rome')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE endsWith(col3, 'boils');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE endsWith(col3, ''boils')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;

SELECT count(*) FROM tb WHERE startsWith(col2, 'honesty');
SYSTEM FLUSH LOGS;
SELECT read_rows from system.query_log 
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND startsWith(trimRight(query), 'SELECT count(*) FROM tb WHERE startsWith(col2, ''honesty')
        AND type='QueryFinish'
        AND result_rows=1
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1;



