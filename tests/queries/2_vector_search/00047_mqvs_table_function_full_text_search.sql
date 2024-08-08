
SET allow_experimental_inverted_index = 1;

DROP TABLE IF EXISTS test_inverted_multi SYNC;
CREATE TABLE test_inverted_multi(`id` UInt64,`col1` String,`col2` String,`col3` String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2; 
ALTER TABLE test_inverted_multi ADD INDEX multi_idx (col1, col2, col3) TYPE fts('{ "col1": {"tokenizer": {"type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": {"tokenizer": {"type": "simple"}}, "col3": {"tokenizer": {"type": "stem"}}}') GRANULARITY 1;

INSERT INTO test_inverted_multi VALUES (0,'An apple a day keeps','The early bird catches worm','Actions speak louder than words'),(1,'Better late than never ever','Easy come easy go always','Strike while the iron hot'),(2,'Too many cooks spoil broth','A stitch in time saves','Dont count your chickens before'),(3,'Every dog has its day','Let sleeping dogs lie peacefully','When in Rome do as'),(4,'The grass is always greener','Honesty is the best policy','Practice makes perfect every time'),(5,'Dont put all your eggs','A picture paints 1000 words','Judge not by its cover'),(6,'All that glitters isnt gold','Every cloud has silver lining','Hope for best prepare worst'),(7,'The pen is mightier than','Rome wasnt built in day','Theres no place like home'),(8,'What is grease means oo','Birds of feather flock together','A watched pot never boils'),(9,'The squeaky wheel gets grease','Never put off till tomorrow','Absence makes the heart fonder');

SELECT 'full_text_search with_score=0';
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 'col1:apple or col2:easy');

SELECT 'full_text_search + limit 1';
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 'col1:apple or col2:easy') limit 1;

-- When with_score = 0 (default), cannot SELECT bm25_score
SELECT id, bm25_score FROM full_text_search(test_inverted_multi, multi_idx, 'col1:apple or col2:easy'); -- {serverError UNKNOWN_IDENTIFIER}

-- When with_score = 1, can SELECT bm25_score
SELECT 'full_text_search with_score=1';
SELECT id, bm25_score FROM full_text_search(test_inverted_multi, multi_idx, 'col1:apple or col2:easy', 1);

-- invalid arguments
SELECT id FROM full_text_search(not_exist_table, multi_idx, 'col1:apple or col2:easy'); -- {serverError UNKNOWN_TABLE}
SELECT id FROM full_text_search(test_inverted_multi, not_exist_idx, 'col1:apple or col2:easy'); -- {serverError ILLEGAL_TEXT_SEARCH}
SELECT id FROM full_text_search(test_inverted_multi, multi_idx); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 'col1:apple or col2:easy', 1, 1, 'OR', 0); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 1, 1, 'OR', 0); -- {serverError BAD_ARGUMENTS}

-- invalid value for operator
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 'col1:apple or col2:easy', operator='or'); -- {serverError BAD_ARGUMENTS}
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 'col1:apple or col2:easy', 1, 1, 'abc'); -- {serverError BAD_ARGUMENTS}

SELECT 'full_text_search + where';
SELECT id, col1 FROM full_text_search(test_inverted_multi, multi_idx, 'col1:has or col2:has') where id < 5 limit 1;

-- query on non-exist text column when enable_nlq = true;
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 'col1:has or not_col2:has'); -- {serverError TANTIVY_SEARCH_INTERNAL_ERROR}

-- query on non-exist text column when enable_nlq = false;
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, 'col1:has or not_col2:has', enable_nlq=0);

SELECT 'query with subquery as query_text';
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, (SELECT 'col1:has'));

SELECT 'query with WITH clause as query_text';
WITH (SELECT 'col1:has') as query
SELECT id FROM full_text_search(test_inverted_multi, multi_idx, query);

DROP TABLE IF EXISTS test_inverted_multi SYNC;

-- Table with column name 'bm25_score'
DROP TABLE IF EXISTS test_inverted_same SYNC;
CREATE TABLE test_inverted_same(`id` UInt64,`bm25_score` String,`col2` String,`col3` String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
ALTER TABLE test_inverted_same ADD INDEX multi_idx (bm25_score, col2, col3) TYPE fts('{ "bm25_score": {"tokenizer": {"type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": {"tokenizer": {"type": "simple"}}, "col3": {"tokenizer": {"type": "stem"}}}') GRANULARITY 1;
INSERT INTO test_inverted_same VALUES (0,'An apple a day keeps','The early bird catches worm','Actions speak louder than words'),(1,'Better late than never ever','Easy come easy go always','Strike while the iron hot');

SELECT * FROM full_text_search(test_inverted_same, multi_idx, 'col1:apple or col2:easy'); -- {serverError ILLEGAL_TEXT_SEARCH}

SELECT 'select from table with bm25_score column';
SELECT id, bm25_score FROM test_inverted_same;
DROP TABLE test_inverted_same SYNC;

-- Multiple parts
DROP TABLE IF EXISTS test_inverted_multi_parts SYNC;
CREATE TABLE test_inverted_multi_parts(`id` UInt64,`col1` String,`col2` String,`col3` String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2; 
ALTER TABLE test_inverted_multi_parts ADD INDEX multi_idx (col1, col2, col3) TYPE fts('{ "col1": {"tokenizer": {"type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": {"tokenizer": {"type": "simple"}}, "col3": {"tokenizer": {"type": "stem"}}}') GRANULARITY 1;

INSERT INTO test_inverted_multi_parts VALUES (0,'An apple a day keeps','The early bird catches worm','Actions speak louder than words'),(1,'Better late than never ever','Easy come easy go always','Strike while the iron hot'),(2,'Too many cooks spoil broth','A stitch in time saves','Dont count your chickens before'),(3,'Every dog has its day','Let sleeping dogs lie peacefully','When in Rome do as'),(4,'The grass is always greener','Honesty is the best policy','Practice makes perfect every time');
INSERT INTO test_inverted_multi_parts VALUES (5,'Dont put all your eggs','A picture paints 1000 words','Judge not by its cover'),(6,'All that glitters isnt gold','Every cloud has silver lining','Hope for best prepare worst'),(7,'The pen is mightier than','Rome wasnt built in day','Theres no place like home'),(8,'What is grease means oo','Birds of feather flock together','A watched pot never boils'),(9,'The squeaky wheel gets grease','Never put off till tomorrow','Absence makes the heart fonder');

SELECT 'full_text_search with_score=1 on two parts';
SELECT id, col1, col2, bm25_score FROM full_text_search(test_inverted_multi_parts, multi_idx, 'col1:apple or col2:easy', 1);

DROP TABLE IF EXISTS test_inverted_multi_parts SYNC;
