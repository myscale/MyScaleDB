SET allow_experimental_inverted_index = 1;
SET enable_brute_force_vector_search = 1;

DROP TABLE IF EXISTS test_inverted_multi SYNC;
CREATE TABLE test_inverted_multi(
    id UInt64, vector Array(Float32),
    col1 String, col2 String, col3 String,
    CONSTRAINT check_length_vec CHECK length(vector) = 4
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

ALTER TABLE test_inverted_multi ADD INDEX multi_idx (col1, col2, col3) TYPE fts('{ "col1": {"tokenizer": {"type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": {"tokenizer": {"type": "simple"}}, "col3": {"tokenizer": {"type": "stem"}}}') GRANULARITY 1;

INSERT INTO test_inverted_multi VALUES (0,[0,0,0,0],'An apple a day keeps','The early bird catches worm','Actions speak louder than words'),(1,[1,1,1,1],'Better late than never ever','Easy come easy go always','Strike while the iron hot'),(2,[2,2,2,2],'Too many cooks spoil broth','A stitch in time saves','Dont count your chickens before'),(3,[3,3,3,3],'Every dog has its day','Let sleeping dogs lie peacefully','When in Rome do as'),(4,[4,4,4,4],'The grass is always greener','Honesty is the best policy','Practice makes perfect every time'),(5,[5,5,5,5],'Dont put all your eggs','A picture paints 1000 words','Judge not by its cover'),(6,[6,6,6,6],'All that glitters isnt gold','Every cloud has silver lining','Hope for best prepare worst'),(7,[7,7,7,7],'The pen is mightier than','Rome wasnt built in day','Theres no place like home'),(8,[8,8,8,8],'What is grease means oo','Birds of feather flock together','A watched pot never boils'),(9,[9,9,9,9],'The squeaky wheel gets grease','Never put off till tomorrow','Absence makes the heart fonder');

SELECT 'text search on col1 with multi_idx';
SELECT id, textsearch(col1, 'apple or dog') as score FROM test_inverted_multi ORDER BY score DESC LIMIT 5;

SELECT 'text search on col2 with multi_idx';
SELECT id, textsearch(col2, 'words') as score FROM test_inverted_multi ORDER BY score DESC LIMIT 5;

SELECT 'hybrid search on col1 with multi_idx';
SELECT id, hybridsearch('fusion_type=rsf')(vector, col1, [1.0,1,1,1], 'apple or dog') as score FROM test_inverted_multi ORDER BY score DESC LIMIT 5;

SELECT 'hybrid search on col3 with multi_idx';
SELECT id, hybridsearch('fusion_type=rsf')(vector, col3, [1.0,1,1,1], 'hot') as score FROM test_inverted_multi ORDER BY score DESC LIMIT 5;

DROP TABLE IF EXISTS test_inverted_multi SYNC;
