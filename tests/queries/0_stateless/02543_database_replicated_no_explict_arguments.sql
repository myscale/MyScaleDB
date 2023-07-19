-- Tags: replica
SET  allow_experimental_database_replicated=1;
DROP DATABASE IF EXISTS test_replicated_db;
CREATE DATABASE test_replicated_db engine = Replicated('/clickhouse/databases/test/test_replicated_db', '{shard}', '{replica}');
DROP DATABASE test_replicated_db;
SET database_replicated_allow_explicit_arguments=0;
CREATE DATABASE test_replicated_db engine = Replicated('/clickhouse/databases/test/test_replicated_db', '{shard}', '{replica}'); -- { serverError 80 }
DROP DATABASE IF EXISTS test_replicated_db;
