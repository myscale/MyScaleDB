-- Tags: replica, no-parallel

SET allow_experimental_database_replicated = 1;
SET database_replicated_default_zk_path_prefix = '/clickhouse/test/databases/';
DROP DATABASE IF EXISTS replicated_database_params;

CREATE DATABASE replicated_database_params ENGINE = Replicated('/clickhouse/test1/databases/replicated_database_params', '{shard}', '{replica}');
SHOW CREATE DATABASE replicated_database_params;
DROP DATABASE replicated_database_params;

CREATE DATABASE replicated_database_params ENGINE = Replicated;
SHOW CREATE DATABASE replicated_database_params;
DROP DATABASE replicated_database_params;

