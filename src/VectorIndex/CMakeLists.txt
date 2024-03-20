add_headers_and_sources(clickhouse_vector_index .)
add_headers_and_sources(clickhouse_vector_index Common)
add_headers_and_sources(clickhouse_vector_index Interpreters)
add_headers_and_sources(clickhouse_vector_index Parsers)
add_headers_and_sources(clickhouse_vector_index Processors)
add_headers_and_sources(clickhouse_vector_index Storages)
add_headers_and_sources(clickhouse_vector_index Utils)
add_headers_and_sources(clickhouse_vector_index Cache)
add_library(clickhouse_vector_index ${clickhouse_vector_index_sources})
include_directories(${ClickHouse_SOURCE_DIR}/contrib/search-index/contrib/google-research/scann/contrib/tensorflow/include)
include_directories(${ClickHouse_SOURCE_DIR}/contrib/search-index/contrib/google-research/scann/contrib/tensorflow/include/src)
target_link_libraries(clickhouse_vector_index
    PUBLIC
	dbms
    PRIVATE
	ch_contrib::search_index
)