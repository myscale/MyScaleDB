add_headers_and_sources(clickhouse_vector_index .)
add_headers_and_sources(clickhouse_vector_index Common)
add_headers_and_sources(clickhouse_vector_index Interpreters)
add_headers_and_sources(clickhouse_vector_index Parsers)
add_headers_and_sources(clickhouse_vector_index Processors)
add_headers_and_sources(clickhouse_vector_index Storages)
add_headers_and_sources(clickhouse_vector_index Utils)
add_headers_and_sources(clickhouse_vector_index Cache)
add_headers_and_sources(clickhouse_vector_index TableFunctions)
add_library(clickhouse_vector_index ${clickhouse_vector_index_sources})
target_link_libraries(clickhouse_vector_index
    PUBLIC
	dbms
    PRIVATE
	ch_contrib::search_index
    daemon
)

target_include_directories(clickhouse_vector_index 
    SYSTEM PUBLIC 
    ${CMAKE_SOURCE_DIR}/contrib/search-index
)

if (TARGET ch_rust::supercrate)
    target_link_libraries(clickhouse_vector_index
        PRIVATE
        ch_rust::supercrate
    )
endif()