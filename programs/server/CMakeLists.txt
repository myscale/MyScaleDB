include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

set(CLICKHOUSE_SERVER_SOURCES
    MetricsTransmitter.cpp
    Server.cpp
)

set (LINK_RESOURCE_LIB INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:clickhouse_server_configs> -Wl,${NO_WHOLE_ARCHIVE}")

set (CLICKHOUSE_SERVER_LINK
    PRIVATE
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
        string_utils
    ${LINK_RESOURCE_LIB}

    PUBLIC
        daemon
)
if (TARGET ch_contrib::jemalloc)
    list(APPEND CLICKHOUSE_SERVER_LINK PRIVATE ch_contrib::jemalloc)
endif()
if (TARGET ch_contrib::azure_sdk)
    list(APPEND CLICKHOUSE_SERVER_LINK PRIVATE ch_contrib::azure_sdk)
endif()

clickhouse_program_add(server)

install(FILES config.xml users.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-server" COMPONENT clickhouse)

clickhouse_embed_binaries(
    TARGET clickhouse_server_configs
    RESOURCES config.xml users.xml embedded.xml play.html dashboard.html js/uplot.js
)
add_dependencies(clickhouse-server-lib clickhouse_server_configs)
