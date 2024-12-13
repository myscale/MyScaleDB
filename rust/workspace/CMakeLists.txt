function(clickhouse_import_crate)
    # This is a workaround for Corrosion case sensitive build type matching in
    # _generator_add_cargo_targets(), that leads to different paths in
    # IMPORTED_LOCATION and real path of the library.
    #
    # It uses CMAKE_CONFIGURATION_TYPES and $<CONFIG>, so here we preserve the
    # case of ${CMAKE_BUILD_TYPE} in ${CMAKE_CONFIGURATION_TYPES}.
    if ("${CMAKE_BUILD_TYPE_UC}" STREQUAL "DEBUG")
        set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE};release")
    else()
        set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE};debug")
    endif()

    if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
        set(profile "")
    else()
        if (ENABLE_THINLTO)
            set(profile "release-thinlto")
        else()
            if (CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO")
                set(profile "relwithdebinfo")
            else()
                set(profile "release")
            endif()
        endif()
    endif()

    # Note, here --offline is not used, since on CI vendor archive is used, and
    # passing --offline here will be inconvenient for local development.
    corrosion_import_crate(${ARGN} NO_STD PROFILE ${profile} LOCKED FLAGS)
endfunction()

# -Wno-dollar-in-identifier-extension: cxx bridge complies names with '$'
# -Wno-unused-macros: unused CXXBRIDGE1_RUST_STRING
set(CXXBRIDGE_CXXFLAGS "-Wno-dollar-in-identifier-extension -Wno-unused-macros")
set(RUST_CXXFLAGS "${RUST_CXXFLAGS} ${CXXBRIDGE_CXXFLAGS}")
message(STATUS "RUST_CXXFLAGS (for skim): ${RUST_CXXFLAGS}")

configure_file(".cargo/config.toml.in" ".cargo/config.toml" @ONLY)
configure_file("tantivy_search/.cargo/config.toml.in" "tantivy_search/.cargo/config.toml" @ONLY)
configure_file("tantivy_search/build.rs.in" "tantivy_search/build.rs" @ONLY)

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

target_include_directories(_ch_rust_prql INTERFACE prql/include)
add_library(ch_rust::prql ALIAS _ch_rust_prql)

# target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
# add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)

# target_include_directories(tantivy_search INTERFACE tantivy_search/include)
# add_library(ch_rust::tantivy_search ALIAS tantivy_search)

# To resolve the cxx symbol conflict that occurs when compiling tantivy-search and skim together,
# the implementations of tantivy-search and skim are integrated into the supercreate library.
target_include_directories(supercreate INTERFACE skim/include tantivy_search/include)
target_compile_options(supercreate INTERFACE "-Wno-dollar-in-identifier-extension") 

add_library(ch_skim_rust INTERFACE)
target_link_libraries(ch_skim_rust INTERFACE supercreate)
add_library(ch_rust::skim ALIAS ch_skim_rust)

add_library(ch_tantivy_search_rust INTERFACE)
target_link_libraries(ch_tantivy_search_rust INTERFACE supercreate)
add_library(ch_rust::tantivy_search ALIAS ch_tantivy_search_rust)

