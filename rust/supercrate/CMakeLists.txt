clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

# -Wno-dollar-in-identifier-extension: cxx bridge complies names with '$'
# -Wno-unused-macros: unused CXXBRIDGE1_RUST_STRING
set(CXXBRIDGE_CXXFLAGS "-Wno-dollar-in-identifier-extension -Wno-unused-macros")
set(RUST_CXXFLAGS "${RUST_CXXFLAGS} ${CXXBRIDGE_CXXFLAGS}")
# NOTE: requires RW access for the source dir
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/libs/skim/build.rs.in" "${CMAKE_CURRENT_SOURCE_DIR}/libs/skim/build.rs" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/libs/skim/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/libs/skim/.cargo/config.toml" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/libs/tantivy_search/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/libs/tantivy_search/.cargo/config.toml" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/libs/tantivy_search/build.rs.in" "${CMAKE_CURRENT_SOURCE_DIR}/libs/tantivy_search/build.rs" @ONLY)

# corrosion 2022 edition doesn't fit this CMake.
# set(cxx_bridge_binary_folder ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/cargo/build/${Rust_CARGO_TARGET_CACHED}/cxxbridge)
set(cxx_bridge_binary_folder ${CMAKE_BINARY_DIR}/cargo/build/${Rust_CARGO_TARGET_CACHED}/cxxbridge)
set(cxx_bridge_source_file "src/lib.rs")

set(cxx_header ${cxx_bridge_binary_folder}/rust/cxx.h)
set(cxx_include ${cxx_bridge_binary_folder}/rust)
# skim library cxx header and source.
set(skim_header ${cxx_bridge_binary_folder}/skim/${cxx_bridge_source_file}.h)
set(skim_source ${cxx_bridge_binary_folder}/skim/${cxx_bridge_source_file}.cc)
# tantivy_search cxx header and source.
set(tantivy_search_header ${cxx_bridge_binary_folder}/tantivy_search/${cxx_bridge_source_file}.h)
set(tantivy_search_source ${cxx_bridge_binary_folder}/tantivy_search/${cxx_bridge_source_file}.cc)

# get include path
set(blake3_include ${CMAKE_CURRENT_BINARY_DIR}/libs/blake3/include)
set(skim_include ${CMAKE_CURRENT_BINARY_DIR}/libs/skim/include)
set(tantivy_search_include ${CMAKE_CURRENT_BINARY_DIR}/libs/tantivy_search/include)

message(STATUS "> cxx header: ${cxx_header}")
message(STATUS "> cxx include: ${cxx_include}")
message(STATUS "> tantivy_search header: ${tantivy_search_header}")
message(STATUS "> tantivy_search source: ${tantivy_search_source}")
message(STATUS "> tantivy_search include: ${tantivy_search_include}")

add_custom_command(
    OUTPUT
    ${cxx_header}
    ${skim_header}
    ${skim_source}
    ${tantivy_search_header}
    ${tantivy_search_source}
    COMMAND
    DEPENDS supercrate-static
    # DEPENDS cargo-build_supercrate  # releated with corrosion version.
    COMMENT "Fixing cmake to find source files"
)


add_library(supercrate_cxxbridge
    ${cxx_header}
    ${skim_header}
    ${skim_source}
    ${tantivy_search_header}
    ${tantivy_search_source}
)
# cxx bridge compiles such bindings
set_target_properties(supercrate_cxxbridge PROPERTIES COMPILE_FLAGS "${CXXBRIDGE_CXXFLAGS}")
set_target_properties(supercrate_cxxbridge PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(supercrate_cxxbridge
    PUBLIC
    ${blake3_include}
    ${skim_include}
    ${tantivy_search_include}
    ${cxx_include}
)

#-Wno-dollar-in-identifier-extension -Wno-unused-macros
target_compile_options(supercrate_cxxbridge PUBLIC -Wno-dollar-in-identifier-extension)
target_compile_options(supercrate_cxxbridge PUBLIC -Wno-unused-macros)

# Create total target with alias with given namespace
add_library(supercrate-total INTERFACE)
target_link_libraries(supercrate-total
    INTERFACE
    supercrate_cxxbridge
    supercrate
)

# for end-user to link into project
add_library(ch_rust::supercrate ALIAS supercrate-total)
