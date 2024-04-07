macro(add_glob cur_list)
    file(GLOB __tmp CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${ARGN})
    list(APPEND ${cur_list} ${__tmp})
endmacro()

macro(add_headers_and_sources prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.h)
endmacro()

macro(add_headers_only prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
endmacro()

function(remove_specific_headers_and_sources prefix)
    set(files_to_remove ${ARGN})
    set(root_path "${CMAKE_CURRENT_SOURCE_DIR}")

    foreach(file IN LISTS files_to_remove)
        get_filename_component(full_path "${file}" ABSOLUTE)
        if("${full_path}" MATCHES "^${root_path}")
            file(RELATIVE_PATH relative_path "${root_path}" "${full_path}")
            set(file_to_remove "${relative_path}")
        else()
            set(file_to_remove "${file}")
        endif()
        # remove files from headers and sources
        list(REMOVE_ITEM ${prefix}_headers "${file_to_remove}")
        list(REMOVE_ITEM ${prefix}_sources "${file_to_remove}")
    endforeach()

    # update headers and sources
    set(${prefix}_headers ${${prefix}_headers} PARENT_SCOPE)
    set(${prefix}_sources ${${prefix}_sources} PARENT_SCOPE)
endfunction()
