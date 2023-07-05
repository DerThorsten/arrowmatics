# basics
cmake_minimum_required(VERSION 3.10)
project(arrowmatics)

# add nanorrow library
add_library(nanoarrow STATIC ${CMAKE_CURRENT_SOURCE_DIR}/nanoarrow/nanoarrow.c)
target_include_directories(nanoarrow PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/nanoarrow)

find_package(pybind11 REQUIRED)
pybind11_add_module(_arrowmatics py_main.cpp)

# link the nanoarrow library to the arrowmatics module
target_link_libraries(_arrowmatics PRIVATE nanoarrow)
target_include_directories(_arrowmatics PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/nanoarrow)

# copy the arrowmatics.so file to the python module directory
add_custom_command(TARGET _arrowmatics POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy
                       $<TARGET_FILE:_arrowmatics>
                       ${CMAKE_CURRENT_SOURCE_DIR}/arrowmatics)



# run pytest from cmake
add_custom_target(python_tests
    ${CMAKE_COMMAND}
    -E env PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}
    ${PYTHON_EXECUTABLE} -m
    pytest ${CMAKE_CURRENT_SOURCE_DIR}/tests -vvv -s
    DEPENDS _arrowmatics
    WORKING_DIRECTORY ${PY_MODULE_DIR}
    COMMENT "Running pytest"
    USES_TERMINAL)