cmake_minimum_required(VERSION 3.17.0)
project(Task_3)

set(CMAKE_CXX_STANDARD 17)

# gruenau2-6 needs to link this library, on some other systems this needs to be disabled
link_libraries(stdc++fs)

add_executable(Task_3 main.cpp
        header/record.h
        record.cpp
        header/block.h
        block.cpp
        header/buffer_manager.h
        buffer_manager.cpp)