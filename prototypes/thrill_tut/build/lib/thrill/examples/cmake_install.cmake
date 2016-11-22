# Install script for directory: /home/eagle/dev/thrill_tut/lib/thrill/examples

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/k-means/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/logistic_regression/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/page_rank/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/select/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/sleep/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/suffix_sorting/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/terasort/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/tutorial/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/vfs_tool/cmake_install.cmake")
  include("/home/eagle/dev/thrill_tut/build/lib/thrill/examples/word_count/cmake_install.cmake")

endif()

