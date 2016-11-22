# CMake generated Testfile for 
# Source directory: /home/eagle/dev/thrill_tut/lib/thrill/benchmarks/net
# Build directory: /home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/net
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(net_benchmark_ping_pong_local3 "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/net/net_benchmark" "ping_pong" "10")
set_tests_properties(net_benchmark_ping_pong_local3 PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=3")
add_test(net_benchmark_ping_pong_local4 "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/net/net_benchmark" "ping_pong" "10")
set_tests_properties(net_benchmark_ping_pong_local4 PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=4")
add_test(net_benchmark_bandwidth_local3 "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/net/net_benchmark" "bandwidth" "10")
set_tests_properties(net_benchmark_bandwidth_local3 PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=3")
add_test(net_benchmark_bandwidth_local4 "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/net/net_benchmark" "bandwidth" "10")
set_tests_properties(net_benchmark_bandwidth_local4 PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=4")
add_test(net_benchmark_prefixsum_local3 "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/net/net_benchmark" "prefixsum" "-r" "10")
set_tests_properties(net_benchmark_prefixsum_local3 PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=3")
add_test(net_benchmark_prefixsum_local4 "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/net/net_benchmark" "prefixsum" "-r" "10")
set_tests_properties(net_benchmark_prefixsum_local4 PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=4")
