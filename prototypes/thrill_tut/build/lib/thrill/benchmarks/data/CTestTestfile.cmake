# CMake generated Testfile for 
# Source directory: /home/eagle/dev/thrill_tut/lib/thrill/benchmarks/data
# Build directory: /home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(data_benchmark_file_consume "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "file" "-b" "64mi" "size_t" "consume")
set_tests_properties(data_benchmark_file_consume PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;")
add_test(data_benchmark_file_keep "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "file" "-b" "64mi" "size_t" "keep")
set_tests_properties(data_benchmark_file_keep PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;")
add_test(data_benchmark_blockqueue_consume "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "blockqueue" "-b" "64mi" "size_t" "consume")
set_tests_properties(data_benchmark_blockqueue_consume PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;")
add_test(data_benchmark_blockqueue_keep "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "blockqueue" "-b" "64mi" "size_t" "keep")
set_tests_properties(data_benchmark_blockqueue_keep PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;")
add_test(data_benchmark_cat_stream_1factor_consume "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "cat_stream_1factor" "-b" "16mib" "size_t" "consume")
set_tests_properties(data_benchmark_cat_stream_1factor_consume PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=3")
add_test(data_benchmark_cat_stream_1factor_keep "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "cat_stream_1factor" "-b" "16mib" "size_t" "keep")
set_tests_properties(data_benchmark_cat_stream_1factor_keep PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=4")
add_test(data_benchmark_mix_stream_1factor_consume "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "mix_stream_1factor" "-b" "16mib" "size_t" "consume")
set_tests_properties(data_benchmark_mix_stream_1factor_consume PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=3")
add_test(data_benchmark_mix_stream_1factor_keep "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "mix_stream_1factor" "-b" "16mib" "size_t" "keep")
set_tests_properties(data_benchmark_mix_stream_1factor_keep PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=4")
add_test(data_benchmark_cat_stream_all2all_consume "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "cat_stream_all2all" "-b" "32mib" "size_t" "consume")
set_tests_properties(data_benchmark_cat_stream_all2all_consume PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=3")
add_test(data_benchmark_cat_stream_all2all_keep "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "cat_stream_all2all" "-b" "32mib" "size_t" "keep")
set_tests_properties(data_benchmark_cat_stream_all2all_keep PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=4")
add_test(data_benchmark_mix_stream_all2all_consume "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "mix_stream_all2all" "-b" "16mib" "size_t" "consume")
set_tests_properties(data_benchmark_mix_stream_all2all_consume PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=3")
add_test(data_benchmark_mix_stream_all2all_keep "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "mix_stream_all2all" "-b" "16mib" "size_t" "keep")
set_tests_properties(data_benchmark_mix_stream_all2all_keep PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;THRILL_LOCAL=4")
add_test(data_benchmark_scatter_consume "/home/eagle/dev/thrill_tut/build/lib/thrill/benchmarks/data/data_benchmark" "scatter" "-b" "64mi" "size_t" "consume")
set_tests_properties(data_benchmark_scatter_consume PROPERTIES  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;;THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;;")
