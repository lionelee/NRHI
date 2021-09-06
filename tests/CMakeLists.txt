
# build CCEH
build_test(cceh_test_ycsb_micro CCEH/cceh_test_ycsb.cpp)
build_test(cceh_test_ycsb_macro CCEH/cceh_test_ycsb_macro.cpp)
build_test(cceh_test_insert_micro CCEH/cceh_test_insert.cpp)
build_test(cceh_test_insert_macro CCEH/cceh_test_insert_macro.cpp)

# build P-CLHT
build_test(clht_test_ycsb_micro CLHT/clht_test_ycsb.cpp)
build_test(clht_test_ycsb_macro CLHT/clht_test_ycsb_macro.cpp)
build_test(clht_test_insert_micro CLHT/clht_test_insert.cpp)
build_test(clht_test_insert_macro CLHT/clht_test_insert_macro.cpp)

# build Concurrent Hash Map
build_test(cmap_test_ycsb_micro CMAP/cmap_test_ycsb.cpp)
build_test(cmap_test_ycsb_macro CMAP/cmap_test_ycsb_macro.cpp)
build_test(cmap_test_insert_micro CMAP/cmap_test_insert.cpp)
build_test(cmap_test_insert_macro CMAP/cmap_test_insert_macro.cpp)

# build Level Hashing
build_test(level_test_ycsb_micro Level/level_test_ycsb.cpp)
build_test(level_test_ycsb_macro Level/level_test_ycsb_macro.cpp)
build_test(level_test_insert_micro Level/level_test_insert.cpp)
build_test(level_test_insert_macro Level/level_test_insert_macro.cpp)

# build Clevel Hashing
build_test(clevel_test_ycsb_micro Clevel/clevel_test_ycsb.cpp)
build_test(clevel_test_ycsb_macro Clevel/clevel_test_ycsb_macro.cpp)
build_test(clevel_test_insert_micro Clevel/clevel_test_insert.cpp)
build_test(clevel_test_insert_macro Clevel/clevel_test_insert_macro.cpp)

# build NRHI
build_test(nrhi_test_cli NRHI/nrhi_test_cli.cpp)
build_test(nrhi_test_ycsb_micro NRHI/nrhi_test_ycsb.cpp)
build_test(nrhi_test_ycsb_macro NRHI/nrhi_test_ycsb_macro.cpp)
build_test(nrhi_test_insert_micro NRHI/nrhi_test_insert.cpp)
build_test(nrhi_test_insert_macro NRHI/nrhi_test_insert_macro.cpp)
