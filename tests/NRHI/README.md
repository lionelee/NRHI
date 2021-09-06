# Benchmark files of NRHI

+ `nrhi_test_cli`: interactive command line tool to test NRHI
```
usage: ./nrhi_test_cli <pool_file>  
    <pool_file> is required by PMDK to create mempool.
```

+ `nrhi_test_ycsb`: test for micro YCSB workloads
```
usage: ./nrhi_test_ycsb <pool_file> <load_file> <run_file> <thread_num>
    <pool_file> is required by PMDK to create mempool.
    <load_file> is workload file for load phase.
    <run_file> is workload file for run phase.
    <thread_num> is the number of request threads.
``` 

+ `nrhi_test_ycsb`: test for macro YCSB workloads
+ `nrhi_test_insert`: test for micro YCSB Load workload
+ `nrhi_test_insert_macro`: test for macro YCSB Load workload
