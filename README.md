# NRHI - Non-Rehashing Hash Index

NRHI is a hash index structure for persistent memory, which leverages layered structure of hash tables to perform resizing without rehashing and performs lock-free concurrency control over hashing operations to improve scalability. And this repo is the implementation of “NRHI - Non-Rehashing Hash Index for Persistent Memory”, in Proceedings of the 38th IEEE International Conference on Computer Design, (ICCD 2021).

## Get Started
NRHI is a header-only library, and is implemented based on [libpmemobj-cpp](https://pmem.io/pmdk/).

Basically,`git clone https://github.com/lionelee/NRHI.git --recursive` is all you need to get started. NRHI provides test files with YCSB benchmarks under [tests/](tests/).

### Build
**Preliminaries**
- Configure Intel Persistent Memory under *App Direct Mode*.  [This document](https://software.intel.com/content/www/us/en/develop/articles/qsg-intro-to-provisioning-pmem.html) can be helpful.
- Build `libpmemobj-cpp`:  get into [that directory](libpmemobj-cpp/) and follow the instructions.

**Build test binaries**

```bash
$ mkdir build
$ cd build
$ cmake ..
$ make -j
```

### Run Benchmark

Generate the workloads by running YCSB:
```bash
$ ./scripts/gen_workloads.sh
```

Then run the benchmarks:
```bash
$ ./scripts/run.sh
```

## License

NRHI is licensed under [SATA-License](https://github.com/lionelee/NRHI/blob/master/LICENSE) (The Star And Thank Author License, [original link](https://github.com/zTrix/sata-license)).
The basic idea is, whenever using a project using SATA license, people should star/like/+1 that project and thank the author.
