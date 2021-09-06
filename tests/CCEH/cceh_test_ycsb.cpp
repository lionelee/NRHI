// SPDX-License-Identifier: BSD-3-Clause
/* Copyright (c) 2020, Xinyu Li */

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "cceh.hpp"
#include "common.hpp"

#define LAYOUT "cceh"
#define KEYLEN 16
#define VALUELEN 16
#define LATENCY_ENABLE 1

// #define LOADFACTOR_TEST 1

#ifdef MACRO_TEST
// 1024*2^12=4194304
#define INITIAL_DEPTH 12U
#define OPERATION_NUM 64000000
#else
// 1024*2^10=1048576
#define INITIAL_DEPTH 10
#define OPERATION_NUM 16000000
#endif

namespace nvobj = pmem::obj;

namespace
{
using pair_t = std::pair<OP, uint8_t *>;

using persistent_map_type = nvobj::experimental::CCEH;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

struct thread_stat {
	uint64_t inserted;
	uint64_t ins_fail;
	uint64_t found;
	uint64_t fnd_fail;
	uint64_t updated;
	uint64_t upd_fail;
	uint64_t deleted;
	uint64_t del_fail;
	std::vector<pair_t> items;
	std::vector<int64_t> latency;
};

using namespace std::chrono;
}

int
main(int argc, char *argv[])
{
	// parse inputs
	if (argc != 5) {
		printf("usage: %s <pool_file> <load_file> <run_file> <thread_num>\n",
		       argv[0]);
		printf("  <pool_file>: the pool file for kv store\n");
		printf("  <load_file>: the wordload file for load phase\n");
		printf("  <run_file>: the workload file for run phase\n");
		printf("  <thread_num>: the number of threads\n");
		exit(1);
	}

	const char *path = argv[1];
	int tmp = atoi(argv[4]);
	assert(tmp > 0);
	size_t thread_num = static_cast<size_t>(tmp);

	nvobj::pool<root> pop;
	remove(path); // delete the mapped file.

	if (file_exists(path)) {
		pop = nvobj::pool<root>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL * 20480, CREATE_MODE_RW);

		nvobj::transaction::run(pop, [&] {
			pop.root()->cons =
				nvobj::make_persistent<persistent_map_type>(
					INITIAL_DEPTH);
			;
		});
	}

	std::ifstream ifs_load(argv[2]), ifs_run(argv[3]);
	if (!ifs_load.is_open()) {
		printf("Failed to open %s.\n", argv[2]);
		exit(1);
	}
	if (!ifs_run.is_open()) {
		printf("Failed to open %s.\n", argv[3]);
		exit(1);
	}

#ifdef LOADFACTOR_TEST
	std::ofstream ofs_loadfactor("cceh_loadfactor.res");
	if (!ofs_loadfactor.is_open()) {
		printf("Failed to write loadfactor file\n");
		exit(1);
	}
#endif

	std::string opstr, keystr;
	auto map = pop.root()->cons;
	uint8_t key[KEYLEN];
	size_t loaded = 0;
	size_t total_load = 0;

	printf("initial capacity %ld\n", map->capacity());

#ifndef LOAD_TEST
	printf("Load phase starts.\n");
	while (ifs_load >> opstr) {
		OP op = parse_ycsb_op(opstr.c_str());
		if (op == OP::PUT) {
			total_load++;
			ifs_load >> keystr;
			memcpy(key, keystr.c_str() + 4, KEYLEN - 1);
			auto ret = map->insert(key, key, KEYLEN, VALUELEN, 0);
			if (ret.found) {
				loaded++;
#ifdef LOADFACTOR_TEST
				if (loaded % 20000 == 0)
					ofs_loadfactor << (loaded * 1.0 /
							   map->capacity())
						       << std::endl;
#endif
			} else {
				std::cout << "load " << keystr << " failed"
					  << std::endl;
				continue;
			}
		}
		getline(ifs_load, keystr);
	}

	ifs_load.close();
	printf("Load phase finished: %ld/%ld inserted\n", loaded, total_load);

#ifdef LOADFACTOR_TEST
	ofs_loadfactor.close();
	exit(0);
#endif

#endif

	thread_stat thread_queue[thread_num];
	for (size_t i = 0; i < thread_num; i++) {
		thread_queue[i].items.reserve(OPERATION_NUM / thread_num);
		thread_queue[i].inserted = thread_queue[i].ins_fail = 0;
		thread_queue[i].found = thread_queue[i].fnd_fail = 0;
		thread_queue[i].updated = thread_queue[i].upd_fail = 0;
		thread_queue[i].deleted = thread_queue[i].del_fail = 0;
	}

	size_t op_total = 0;
	printf("Run phase starts.\n");
	while (ifs_run >> opstr) {
		OP op = parse_ycsb_op(opstr.c_str());
		if (op > OP::DELETE)
			continue;
		ifs_run >> keystr;
		uint8_t *key = (uint8_t *)calloc(KEYLEN, sizeof(uint8_t));
		memcpy(key, keystr.c_str() + 4, KEYLEN - 1);

		thread_queue[op_total % thread_num].items.push_back(
			std::make_pair(op, key));
		op_total++;
		getline(ifs_run, keystr);
	}
	ifs_run.close();

	std::vector<std::thread> threads;
	size_t op_cnt = op_total / thread_num;
	auto start = high_resolution_clock::now();
	for (size_t i = 0; i < thread_num; i++) {
		threads.emplace_back(
			[&](size_t tid) {
				size_t offset = loaded + OPERATION_NUM /
								 thread_num *
								 tid;
				for (size_t j = 0; j < op_cnt; j++) {
#ifdef LATENCY_ENABLE
					auto req_start =
						high_resolution_clock::now();
#endif
					pair_t &item =
						thread_queue[tid].items[j];
					if (item.first == OP::PUT) {
						auto ret = map->insert(
							item.second,
							item.second, KEYLEN,
							VALUELEN, j);
						if (ret.found)
							thread_queue[tid]
								.inserted++;
						else
							thread_queue[tid]
								.ins_fail++;
					} else if (item.first == OP::GET) {
						auto ret = map->get(item.second,
								    KEYLEN);
						if (ret.found)
							thread_queue[tid]
								.found++;
						else
							thread_queue[tid]
								.fnd_fail++;
					} else if (item.first == OP::UPDATE ||
						   item.first == OP::DELETE) {
						continue;
					} else {
						break;
					}
#ifdef LATENCY_ENABLE
					auto req_end =
						high_resolution_clock::now();
					thread_queue[tid].latency.push_back(
						(req_end - req_start).count());
#endif
				}
			},
			i);
	}

	for (auto &t : threads)
		t.join();

	auto end = high_resolution_clock::now();
	auto elapsed = (end - start).count() / 1000000000.0;
	auto throughput = op_total / elapsed;
	printf("Run phase finished in %f seconds\n", elapsed);
	printf("%f reqs per second (%ld threads)\n", throughput, thread_num);
	std::ofstream ofs_throughput("cceh_throughput.res");
	ofs_throughput << throughput << std::endl;
	ofs_throughput.close();

	uint64_t inserted = 0, ins_fail = 0, found = 0, fnd_fail = 0;
	uint64_t updated = 0, upd_fail = 0, deleted = 0, del_fail = 0;
	for (auto &t : thread_queue) {
		inserted += t.inserted;
		ins_fail += t.ins_fail;
		found += t.found;
		fnd_fail += t.fnd_fail;
		updated += t.updated;
		upd_fail += t.upd_fail;
		deleted += t.deleted;
		del_fail += t.del_fail;
	}

	uint64_t total_slots = map->capacity();
	printf("capacity (after insertion) %ld, load factor %f\n", total_slots,
	       (loaded + inserted) * 1.0 / total_slots);

	printf("Insert operations: %ld loaded, %ld inserted, %ld failed\n",
	       loaded, inserted, ins_fail);
	printf("Read operations:   %ld found, %ld failed\n", found, fnd_fail);
	printf("Delete operations: %ld deleted, %ld failed\n", deleted,
	       del_fail);
	printf("Update operations: %ld updated, %ld failed\n", updated,
	       upd_fail);

#ifdef LATENCY_ENABLE
	std::ofstream ofs_latency("cceh_latency.res");
	if (!ofs_latency.is_open()) {
		printf("Failed to write latency file\n");
		exit(1);
	}

	int64_t total_latency = 0;
	for (auto &t : thread_queue) {
		for (auto &l : t.latency) {
			ofs_latency << l << std::endl;
			total_latency += l;
		}
	}
	ofs_latency.close();

	auto avg_latency = total_latency * 1.0 / op_total;
	printf("Average latency: %f (ns)\n", avg_latency);
#endif

	return 0;
}
