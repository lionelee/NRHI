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

#include "common.hpp"
#include "level_hash.hpp"
#include "polymorphic_string.hpp"

#define LAYOUT "level_hash"
#define KEYLEN 15
#define LATENCY_ENABLE 1

#ifdef MACRO_TEST
// (2^15 + 2^14) * 4 = 196608
#define HASH_POWER 15
#define OPERATION_NUM 64000000
#else
// (2^13 + 2^12) * 4 = 49152
#define HASH_POWER 13
#define OPERATION_NUM 16000000
#endif

namespace nvobj = pmem::obj;

namespace
{
using string_t = polymorphic_string;
using pair_t = std::pair<OP, string_t>;

class key_equal {
public:
	template <typename M, typename U>
	bool
	operator()(const M &lhs, const U &rhs) const
	{
		return lhs == rhs;
	}
};

class string_hasher {
	/* hash multiplier used by fibonacci hashing */
	static const size_t hash_multiplier = 11400714819323198485ULL;

public:
	using transparent_key_equal = key_equal;

	size_t
	operator()(const polymorphic_string &str) const
	{
		return hash(str.c_str(), str.size());
	}

private:
	size_t
	hash(const char *str, size_t size) const
	{
		size_t h = 0;
		for (size_t i = 0; i < size; ++i) {
			h = static_cast<size_t>(str[i]) ^ (h * hash_multiplier);
		}
		return h;
	}
};

using persistent_map_type =
	nvobj::experimental::level_hash<string_t, string_t, string_hasher,
					std::equal_to<string_t>>;

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
					(uint64_t)HASH_POWER, 1);
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

	std::string opstr, keystr;
	auto map = pop.root()->cons;
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
			string_t key(keystr.c_str() + 4, KEYLEN);
			auto ret = map->insert(
				persistent_map_type::value_type(key, key),
				(size_t)0);
			if (!ret.found) {
				loaded++;
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
		string_t key(keystr.c_str() + 4, KEYLEN);

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
				for (size_t j = 0; j < op_cnt; j++) {
#ifdef LATENCY_ENABLE
					auto req_start =
						high_resolution_clock::now();
#endif
					pair_t &item =
						thread_queue[tid].items[j];
					if (item.first == OP::PUT) {
						auto ret = map->insert(
							persistent_map_type::value_type(
								item.second,
								item.second),
							tid);
						if (!ret.found)
							thread_queue[tid]
								.inserted++;
						else
							thread_queue[tid]
								.ins_fail++;
					} else if (item.first == OP::GET) {
						auto ret = map->query(
							persistent_map_type::key_type(
								item.second),
							tid);
						if (ret.found)
							thread_queue[tid]
								.found++;
						else
							thread_queue[tid]
								.fnd_fail++;
					} else if (item.first == OP::UPDATE) {
						string_t new_val = item.second;
						new_val[0] = ~new_val[0];
						auto ret = map->update(
							persistent_map_type::
								value_type(
									item.second,
									new_val),
							tid);
						if (ret.found)
							thread_queue[tid]
								.updated++;
						else
							thread_queue[tid]
								.upd_fail++;
					} else if (item.first == OP::DELETE) {
						auto ret = map->erase(
							persistent_map_type::key_type(
								item.second),
							tid);
						if (ret.found)
							thread_queue[tid]
								.deleted++;
						else
							thread_queue[tid]
								.del_fail++;
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
	std::ofstream ofs_throughput("level_throughput.res");
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
	std::ofstream ofs_latency("level_latency.res");
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
