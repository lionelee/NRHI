// SPDX-License-Identifier: BSD-3-Clause
/* Copyright (c) 2020, Xinyu Li */

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <cstdio>
#include <iostream>
#include <string>
#include <unistd.h>
#include <vector>

#include "common.hpp"
#include "nrhi.hpp"

#define LAYOUT "NRHI"

namespace nvobj = pmem::obj;

namespace
{
using persistent_map_type = nvobj::nrhi::NRHI<nvobj::p<int>, nvobj::p<int>>;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

void
put_item(nvobj::pool<root> &pop)
{
	auto map = pop.root()->cons;
	assert(map != nullptr);

	int key;
	std::string keystr;
	std::cin >> keystr;
	try {
		key = stoi(keystr);
	} catch (...) {
		printf("%s is not a valid integer\n", keystr.c_str());
		return;
	}

	persistent_map_type::accessor r;
	bool ret = map->insert(persistent_map_type::value_type(key, key), r);

	if (ret) {
		printf("[SUCCESS] inserted %d : %d\n", (int)r->first,
		       (int)r->second);
	} else {
		printf("[FAIL] can not insert %d\n", key);
	}
}

void
get_item(nvobj::pool<root> &pop)
{
	auto map = pop.root()->cons;
	assert(map != nullptr);

	int key;
	std::string keystr;
	std::cin >> keystr;
	try {
		key = stoi(keystr);
	} catch (...) {
		printf("%s is not a valid integer\n", keystr.c_str());
		return;
	}

	persistent_map_type::accessor r;
	bool ret = map->find(persistent_map_type::key_type(key), r);

	if (ret) {
		printf("[SUCCESS] found %d : %d\n", (int)r->first,
		       (int)r->second);
	} else {
		printf("[FAIL] can not find %d\n", key);
	}
}

void
free_item(nvobj::pool<root> &pop)
{
	auto map = pop.root()->cons;
	assert(map != nullptr);

	int key;
	std::string keystr;
	std::cin >> keystr;
	try {
		key = stoi(keystr);
	} catch (...) {
		printf("%s is not a valid integer\n", keystr.c_str());
		return;
	}

	bool ret = map->erase(persistent_map_type::key_type(key));

	if (ret) {
		printf("[SUCCESS] deleted %d\n", key);
	} else {
		printf("[FAIL] can not delete %d\n", key);
	}
}

void
print_help()
{
	printf("command format:\n");
	printf("  <op> [<key>]\n");
	printf("  while <op> can be put/get/free/help/quit, <key> must be an integer\n");
}

}

int
main(int argc, char *argv[])
{
	// parse inputs
	if (argc != 2) {
		printf("usage: %s <pool_file_path>\n", argv[0]);
		exit(1);
	}

	const char *path = argv[1];
	nvobj::pool<root> pop;

	if (file_exists(path)) {
		pop = nvobj::pool<root>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL * 20, CREATE_MODE_RW);

		nvobj::transaction::run(pop, [&] {
			pop.root()->cons =
				nvobj::make_persistent<persistent_map_type>();
		});
	} else {
		pop = nvobj::pool<root>::open(path, LAYOUT);
	}

	print_help();
	std::string opstr;

	while (true) {
		std::cout << ">>> " << std::flush;
		std::cin >> opstr;
		OP op = parse_cli_op(opstr.c_str());

		switch (op) {
			case OP::PUT:
				put_item(pop);
				break;
			case OP::GET:
				get_item(pop);
				break;
			case OP::DELETE:
				free_item(pop);
				break;
			case OP::HELP:
				print_help();
				break;
			case OP::QUIT:
				goto quit;
			default:
				printf("unknown operation\n");
		}
	}

quit:
	pop.close();
	return 0;
}