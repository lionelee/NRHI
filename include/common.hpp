// SPDX-License-Identifier: BSD-3-Clause
/* Copyright (c) 2020, Xinyu Li */

#ifndef COMMON_HPP
#define COMMON_HPP

#include <cstdint>

enum class OP { PUT, GET, UPDATE, DELETE, HELP, QUIT, UNKNOWN };

inline OP
parse_cli_op(const char *op)
{
	if (!strcmp(op, "put")) {
		return OP::PUT;
	} else if (!strcmp(op, "get")) {
		return OP::GET;
	} else if (!strcmp(op, "free")) {
		return OP::DELETE;
	} else if (!strcmp(op, "help")) {
		return OP::HELP;
	} else if (!strcmp(op, "quit")) {
		return OP::QUIT;
	} else {
		return OP::UNKNOWN;
	}
}

inline OP
parse_ycsb_op(const char *op)
{
	if (!strcmp(op, "INSERT")) {
		return OP::PUT;
	} else if (!strcmp(op, "READ")) {
		return OP::GET;
	} else if (!strcmp(op, "UPDATE")) {
		return OP::UPDATE;
	} else if (!strcmp(op, "DELETE")) {
		return OP::DELETE;
	} else if (!strcmp(op, "help")) {
		return OP::HELP;
	} else if (!strcmp(op, "quit")) {
		return OP::QUIT;
	} else {
		return OP::UNKNOWN;
	}
}

#ifndef _WIN32

#include <unistd.h>

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

static inline int
file_exists(char const *path)
{
	return access(path, 0);
}

#else

#include <corecrt_io.h>
#include <process.h>
#include <windwos.h>

#define CREATE_MODE_RW (S_IWRITE | S_IREAD)

static inline int
file_exists(char const *path)
{
	return _access(path, 0);
}

#endif

#endif /* COMMON_HPP */
