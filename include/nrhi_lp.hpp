// SPDX-License-Identifier: BSD-3-Clause
/* Copyright (c) 2020, Xinyu Li */

#ifndef PMEMOBJ_NRHI_HPP
#define PMEMOBJ_NRHI_HPP

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/persistent_pool_ptr.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

#include <atomic>
#include <cassert>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>

#include "compound_pool_ptr.hpp"

#if _MSC_VER
#include <intrin.h>
#include <windows.h>
#endif

#define CAS(ptr, oldval, newval)                                               \
	(__sync_bool_compare_and_swap(ptr, oldval, newval))
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define ALIGNED(N) __attribute__((aligned(N)))
#define CACHE_LINE_SIZE 64

// #define DEBUG 1
#define EXPO 1
#define LP_DIS_B 4
#define LP_DIS_S 4

namespace std
{
/**
 * Specialization of std::hash for p<T>
 */
template <typename T>
struct hash<pmem::obj::p<T>> {
	size_t
	operator()(const pmem::obj::p<T> &x) const
	{
		return hash<T>()(x.get_ro());
	}
};
} /* namespace std */

namespace pmem
{
namespace obj
{
namespace nrhi
{

using namespace pmem::obj;

template <typename Hash>
using transparent_key_equal = typename Hash::transparent_key_equal;

template <typename Hash>
using has_transparent_key_equal = detail::supports<Hash, transparent_key_equal>;

template <typename Hash, typename Pred,
	  bool = has_transparent_key_equal<Hash>::value>
struct key_equal_type {
	using type = typename Hash::transparent_key_equal;
};

template <typename Hash, typename Pred>
struct key_equal_type<Hash, Pred, false> {
	using type = Pred;
};

template <typename T, typename U, typename... Args>
void
make_persistent_object(pool_base &pop, persistent_ptr<U> &ptr, Args &&...args)
{
#if USE_ATOMIC_ALLOCATOR
	make_persistent_atomic<T>(pop, ptr, std::forward<Args>(args)...);
#else
	transaction::manual tx(pop);
	ptr = make_persistent<T>(std::forward<Args>(args)...);
	transaction::commit();
#endif
}

template <typename Key, typename T, typename Hash = std::hash<Key>,
	  typename KeyEqual = std::equal_to<Key>>
class NRHI {
public:
	using key_type = Key;
	using value_type = std::pair<const Key, T>;
	using size_type = size_t;
	using pointer = value_type *;
	using const_pointer = const value_type *;
	using reference = value_type &;
	using const_reference = const value_type &;

	using hashcode_t = uint64_t;
	using partial_t = uint16_t;
	using hasher = Hash;
	using key_equal = typename key_equal_type<Hash, KeyEqual>::type;
	using kv_ptr_t = detail::compound_pool_ptr<value_type>;

	static const size_type hashcode_size = sizeof(uint64_t) * 8;
	static const size_type slots_num = 8;
	static const size_type partial_shift = sizeof(partial_t) * 8;
	static const size_type token_shift =
		(sizeof(hashcode_t) - sizeof(partial_t)) * 8;
	static const size_type partial_mask = 0xFFFF000000000000;

	class accessor {
		friend class NRHI<Key, T, Hash, KeyEqual>;
		kv_ptr_t kv_p;
		uint64_t pool_uuid;

		void
		set(uint64_t r_pool_uuid, kv_ptr_t r_kv_p)
		{
			pool_uuid = r_pool_uuid;
			kv_p = r_kv_p;
		}

	public:
		bool
		empty() const
		{
			return !kv_p;
		}

		reference
		operator*()
		{
			return *operator->();
		}

		pointer
		operator->()
		{
			assert(kv_p);
			return kv_p.get_address(pool_uuid);
		}

		accessor() : kv_p(OID_NULL)
		{
		}

		~accessor()
		{
			kv_p.off = 0;
		}
	};

	union kv_ptr_u {
		kv_ptr_t p;
		struct {
			char padding[6];
			partial_t token;
		};

		kv_ptr_u() : p(nullptr)
		{
		}

		kv_ptr_u(uint64_t _off, partial_t _token)
		    : p(_off), token(_token)
		{
		}
	};

	struct ALIGNED(CACHE_LINE_SIZE) bucket {
		kv_ptr_u slots[slots_num];
	};
	using buckets_ptr_t = detail::compound_pool_ptr<bucket[]>;

	struct segment {
		buckets_ptr_t buckets;
	};
	using segments_ptr_t = persistent_ptr<segment[]>;

	struct directory;
	using directory_ptr_t = detail::compound_pool_ptr<directory>;

	struct directory {
		p<size_type> segs_power;
		segments_ptr_t segments;
		directory_ptr_t prev;
		directory_ptr_t next;
	};

	/* Explicit specialization of the converting constructor. */
	explicit NRHI(size_type hashpower = 10, size_type segspower = 3)
	{
		assert(hashpower > 0);

		PMEMoid oid = pmemobj_oid(this);
		assert(!OID_IS_NULL(oid));
		my_pool_uuid.get_rw() = oid.pool_uuid_lo;
		bucket_size.get_rw() = 1UL << hashpower;

		pool_base pop = get_pool_base();
		transaction::run(pop, [&] {
			persistent_ptr<directory> tmp_dir =
				make_persistent<directory>();
			tmp_dir->segs_power.get_rw() = segspower;
			tmp_dir->prev = nullptr;
			tmp_dir->next = nullptr;
			size_type segs_num = 1UL << segspower;
			tmp_dir->segments =
				make_persistent<segment[]>(segs_num);

			for (ptrdiff_t i = 0; i < (ptrdiff_t)(segs_num); i++) {
				persistent_ptr<bucket[]> tmp_buckets =
					make_persistent<bucket[]>(bucket_size);
				tmp_dir->segments[i].buckets.off =
					tmp_buckets.raw().off;
				pop.persist(&(tmp_dir->segments[i].buckets.off),
					    sizeof(uint64_t));
			}

			root_dir.off = tmp_dir.raw().off;
			top_dir.off = tmp_dir.raw().off;
		});
		dirs.push_back(root_dir.get_address(my_pool_uuid));
	}

	NRHI &operator=(const NRHI &table) = delete;
	NRHI &operator=(NRHI &&table) = delete;

	~NRHI()
	{
		std::cout << "nrhi destroy!" << std::endl;
	}

	void
	recover()
	{
		directory_ptr_t dp = root_dir;
		while (dp != nullptr) {
			directory *layer = dp.get_address(my_pool_uuid);
			top_dir = dp;
			dp = layer->next;
		}
	}

	static void
	allocate_kv_copy_construct(pool_base &pop,
				   persistent_ptr<value_type> &kv_ptr,
				   const void *param)
	{
		const value_type *v = static_cast<const value_type *>(param);
		make_persistent_object<value_type>(pop, kv_ptr, *v);
	}

	static void
	allocate_kv_move_construct(pool_base &pop,
				   persistent_ptr<value_type> &kv_ptr,
				   const void *param)
	{
		const value_type *v = static_cast<const value_type *>(param);
		make_persistent_object<value_type>(
			pop, kv_ptr, std::move(*const_cast<value_type *>(v)));
	}

	//------------------------------------------------------------------------
	// NRHI operations
	//------------------------------------------------------------------------

	/**
	 * Find item with corresponding key
	 * @return true if item is found, false otherwise.
	 */
	bool
	find(const Key &key, accessor &res)
	{
		return generic_find(key, &res);
	}

	bool
	find(const Key &key)
	{
		return generic_find(key, nullptr);
	}

	/**
	 * Find item with corresponding key
	 *
	 * This overload only participates in overload resolution if the
	 * qualified-id Hash::transparent_key_equal is valid and denotes a type.
	 * This assumes that such Hash is callable with both K and Key type, and
	 * that its key_equal is transparent, which, together, allows calling
	 * this function without constructing an instance of Key
	 *
	 * @return true if item is found, false otherwise.
	 */
	template <typename K,
		  typename = typename std::enable_if<
			  has_transparent_key_equal<hasher>::value, K>::type>
	bool
	find(const K &key, accessor &res)
	{
		return generic_find(key, &res);
	}

	/**
	 * Insert item (if not already present)
	 * @return true if item is new.
	 * @throw std::bad_alloc on allocation failure.
	 */
	bool
	insert(const value_type &value)
	{
		return generic_insert(value.first, &value,
				      allocate_kv_copy_construct, nullptr);
	}

	bool
	insert(const value_type &value, accessor &res)
	{
		return generic_insert(value.first, &value,
				      allocate_kv_copy_construct, &res);
	}

	bool
	insert(value_type &&value)
	{
		return generic_insert(value.first, &value,
				      allocate_kv_move_construct, nullptr);
	}

	bool
	insert(value_type &&value, accessor &res)
	{
		return generic_insert(value.first, &value,
				      allocate_kv_move_construct, &res);
	}

	/**
	 * Update item (if already present)
	 * @return true if item is new.
	 * @throw std::bad_alloc on allocation failure.
	 * @throw std::runtime_error in case of PMDK unable to free the memory.
	 */
	bool
	update(const value_type &value)
	{
		return generic_update(value.first, &value,
				      allocate_kv_copy_construct, nullptr);
	}

	bool
	update(const value_type &value, accessor &res)
	{
		return generic_update(value.first, &value,
				      allocate_kv_copy_construct, &res);
	}

	bool
	update(value_type &&value)
	{
		return generic_update(value.first, &value,
				      allocate_kv_move_construct, nullptr);
	}

	bool
	update(value_type &&value, accessor &res)
	{
		return generic_update(value.first, &value,
				      allocate_kv_move_construct, &res);
	}
	/**
	 * Remove item with corresponding key
	 * @return true if item was deleted by this call.
	 * @throw std::runtime_error in case of PMDK unable to free the memory.
	 */
	bool
	erase(const Key &key)
	{
		return generic_erase(key);
	}

	/**
	 * Remove item with corresponding key
	 *
	 * This overload only participates in overload resolution if the
	 * qualified-id Hash::transparent_key_equal is valid and denotes a type.
	 * This assumes that such Hash is callable with both K and Key type, and
	 * that its key_equal is transparent, which, together, allows calling
	 * this function without constructing an instance of Key
	 *
	 * @return true if item was deleted by this call.
	 * @throw std::runtime_error in case of PMDK unable to free the memory.
	 */
	template <typename K,
		  typename = typename std::enable_if<
			  has_transparent_key_equal<hasher>::value, K>::type>
	bool
	erase(const K &key)
	{
		return generic_erase(key);
	}

	/**
	 * Get current capacity
	 */
	uint64_t
	capacity()
	{
		std::unordered_map<bucket *, bool> umap;
		directory_ptr_t dp = root_dir;
		uint64_t effective_segs_num = 0;
		while (dp != nullptr) {
			directory *layer = dp.get_address(my_pool_uuid);
			size_type segs_num = 1UL << layer->segs_power.get_ro();

			for (ptrdiff_t i = 0; i < (ptrdiff_t)(segs_num); i++) {
				segment &seg = layer->segments[i];
				if (seg.buckets.off != 0) {
					umap[seg.buckets.get_address(
						my_pool_uuid)] = true;
					effective_segs_num++;
				}
			}
			dp = layer->next;
		}

		uint64_t cap = effective_segs_num * bucket_size * slots_num;

#ifdef DEBUG_CAPACITY
		uint64_t items = 0;
		for (auto it = umap.begin(); it != umap.end(); it++) {
			int sc = 0;
			for (size_type j = 0; j < bucket_size; j++) {
				bucket &b = it->first[j];
				int c = 0;
				for (size_type m = 0; m < slots_num; m++) {
					if (b.slots[m].p.get_offset() != 0 &&
					    b.slots[m].token != 0) {
						items++;
						c++;
					}
				}
				std::cout << "bucket " << j << " cap: " << c
					  << std::endl;
				sc += c;
			}
			std::cout << "segment: " << (it->first)
				  << " cap: " << sc << std::endl
				  << std::endl;
		}

		std::cout << "items:" << items << "\tcap:" << cap << '\t'
			  << items / (double)cap << std::endl;
#endif

		return cap;
	}

protected:
	bool
	expand(pool_base &pop, directory_ptr_t &dp, ptrdiff_t &segment_idx,
	       bool is_null)
	{
		directory *layer = dp.get_address(my_pool_uuid);
		if (likely(is_null)) { /* allocate a segment w/o resizing dir */
		EXPAND_SEG:
			segment &seg = layer->segments[segment_idx];
			uint64_t tmp_off = seg.buckets.off;
			if (unlikely(tmp_off != 0))
				return true; /* expanded by other thread */

			persistent_ptr<bucket[]> new_buckets;
			make_persistent_atomic<bucket[]>(pop, new_buckets,
							 bucket_size);

			if (CAS(&(seg.buckets.off), tmp_off,
				new_buckets.raw().off)) {
				pop.persist(&(seg.buckets.off),
					    sizeof(uint64_t));
#ifdef DEBUG
				std::cout << "[SUCC] expand segment "
					  << segment_idx << std::endl;
#endif
			} else {
				/* failed means it was updated by others */
				delete_persistent_atomic<bucket[]>(new_buckets,
								   bucket_size);
#ifdef DEBUG
				std::cout << "[FAIL] expand segment "
					  << segment_idx << std::endl;
#endif
			}

			return true;
		} else { /* expand directory */
			size_type segs_num =
				1UL << (layer->segs_power.get_ro() + EXPO);
			uint64_t tmp_off = layer->next.off;

			if (unlikely(tmp_off != 0))
				return true; /* expanded by other thread */

			persistent_ptr<directory> new_layer;
			make_persistent_atomic<directory>(pop, new_layer);
			new_layer->segs_power.get_rw() =
				layer->segs_power.get_ro() + EXPO;
			pop.persist(new_layer->segs_power);
			new_layer->next = nullptr;
			pop.persist(&(new_layer->next.off), sizeof(uint64_t));
			new_layer->prev.off = dp.off;
			pop.persist(&(new_layer->prev.off), sizeof(uint64_t));
			make_persistent_atomic<segment[]>(
				pop, new_layer->segments, segs_num);
			pop.persist(new_layer->segments);

			bool succ = false;
			if (CAS(&(layer->next.off), tmp_off,
				new_layer.raw().off)) {
				pop.persist(&(layer->next.off),
					    sizeof(uint64_t));
				std::cout << "expand new layer with cap "
					  << segs_num << std::endl;
				succ = true;
			} else {
				delete_persistent_atomic<segment[]>(
					new_layer->segments, segs_num);
				delete_persistent_atomic<directory>(new_layer);

				std::cout << "another thread is expanding"
					  << std::endl;
			}
			dp = layer->next;
			top_dir = dp;
			layer = dp.get_address(my_pool_uuid);
			if (succ)
				dirs.push_back(layer);
			goto EXPAND_SEG;
		}

		return false;
	}

	template <typename K>
	bool
	generic_find(const K &key, accessor *res)
	{
		/* not implemented */
		return false;
	}

	template <typename K>
	bool
	generic_erase(const K &key)
	{
		/* not implemented */
		return false;
	}

	bool generic_insert(const key_type &key, const void *param,
			    void (*allocate_kv)(pool_base &,
						persistent_ptr<value_type> &,
						const void *),
			    accessor *res);

	bool
	generic_update(const key_type &key, const void *param,
		       void (*allocate_kv)(pool_base &,
					   persistent_ptr<value_type> &,
					   const void *),
		       accessor *res)
	{
		/* not implemented */
		return false;
	}

	/**
	 * Get the persistent memory pool where hashmap
	 * resides.
	 * @returns pmem::obj::pool_base object.
	 */
	pool_base
	get_pool_base()
	{
		PMEMobjpool *pop =
			pmemobj_pool_by_oid(PMEMoid{my_pool_uuid, 0});

		return pool_base(pop);
	}

private:
	/* ID of persistent memory pool where hash map resides. */
	p<uint64_t> my_pool_uuid;

	/* size of bucket in segment */
	p<size_type> bucket_size;

	/* directory of hash table */
	directory_ptr_t root_dir, top_dir;

	/* access frequence*/
	std::vector<directory *> dirs;

}; /* End of class NRHI */

template <typename Key, typename T, typename Hash, typename KeyEqual>
bool
NRHI<Key, T, Hash, KeyEqual>::generic_insert(
	const key_type &key, const void *param,
	void (*allocate_kv)(pool_base &, persistent_ptr<value_type> &,
			    const void *),
	accessor *res)
{
	hashcode_t h = hasher{}(key);
	pool_base pop = get_pool_base();

	partial_t token = (partial_t)(h >> partial_shift);
	size_t bucket_idx1 = (h & (bucket_size - 1));

	size_type slot_idx = 0;

	while (true) {
		directory_ptr_t dp = root_dir;
		directory_ptr_t effective_dp = nullptr;
		directory_ptr_t insert_dp = nullptr;

		ptrdiff_t segment_idx = -1;
		ptrdiff_t insert_segment_idx = -1;
		ptrdiff_t insert_bucket_idx = (ptrdiff_t)bucket_idx1;

		bool found_empty = false;

		while (dp != nullptr) {
			effective_dp = dp;
			directory *layer = dp.get_address(my_pool_uuid);
			size_t segs_size = 1UL << layer->segs_power.get_ro();
			size_type segment_idx1 =
				h >>
				(hashcode_size - layer->segs_power.get_ro());

			for (size_type s = 0; s < LP_DIS_S; s++) {
				segment_idx = (ptrdiff_t)((segment_idx1 + s) %
							  segs_size);
				segment &seg = layer->segments[segment_idx];
				if (seg.buckets.get_offset() == 0)
					goto OUT;

				for (size_type k = 0; k < LP_DIS_B; k++) {
					ptrdiff_t bucket_idx =
						(ptrdiff_t)((bucket_idx1 + k) %
							    bucket_size);
					bucket &b = seg.buckets.get_address(
						my_pool_uuid)[bucket_idx];

					for (size_type i = 0; i < slots_num;
					     i++) {
						if (!found_empty &&
						    b.slots[i].p.off == 0) {
							insert_dp = dp;
							insert_segment_idx =
								segment_idx;
							insert_bucket_idx =
								bucket_idx;
							slot_idx = i;
							found_empty = true;
						} else if (
							b.slots[i].p.off != 0 &&
							b.slots[i].token ==
								token &&
							key_equal{}(
								b.slots[i]
									.p
									.get_address(
										my_pool_uuid)
									->first,
								key)) {
							if (res)
								res->set(
									my_pool_uuid,
									b.slots[i]
										.p);
#ifdef DEBUG
							std::cout
								<< "hashcode 0x"
								<< std::hex << h
								<< std::dec
								<< " found"
								<< std::endl;
#endif

							return true;
						}
					}
				}
			}

			dp = layer->next;
		}
	OUT:

		if (likely(found_empty)) {
		FAST_INSERT:
			persistent_ptr<value_type> newkv_ptr;
			allocate_kv(pop, newkv_ptr, param);
			uint64_t newcont =
				(((uint64_t)token) << token_shift) ^
				(newkv_ptr.raw().off & (~partial_mask));

			segment &seg = insert_dp.get_address(my_pool_uuid)
					       ->segments[insert_segment_idx];
			bucket &b = seg.buckets.get_address(
				my_pool_uuid)[insert_bucket_idx];
			uint64_t tmp_off = b.slots[slot_idx].p.off;

#ifdef DEBUG
			std::cout << "insert hashcode 0x" << std::hex << h
				  << std::dec << " to segment "
				  << insert_segment_idx << " to bucket "
				  << bucket_idx << std::endl;
#endif

			if (CAS(&(b.slots[slot_idx].p.off), tmp_off, newcont)) {
				pop.persist(&(b.slots[slot_idx].p.off),
					    sizeof(uint64_t));
				if (res)
					res->set(my_pool_uuid,
						 b.slots[slot_idx].p);
				return true;
			} else {
				pmemobj_free(newkv_ptr.raw_ptr());
			}
		} else {
			bool is_null = (dp != nullptr);
			insert_segment_idx =
				(is_null)
					? segment_idx
					: (ptrdiff_t)(h >>
						      (hashcode_size -
						       effective_dp
							       .get_address(
								       my_pool_uuid)
							       ->segs_power
							       .get_ro() -
						       1));
			insert_dp = effective_dp;
			if (expand(pop, insert_dp, insert_segment_idx,
				   is_null)) {
				goto FAST_INSERT;
			} else {
				std::cout << "expand failed" << std::endl;
				return false;
			}
		}
	}

	return false;
}

} /* namespace nrhi */
} /* namespace obj */
} /* namespace pmem */

#endif /* PMEMOBJ_NRHI_HPP */
