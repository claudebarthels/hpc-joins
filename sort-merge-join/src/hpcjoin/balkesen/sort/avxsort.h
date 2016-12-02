/**
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * (c) 2014, ETH Zurich, Systems Group
 *
 */

#ifndef AVXSORT_H
#define AVXSORT_H

#include <stdint.h>

typedef struct {
	uint32_t key;
	uint32_t rid;
} tuple_t;

/**
 * @defgroup sorting Sorting routines
 * @{
 */


/**
 * Sorts given array of items using AVX instructions. If the input is aligned
 * to cache-line, then aligned version of the implementation is executed.
 *
 * @note output array must be pre-allocated before the call.
 *
 * @param inputptr
 * @param outputptr
 * @param nitems
 */
void
avxsort_int64(int64_t ** inputptr, int64_t ** outputptr, uint64_t nitems);

/**
 * \copydoc avxsort_int64
 * @note currently not implemented.
 */
void
avxsort_int32(int32_t ** inputptr, int32_t ** outputptr, uint64_t nitems);

/**
 * Sorts given array of tuples on "key" field using AVX instructions. If the
 * input is aligned to cache-line, then aligned version of the implementation
 * is executed.
 *
 * @note output array must be pre-allocated before the call.
 *
 * @param inputptr
 * @param outputptr
 * @param nitems
 */
void
avxsort_tuples(tuple_t ** inputptr, tuple_t ** outputptr, uint64_t nitems);

/** @} */

#endif /* AVXSORT_H */
