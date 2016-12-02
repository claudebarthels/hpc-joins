/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#include "Pool.h"

#include <stdlib.h>
#include <string.h>

#include <hpcjoin/utils/Debug.h>

namespace hpcjoin {
namespace memory {

uint64_t Pool::dataSize;
void * Pool::data;
uint64_t Pool::remainingSize;
void * Pool::nextFreeData;
uint64_t Pool::lowerAddressBound;
uint64_t Pool::upperAddressBound;

void Pool::allocate(uint64_t size) {

	int result = posix_memalign((void **) &(data), 64, size);
	JOIN_ASSERT(result == 0, "Pool", "Could not allocate memory");
	memset(data, 0, size);

	dataSize = size;
	remainingSize = size;
	nextFreeData = data;

	lowerAddressBound = (uint64_t) data;
	upperAddressBound = lowerAddressBound + size;

}

void* Pool::getMemory(uint64_t size) {

	void *memory = NULL;

	if (remainingSize >= size) {
		memory = nextFreeData;
		uint64_t aligned64Size = 0;
		if (((size >> 6) << 6) == size) {
			aligned64Size = size;
		} else {
			aligned64Size = (size + 64) & (~0x3F);
		}
		JOIN_ASSERT(aligned64Size % 64 == 0, "Pool", "Size not aligned to 64")
		nextFreeData = (void*) (((uint64_t) nextFreeData) + aligned64Size);
		remainingSize -= aligned64Size;
	} else {
		JOIN_DEBUG("Pool", "Out of memory");
		int result = posix_memalign((void **) &(memory), 64, size);
		JOIN_ASSERT(result == 0, "Pool", "Could not allocate memory");
	}

	JOIN_ASSERT(((uint64_t ) memory) % 64 == 0, "Pool", "Returned memory not aligned to 64")
	return memory;

}

void Pool::free(void* memory) {
	if ((((uint64_t) memory) < lowerAddressBound) || (((uint64_t) memory) >= upperAddressBound)) {
		free(memory);
	}
}

void Pool::freeAll() {
	free(data);
}

void Pool::reset() {
	remainingSize = dataSize;
	nextFreeData = data;
}

} /* namespace memory */
} /* namespace hpcjoin */

