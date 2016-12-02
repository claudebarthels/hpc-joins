/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_MEMORY_POOL_H_
#define HPCJOIN_MEMORY_POOL_H_

#include <stdint.h>

namespace hpcjoin {
namespace memory {

class Pool {

public:

	static void allocate(uint64_t size);
	static void * getMemory(uint64_t size);
	static void free(void *memory);
	static void freeAll();
	static void reset();

protected:

	static uint64_t dataSize;
	static void * data;

	static uint64_t remainingSize;
	static void * nextFreeData;

	static uint64_t lowerAddressBound;
	static uint64_t upperAddressBound;

};

} /* namespace memory */
} /* namespace hpcjoin */

#endif /* HPCJOIN_MEMORY_POOL_H_ */
