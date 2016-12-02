/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_CORE_CONFIGURATION_H_
#define HPCJOIN_CORE_CONFIGURATION_H_

#include <stdint.h>

namespace hpcjoin {
namespace core {

class Configuration {

public:

	static const uint32_t RESULT_AGGREGATION_NODE = 0;

	static const uint32_t CACHELINE_SIZE_BYTES = 64;
	static const uint32_t CACHELINES_PER_MEMORY_BUFFER = 1024;
	static const uint32_t MEMORY_BUFFERS_PER_PARTITION = 2;

	static const uint64_t MEMORY_BUFFER_SIZE_BYTES = CACHELINES_PER_MEMORY_BUFFER * CACHELINE_SIZE_BYTES;
	static const uint64_t MEMORY_PARTITION_SIZE_BYTES = MEMORY_BUFFERS_PER_PARTITION * MEMORY_BUFFER_SIZE_BYTES;

	static const bool ENABLE_TWO_LEVEL_PARTITIONING = true;

	static const uint64_t NETWORK_PARTITIONING_FANOUT = 10;
	static const uint64_t LOCAL_PARTITIONING_FANOUT = 10;

	static const uint64_t NETWORK_PARTITIONING_COUNT = (1 << NETWORK_PARTITIONING_FANOUT);
	static const uint64_t LOCAL_PARTITIONING_COUNT = (1 << LOCAL_PARTITIONING_FANOUT);

	static constexpr double ALLOCATION_FACTOR = 1.1;

	static const uint32_t PAYLOAD_BITS = 27;

};

} /* namespace core */
} /* namespace hpcjoin */

#endif /* HPCJOIN_CORE_CONFIGURATION_H_ */
