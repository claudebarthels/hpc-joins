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

	static const uint32_t SORT_RUN_ELEMENT_COUNT = (16384);

	static constexpr double ALLOCATION_FACTOR = 1.5;

	static const uint32_t PAYLOAD_BITS = 27;

	static const uint32_t MAX_MERGE_FAN_IN = 16;

};

} /* namespace core */
} /* namespace hpcjoin */

#endif /* HPCJOIN_CORE_CONFIGURATION_H_ */
