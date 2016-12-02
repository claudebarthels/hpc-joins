/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "BuildProbe.h"

#include <stdlib.h>

#include <hpcjoin/operators/HashJoin.h>
#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>

#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)

#define HASH_BIT_MODULO(KEY, MASK, NBITS) (((KEY) & (MASK)) >> (NBITS))

namespace hpcjoin {
namespace tasks {

BuildProbe::BuildProbe(uint64_t innerPartitionSize, hpcjoin::data::CompressedTuple *innerPartition, uint64_t outerPartitionSize, hpcjoin::data::CompressedTuple *outerPartition) {

	this->innerPartitionSize = innerPartitionSize;
	this->innerPartition = innerPartition;

	this->outerPartitionSize = outerPartitionSize;
	this->outerPartition = outerPartition;

}

BuildProbe::~BuildProbe() {

}

void BuildProbe::execute() {

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::startBuildProbeTask();
#endif

	JOIN_DEBUG("Build-Probe", "Executing build-probe phase of size %lu x %lu", innerPartitionSize, outerPartitionSize);

	uint32_t const keyShift = hpcjoin::core::Configuration::NETWORK_PARTITIONING_FANOUT + hpcjoin::core::Configuration::PAYLOAD_BITS;
	uint32_t const shiftBits =  keyShift + hpcjoin::core::Configuration::LOCAL_PARTITIONING_FANOUT;


	uint64_t N = this->innerPartitionSize;
	NEXT_POW_2(N);
	uint64_t const MASK = (N-1) << (shiftBits);

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::startBuildProbeMemoryAllocation();
#endif

	uint64_t *hashTableNext = (uint64_t*) calloc(this->innerPartitionSize, sizeof(uint64_t));
	uint64_t *hashTableBucket = (uint64_t*) calloc(N, sizeof(uint64_t));

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::stopBuildProbeMemoryAllocation(this->innerPartitionSize);
#endif


	// Build hash table

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::startBuildProbeBuild();
#endif

	for (uint64_t t=0; t<this->innerPartitionSize;) {
		uint64_t idx = HASH_BIT_MODULO(innerPartition[t].value, MASK, shiftBits);
		hashTableNext[t] = hashTableBucket[idx];
		hashTableBucket[idx]  = ++t;
	}

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::stopBuildProbeBuild(this->innerPartitionSize);
#endif

	// Probe hash table

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::startBuildProbeProbe();
#endif

	uint64_t matches = 0;
	for (uint64_t t=0; t<this->outerPartitionSize; ++t) {
		uint64_t idx = HASH_BIT_MODULO(outerPartition[t].value, MASK, shiftBits);
		for(uint64_t hit = hashTableBucket[idx]; hit > 0; hit = hashTableNext[hit-1]){
			if((outerPartition[t].value >> keyShift) == (innerPartition[hit-1].value >> keyShift)){
				++matches;
			}
		}
	}

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::stopBuildProbeProbe(this->outerPartitionSize);
#endif

	free(hashTableNext);
	free(hashTableBucket);

	hpcjoin::operators::HashJoin::RESULT_COUNTER += matches;

#ifdef MEASUREMENT_DETAILS_LOCALBP
	hpcjoin::performance::Measurements::stopBuildProbeTask();
#endif

}

task_type_t BuildProbe::getType() {
	return TASK_BUILD_PROBE;
}

} /* namespace tasks */
} /* namespace hpcjoin */


