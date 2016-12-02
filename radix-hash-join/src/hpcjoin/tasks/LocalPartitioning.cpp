/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "LocalPartitioning.h"

#include <immintrin.h>
#include <stdlib.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/operators/HashJoin.h>
#include <hpcjoin/tasks/BuildProbe.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/memory/Pool.h>

#define LOCAL_PARTITIONING_CACHELINE_SIZE (64)
#define TUPLES_PER_CACHELINE (LOCAL_PARTITIONING_CACHELINE_SIZE / sizeof(hpcjoin::data::CompressedTuple))

#define HASH_BIT_MODULO(KEY, MASK, NBITS) (((KEY) & (MASK)) >> (NBITS))

typedef union {
    struct {
    	hpcjoin::data::CompressedTuple tuples[TUPLES_PER_CACHELINE];
    } tuples;
    struct {
    	hpcjoin::data::CompressedTuple tuples[TUPLES_PER_CACHELINE - 1];
        uint64_t slot;
    } data;
} cacheline_t;

namespace hpcjoin {
namespace tasks {

LocalPartitioning::LocalPartitioning(uint64_t innerPartitionSize, hpcjoin::data::CompressedTuple *innerPartition, uint64_t outerPartitionSize, hpcjoin::data::CompressedTuple *outerPartition) {

	this->innerPartitionSize = innerPartitionSize;
	this->innerPartition = innerPartition;

	this->outerPartitionSize = outerPartitionSize;
	this->outerPartition = outerPartition;

	JOIN_ASSERT(hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES == LOCAL_PARTITIONING_CACHELINE_SIZE, "Local Partitioning",
			"Cache line sizes do not match. This is a hack and the value needs to be edited in two places.");

}

LocalPartitioning::~LocalPartitioning() {

}

void LocalPartitioning::execute() {

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::startLocalPartitioningTask();
#endif

	uint64_t *innerHistogram = computeHistogram(this->innerPartition, this->innerPartitionSize);
	uint64_t *outerHistogram = computeHistogram(this->outerPartition, this->outerPartitionSize);

	uint64_t *innerOffsets = computePrefixSum(innerHistogram);
	uint64_t *outerOffsets = computePrefixSum(outerHistogram);

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::startLocalPartitioningMemoryAllocation();
#endif

	hpcjoin::data::CompressedTuple *innerPartitions = NULL;
	uint64_t innerOutputSize = (innerPartitionSize + hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT * TUPLES_PER_CACHELINE) * sizeof(hpcjoin::data::CompressedTuple);
	//int result = posix_memalign((void **) &(innerPartitions), LOCAL_PARTITIONING_CACHELINE_SIZE, innerOutputSize);
	//JOIN_ASSERT(result == 0, "Local Partitioning", "Could not allocate output buffer for inner partition");
	innerPartitions = (hpcjoin::data::CompressedTuple *) hpcjoin::memory::Pool::getMemory(innerOutputSize);

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::stopLocalPartitioningMemoryAllocation(innerOutputSize);
#endif

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::startLocalPartitioningMemoryAllocation();
#endif

	hpcjoin::data::CompressedTuple *outerPartitions = NULL;
	uint64_t outerOutputSize = (outerPartitionSize + hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT * TUPLES_PER_CACHELINE) * sizeof(hpcjoin::data::CompressedTuple);
	//result = posix_memalign((void **) &(outerPartitions), LOCAL_PARTITIONING_CACHELINE_SIZE, outerOutputSize);
	//JOIN_ASSERT(result == 0, "Local Partitioning", "Could not allocate output buffer for outer partition");
	outerPartitions = (hpcjoin::data::CompressedTuple *) hpcjoin::memory::Pool::getMemory(outerOutputSize);

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::stopLocalPartitioningMemoryAllocation(outerOutputSize);
#endif

	JOIN_DEBUG("Local Partitioning", "Partitioning inner partition of size %lu", innerPartitionSize);
	partitionData(innerPartition, innerPartitionSize, innerPartitions, innerOffsets, innerHistogram);

	JOIN_DEBUG("Local Partitioning", "Partitioning outer partition of size %lu", outerPartitionSize);
	partitionData(outerPartition, outerPartitionSize, outerPartitions, outerOffsets, outerHistogram);

	// Add build-probe tasks to queue
	for(uint32_t p=0; p<hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT; ++p) {
		if(innerHistogram[p] > 0 && outerHistogram[p] > 0) {
			hpcjoin::operators::HashJoin::TASK_QUEUE.push(new hpcjoin::tasks::BuildProbe(innerHistogram[p], innerPartitions+innerOffsets[p], outerHistogram[p], outerPartitions+outerOffsets[p]));
		}
	}

	free(innerHistogram);
	free(outerHistogram);

	free(innerOffsets);
	free(outerOffsets);

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::stopLocalPartitioningTask();
#endif

}

uint64_t* LocalPartitioning::computeHistogram(hpcjoin::data::CompressedTuple* tuples, uint64_t size) {

	uint64_t *histogram = (uint64_t*) calloc(hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT, sizeof(uint64_t));

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::startLocalPartitioningHistogramComputation();
#endif

	uint64_t MASK = (hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT - 1) << (hpcjoin::core::Configuration::NETWORK_PARTITIONING_FANOUT + hpcjoin::core::Configuration::PAYLOAD_BITS);
	for (uint64_t t = 0; t < size; ++t) {
		uint64_t idx = HASH_BIT_MODULO(tuples[t].value, MASK, hpcjoin::core::Configuration::NETWORK_PARTITIONING_FANOUT+ hpcjoin::core::Configuration::PAYLOAD_BITS);
		++(histogram[idx]);
	}

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::stopLocalPartitioningHistogramComputation(size);
#endif

	return histogram;

}

uint64_t* LocalPartitioning::computePrefixSum(uint64_t* histogram) {

	uint64_t *prefix = (uint64_t*) calloc(hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT, sizeof(uint64_t));

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::startLocalPartitioningOffsetComputation();
#endif

	uint64_t sum = 0;
	for(uint32_t p=0; p<hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT; ++p) {
		prefix[p] = sum;
		sum += histogram[p];
		// Add padding to ensure that every partition is cache-line aligned
		if(sum % TUPLES_PER_CACHELINE != 0) {
			sum += TUPLES_PER_CACHELINE - (sum % TUPLES_PER_CACHELINE);
		}
		JOIN_ASSERT(prefix[p] % TUPLES_PER_CACHELINE == 0, "Local Partitioning", "Sub-partition %d is not aligned", p);
	}

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::stopLocalPartitioningOffsetComputation();
#endif

	return prefix;

}

void LocalPartitioning::partitionData(hpcjoin::data::CompressedTuple* input, uint64_t inputSize, hpcjoin::data::CompressedTuple* output, uint64_t* partitionOffsets, uint64_t* histogram) {

	cacheline_t inCacheBuffer[hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT] __attribute__((aligned(LOCAL_PARTITIONING_CACHELINE_SIZE)));

	for(uint64_t p=0; p<hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT; ++p) {
		inCacheBuffer[p].data.slot = partitionOffsets[p];
	}

	// Partition data
	uint64_t MASK = (hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT - 1) << (hpcjoin::core::Configuration::NETWORK_PARTITIONING_FANOUT + hpcjoin::core::Configuration::PAYLOAD_BITS);

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::startLocalPartitioningPartitioning();
#endif

	for (uint64_t t = 0; t < inputSize; ++t) {

		uint64_t partitionId = HASH_BIT_MODULO(input[t].value, MASK, hpcjoin::core::Configuration::NETWORK_PARTITIONING_FANOUT + hpcjoin::core::Configuration::PAYLOAD_BITS);
		uint64_t slot = inCacheBuffer[partitionId].data.slot;
		hpcjoin::data::CompressedTuple *cacheLine = (hpcjoin::data::CompressedTuple *) (inCacheBuffer + partitionId);
		uint32_t slotMod = (slot) & (TUPLES_PER_CACHELINE - 1);

		cacheLine[slotMod] = input[t];

		if(slotMod == (TUPLES_PER_CACHELINE-1)){
			streamWrite((output+slot-(TUPLES_PER_CACHELINE-1)), cacheLine);
		}

		inCacheBuffer[partitionId].data.slot = slot+1;
	}

	// Flush the remaining in-cache data
	for(uint64_t p=0; p<hpcjoin::core::Configuration::LOCAL_PARTITIONING_COUNT; ++p) {

		uint64_t slot = inCacheBuffer[p].data.slot;
		uint32_t remainingElements = (slot) & (TUPLES_PER_CACHELINE - 1);
		slot -= remainingElements;

		for(uint32_t t = 0; t < remainingElements; ++t) {
			output[slot + t]  = inCacheBuffer[p].data.tuples[t];
		}

	}

#ifdef MEASUREMENT_DETAILS_LOCALPART
	hpcjoin::performance::Measurements::stopLocalPartitioningPartitioning(inputSize);
#endif

}

void LocalPartitioning::streamWrite(void* to, void* from) {

	JOIN_ASSERT(to != NULL, "Local Partitioning", "Stream destination should not be NULL");
	JOIN_ASSERT(from != NULL, "Local Partitioning", "Stream source should not be NULL");

	JOIN_ASSERT(((uint64_t) to) % LOCAL_PARTITIONING_CACHELINE_SIZE == 0, "Local Partitioning", "Stream destination not aligned");
	JOIN_ASSERT(((uint64_t) from) % LOCAL_PARTITIONING_CACHELINE_SIZE == 0, "Local Partitioning", "Stream source not aligned");

	register __m256i * d1 = (__m256i *) to;
	register __m256i s1 = *((__m256i *) from);
	register __m256i * d2 = d1 + 1;
	register __m256i s2 = *(((__m256i *) from) + 1);

	_mm256_stream_si256(d1, s1);
	_mm256_stream_si256(d2, s2);

}

task_type_t LocalPartitioning::getType() {
	return TASK_PARTITION;
}


} /* namespace tasks */
} /* namespace hpcjoin */

