/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "NetworkPartitioning.h"

#include <immintrin.h>
#include <stdlib.h>
#include <string.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/data/CompressedTuple.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>

#define NETWORK_PARTITIONING_CACHELINE_SIZE (64)
#define TUPLES_PER_CACHELINE (NETWORK_PARTITIONING_CACHELINE_SIZE / sizeof(hpcjoin::data::CompressedTuple))

#define HASH_BIT_MODULO(KEY, MASK, NBITS) (((KEY) & (MASK)) >> (NBITS))

#define PARTITION_ACCESS(p) (((char *) inMemoryBuffer) + (p * hpcjoin::core::Configuration::MEMORY_PARTITION_SIZE_BYTES))

namespace hpcjoin {
namespace tasks {

typedef union {

	struct {
		hpcjoin::data::CompressedTuple tuples[TUPLES_PER_CACHELINE];
	} tuples;

	struct {
		hpcjoin::data::CompressedTuple tuples[TUPLES_PER_CACHELINE - 1];
		uint32_t inCacheCounter;
		uint32_t memoryCounter;
	} data;

} cacheline_t;

NetworkPartitioning::NetworkPartitioning(uint32_t nodeId, hpcjoin::data::Relation* innerRelation, hpcjoin::data::Relation* outerRelation, hpcjoin::data::Window* innerWindow,
		hpcjoin::data::Window* outerWindow) {

	this->nodeId = nodeId;

	this->innerRelation = innerRelation;
	this->outerRelation = outerRelation;

	this->innerWindow = innerWindow;
	this->outerWindow = outerWindow;

	JOIN_ASSERT(hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES == NETWORK_PARTITIONING_CACHELINE_SIZE, "Network Partitioning", "Cache line sizes do not match. This is a hack and the value needs to be edited in two places.");

}

NetworkPartitioning::~NetworkPartitioning() {
}

void NetworkPartitioning::execute() {

	JOIN_DEBUG("Network Partitioning", "Node %d is partitioning inner relation", this->nodeId);
	partition(innerRelation, innerWindow);

	JOIN_DEBUG("Network Partitioning", "Node %d is partitioning outer relation", this->nodeId);
	partition(outerRelation, outerWindow);

}

void NetworkPartitioning::partition(hpcjoin::data::Relation *relation, hpcjoin::data::Window *window) {

	window->start();

	uint64_t const numberOfElements = relation->getLocalSize();
	hpcjoin::data::Tuple * const data = relation->getData();

	// Create in-memory buffer
	uint64_t const bufferedPartitionCount = hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT;
	uint64_t const bufferedPartitionSize = hpcjoin::core::Configuration::MEMORY_PARTITION_SIZE_BYTES;
	uint64_t const inMemoryBufferSize = bufferedPartitionCount * bufferedPartitionSize;

	const uint32_t partitionBits = hpcjoin::core::Configuration::NETWORK_PARTITIONING_FANOUT;

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::startNetworkPartitioningMemoryAllocation();
#endif

	hpcjoin::data::CompressedTuple * inMemoryBuffer = NULL;
	int result = posix_memalign((void **) &(inMemoryBuffer), NETWORK_PARTITIONING_CACHELINE_SIZE, inMemoryBufferSize);

	JOIN_ASSERT(result == 0, "Network Partitioning", "Could not allocate in-memory buffer");
	memset(inMemoryBuffer, 0, inMemoryBufferSize);

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::stopNetworkPartitioningMemoryAllocation(inMemoryBufferSize);
#endif

	// Create in-cache buffer
	cacheline_t inCacheBuffer[hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT] __attribute__((aligned(NETWORK_PARTITIONING_CACHELINE_SIZE)));;

	JOIN_DEBUG("Network Partitioning", "Node %d is setting counter to zero", this->nodeId);
	for (uint32_t p = 0; p < hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++p) {
		inCacheBuffer[p].data.inCacheCounter = 0;
		inCacheBuffer[p].data.memoryCounter = 0;
	}

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::startNetworkPartitioningMainPartitioning();
#endif

	for (uint64_t i = 0; i < numberOfElements; ++i) {

		// Compute partition
		uint32_t partitionId = HASH_BIT_MODULO(data[i].key, hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT - 1, 0);

		// Save counter to register
		uint32_t inCacheCounter = inCacheBuffer[partitionId].data.inCacheCounter;
		uint32_t memoryCounter = inCacheBuffer[partitionId].data.memoryCounter;

		// Move data to cache line
		hpcjoin::data::CompressedTuple *cacheLine = (hpcjoin::data::CompressedTuple *) (inCacheBuffer + partitionId);
		//cacheLine[inCacheCounter] = data[i];
		cacheLine[inCacheCounter].value = data[i].rid + ((data[i].key >> partitionBits) << (partitionBits + hpcjoin::core::Configuration::PAYLOAD_BITS));
		++inCacheCounter;

		// Check if cache line is full
		if (inCacheCounter == TUPLES_PER_CACHELINE) {

			//JOIN_DEBUG("Network Partitioning", "Node %d has a full cache line %d", this->nodeId, partitionId);

			// Move cache line to memory buffer
			char *inMemoryStreamDestination = PARTITION_ACCESS(partitionId) + (memoryCounter * NETWORK_PARTITIONING_CACHELINE_SIZE);
			streamWrite(inMemoryStreamDestination, cacheLine);
			++memoryCounter;

			//JOIN_DEBUG("Network Partitioning", "Node %d has completed the stream write of cache line %d", this->nodeId, partitionId);

			// Check if memory buffer is full
			if (memoryCounter % hpcjoin::core::Configuration::CACHELINES_PER_MEMORY_BUFFER == 0) {

				bool rewindBuffer = (memoryCounter == hpcjoin::core::Configuration::MEMORY_BUFFERS_PER_PARTITION * hpcjoin::core::Configuration::CACHELINES_PER_MEMORY_BUFFER);

				//JOIN_DEBUG("Network Partitioning", "Node %d has a full memory buffer %d", this->nodeId, partitionId);
				hpcjoin::data::CompressedTuple *inMemoryBufferLocation = reinterpret_cast<hpcjoin::data::CompressedTuple *>(PARTITION_ACCESS(partitionId) + (memoryCounter * NETWORK_PARTITIONING_CACHELINE_SIZE) - (hpcjoin::core::Configuration::MEMORY_BUFFER_SIZE_BYTES));
				window->write(partitionId, inMemoryBufferLocation, hpcjoin::core::Configuration::CACHELINES_PER_MEMORY_BUFFER * TUPLES_PER_CACHELINE, rewindBuffer);

				if(rewindBuffer) {
					memoryCounter = 0;
				}

				//JOIN_DEBUG("Network Partitioning", "Node %d has completed the put operation of memory buffer %d", this->nodeId, partitionId);
			}

			inCacheCounter = 0;
		}

		inCacheBuffer[partitionId].data.inCacheCounter = inCacheCounter;
		inCacheBuffer[partitionId].data.memoryCounter = memoryCounter;

	}

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::stopNetworkPartitioningMainPartitioning(numberOfElements);
#endif

	JOIN_DEBUG("Network Partitioning", "Node %d is flushing remaining tuples", this->nodeId);

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::startNetworkPartitioningFlushPartitioning();
#endif

	// Flush remaining elements to memory buffers
	for(uint32_t p=0; p<hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++p) {

		uint32_t inCacheCounter = inCacheBuffer[p].data.inCacheCounter;
		uint32_t memoryCounter = inCacheBuffer[p].data.memoryCounter;

		hpcjoin::data::CompressedTuple *cacheLine = (hpcjoin::data::CompressedTuple *) (inCacheBuffer + p);
		hpcjoin::data::CompressedTuple *inMemoryFreeSpace = reinterpret_cast<hpcjoin::data::CompressedTuple *>(PARTITION_ACCESS(p) + (memoryCounter * NETWORK_PARTITIONING_CACHELINE_SIZE));
		for(uint32_t t=0; t<inCacheCounter; ++t) {
				inMemoryFreeSpace[t] = cacheLine[t];
		}

		uint32_t remainingTupleInMemory = ((memoryCounter % hpcjoin::core::Configuration::CACHELINES_PER_MEMORY_BUFFER) * TUPLES_PER_CACHELINE) + inCacheCounter;

		if(remainingTupleInMemory > 0) {
			hpcjoin::data::CompressedTuple *inMemoryBufferOfPartition = reinterpret_cast<hpcjoin::data::CompressedTuple *>(PARTITION_ACCESS(p) + (memoryCounter/hpcjoin::core::Configuration::CACHELINES_PER_MEMORY_BUFFER) * hpcjoin::core::Configuration::MEMORY_BUFFER_SIZE_BYTES);
			window->write(p, inMemoryBufferOfPartition, remainingTupleInMemory, false);
		}

	}

	window->flush();
	window->stop();

	free(inMemoryBuffer);

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::stopNetworkPartitioningFlushPartitioning();
#endif

	window->assertAllTuplesWritten();

}

inline void NetworkPartitioning::streamWrite(void* to, void* from) {

	JOIN_ASSERT(to != NULL, "Network Partitioning", "Stream destination should not be NULL");
	JOIN_ASSERT(from != NULL, "Network Partitioning", "Stream source should not be NULL");

	JOIN_ASSERT(((uint64_t) to) % NETWORK_PARTITIONING_CACHELINE_SIZE == 0, "Network Partitioning", "Stream destination not aligned");
	JOIN_ASSERT(((uint64_t) from) % NETWORK_PARTITIONING_CACHELINE_SIZE == 0, "Network Partitioning", "Stream source not aligned");

	register __m256i * d1 = (__m256i *) to;
	register __m256i s1 = *((__m256i *) from);
	register __m256i * d2 = d1 + 1;
	register __m256i s2 = *(((__m256i *) from) + 1);

	_mm256_stream_si256(d1, s1);
	_mm256_stream_si256(d2, s2);

	/*
    register __m128i * d1 = (__m128i*) to;
    register __m128i * d2 = d1+1;
    register __m128i * d3 = d1+2;
    register __m128i * d4 = d1+3;
    register __m128i s1 = *(__m128i*) from;
    register __m128i s2 = *((__m128i*)from + 1);
    register __m128i s3 = *((__m128i*)from + 2);
    register __m128i s4 = *((__m128i*)from + 3);

    _mm_stream_si128 (d1, s1);
    _mm_stream_si128 (d2, s2);
    _mm_stream_si128 (d3, s3);
    _mm_stream_si128 (d4, s4);
    */

}

task_type_t NetworkPartitioning::getType() {
	return TASK_NET_PARTITION;
}

} /* namespace tasks */
} /* namespace hpcjoin */

