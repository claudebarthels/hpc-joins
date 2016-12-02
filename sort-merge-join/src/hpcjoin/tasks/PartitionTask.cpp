/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "PartitionTask.h"

#include <immintrin.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <math.h>

#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/core/Configuration.h>

#define CACHELINE_SIZE (64)
#define TUPLES_PER_CACHELINE (CACHELINE_SIZE/sizeof(hpcjoin::data::CompressedTuple))
#define ALIGN_TO_CACHELINE(N) ((N+TUPLES_PER_CACHELINE-1) & ~(TUPLES_PER_CACHELINE-1))
#define HASH_BIT_MODULO(KEY, MASK, NBITS) (((KEY) & (MASK)) >> (NBITS))

namespace hpcjoin {
namespace tasks {

typedef union cacheline {
	struct {
		hpcjoin::data::CompressedTuple tuples[TUPLES_PER_CACHELINE];
	} tuples;
	struct {
		hpcjoin::data::CompressedTuple tuples[TUPLES_PER_CACHELINE - 1];
		uint64_t slot;
	} data;
} cacheline_t;

PartitionTask::PartitionTask(hpcjoin::data::Relation* innerRelation, hpcjoin::data::Relation* outerRelation, uint32_t numberOfNodes) {

	this->innerRelation = innerRelation;
	this->outerRelation = outerRelation;
	this->numberOfNodes = numberOfNodes;
	this->innerHistogram = NULL;
	this->outerHistogram = NULL;
	this->innerWindowSize = 0;
	this->outerWindowSize = 0;
	this->innerWriteOffsets = NULL;
	this->outerWriteOffsets = NULL;
	this->innerIncomingData = NULL;
	this->outerIncomingData = NULL;
	this->innerLocalWriteOffsets = NULL;
	this->outerLocalWriteOffsets = NULL;
	this->innerPartitionOutput = NULL;
	this->outerPartitionOutput = NULL;

}

void PartitionTask::execute() {

	/**
	 * Compute inter-node histograms
	 */

	hpcjoin::performance::Measurements::startLocalHistogram();
	this->innerHistogram = computeHistogram(this->innerRelation, this->numberOfNodes);
	hpcjoin::performance::Measurements::stopLocalHistogram(this->innerRelation->getLocalSize());

	hpcjoin::performance::Measurements::startLocalHistogram();
	this->outerHistogram = computeHistogram(this->outerRelation, this->numberOfNodes);
	hpcjoin::performance::Measurements::stopLocalHistogram(this->outerRelation->getLocalSize());

	hpcjoin::performance::Measurements::startWindowPreparationComputation();
	this->innerWindowSize = computeWindowSize(this->innerHistogram, this->numberOfNodes);
	this->outerWindowSize = computeWindowSize(this->outerHistogram, this->numberOfNodes);
	this->innerWriteOffsets = computeWriteOffsets(this->innerHistogram, this->numberOfNodes);
	this->outerWriteOffsets = computeWriteOffsets(this->outerHistogram, this->numberOfNodes);
	this->innerIncomingData = computeIncomingData(this->innerHistogram, this->numberOfNodes);
	this->outerIncomingData = computeIncomingData(this->outerHistogram, this->numberOfNodes);
	hpcjoin::performance::Measurements::stopWindowPreparationComputation();

	/**
	 * Compute intra-node write offsets
	 */

	this->innerLocalWriteOffsets = computeLocalWriteOffsets(this->innerHistogram, this->numberOfNodes);
	this->outerLocalWriteOffsets = computeLocalWriteOffsets(this->outerHistogram, this->numberOfNodes);

	uint64_t innerOutputSize = (this->innerRelation->getLocalSize() * sizeof(hpcjoin::data::CompressedTuple)) + (numberOfNodes * CACHELINE_SIZE);
	int32_t returnValue = posix_memalign((void **) &(this->innerPartitionOutput), CACHELINE_SIZE, innerOutputSize);
	JOIN_ASSERT(returnValue == 0, "PartitionTask", "Could not allocate memory");

	uint64_t outerOutputSize = (this->outerRelation->getLocalSize() * sizeof(hpcjoin::data::CompressedTuple)) + (numberOfNodes * CACHELINE_SIZE);
	returnValue = posix_memalign((void **) &(this->outerPartitionOutput), CACHELINE_SIZE, outerOutputSize);
	JOIN_ASSERT(returnValue == 0, "PartitionTask", "Could not allocate memory");

	hpcjoin::performance::Measurements::startPartitioningElements();
	partitionData(this->innerRelation, this->innerPartitionOutput, this->innerLocalWriteOffsets, this->numberOfNodes);
	hpcjoin::performance::Measurements::stopPartitioningElements(innerRelation->getLocalSize());

	hpcjoin::performance::Measurements::startPartitioningElements();
	partitionData(this->outerRelation, this->outerPartitionOutput, this->outerLocalWriteOffsets, this->numberOfNodes);
	hpcjoin::performance::Measurements::stopPartitioningElements(outerRelation->getLocalSize());

}

PartitionTask::~PartitionTask() {
	free(this->innerPartitionOutput);
	free(this->outerPartitionOutput);
	delete[] this->innerHistogram;
	delete[] this->outerHistogram;
	delete[] this->innerWriteOffsets;
	delete[] this->outerWriteOffsets;
}

uint64_t* PartitionTask::computeHistogram(hpcjoin::data::Relation* relation, uint32_t numberOfNodes) {
	uint64_t* result = new uint64_t[numberOfNodes];
	memset(result, 0, numberOfNodes * sizeof(uint64_t));

	const uint64_t numberOfElements = relation->getLocalSize();
	const hpcjoin::data::Tuple *data = relation->getData();
	const uint32_t mask = numberOfNodes - 1;

	for (uint64_t i = 0; i < numberOfElements; ++i) {
		uint32_t idx = HASH_BIT_MODULO(data[i].key, mask, 0);
		++(result[idx]);
	}

	return result;
}

uint64_t PartitionTask::computeWindowSize(uint64_t* histogram, uint32_t numberOfNodes) {
	uint64_t result = 0;
	MPI_Reduce_scatter_block(histogram, &result, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
	return result + numberOfNodes * sizeof(hpcjoin::data::CompressedTuple); // Worst case: every node has un odd number of tuples and padding is required
}

uint64_t* PartitionTask::computeWriteOffsets(uint64_t* histogram, uint32_t numberOfNodes) {
	uint64_t* result = new uint64_t[numberOfNodes];
	memset(result, 0, numberOfNodes * sizeof(uint64_t));

	uint64_t* alignedHistogram = new uint64_t[numberOfNodes];
	memcpy(alignedHistogram, histogram, numberOfNodes*sizeof(uint64_t));
	for(uint32_t i=0; i<numberOfNodes; ++i) {
		if(alignedHistogram[i] % 2 != 0) {
			++(alignedHistogram[i]);
		}
	}

	MPI_Scan(alignedHistogram, result, numberOfNodes, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
	for (uint32_t i = 0; i < numberOfNodes; ++i) {
		result[i] -= alignedHistogram[i];
	}

	return result;
}

uint64_t* PartitionTask::computeIncomingData(uint64_t* histogram, uint32_t numberOfNodes) {
	uint64_t* result = new uint64_t[numberOfNodes];
	MPI_Alltoall(histogram, 1, MPI_UINT64_T, result, 1, MPI_UINT64_T, MPI_COMM_WORLD);
	return result;
}

uint64_t* PartitionTask::computeLocalWriteOffsets(uint64_t* histogram, uint32_t numberOfNodes) {
	uint64_t* result = new uint64_t[numberOfNodes];
	result[0] = 0;
	for (uint32_t i = 1; i < numberOfNodes; ++i) {
		result[i] = result[i - 1] + ALIGN_TO_CACHELINE(histogram[i - 1]);
	}
	return result;
}

void PartitionTask::partitionData(hpcjoin::data::Relation* relation, hpcjoin::data::CompressedTuple* outputBuffer, uint64_t* localWriteOffsets, uint32_t numberOfNodes) {

	const uint64_t numberOfElements = relation->getLocalSize();
	const hpcjoin::data::Tuple *input = relation->getData();
	const uint32_t mask = numberOfNodes - 1;
	const uint32_t nodeBits = log2(numberOfNodes);

	cacheline_t *buffer = NULL;
	int32_t returnValue = posix_memalign((void**) &(buffer), CACHELINE_SIZE, numberOfNodes * sizeof(cacheline_t));
	JOIN_ASSERT(returnValue == 0, "PartitionTask", "Could not allocate memory");

	for (uint32_t i = 0; i < numberOfNodes; ++i) {
		buffer[i].data.slot = localWriteOffsets[i];
	}

	for (uint64_t i = 0; i < numberOfElements; ++i) {
		uint32_t idx = HASH_BIT_MODULO(input[i].key, mask, 0);
		JOIN_ASSERT(idx == (input[i].key % numberOfNodes), "PartitioningTask", "Key %lu assigned to partition %d", input[i].key, idx);

		uint32_t slot = buffer[idx].data.slot;
		hpcjoin::data::CompressedTuple *cacheline = (hpcjoin::data::CompressedTuple *) (buffer + idx);
		uint32_t slotMod = (slot) & (TUPLES_PER_CACHELINE - 1);

		//cacheline[slotMod] = input[i];
		cacheline[slotMod].value = input[i].rid + ((input[i].key >> nodeBits) << (nodeBits + hpcjoin::core::Configuration::PAYLOAD_BITS));

		if (slotMod == (TUPLES_PER_CACHELINE - 1)) {
			store((outputBuffer + slot - (TUPLES_PER_CACHELINE - 1)), cacheline);
		}

		buffer[idx].data.slot = slot + 1;
	}

	for (uint32_t i = 0; i < numberOfNodes; ++i) {
		uint32_t slot = buffer[i].data.slot;
		uint32_t num = (slot) & (TUPLES_PER_CACHELINE - 1);
		if (num > 0) {
			hpcjoin::data::CompressedTuple *dest = outputBuffer + slot - num;
			for (uint32_t j = 0; j < num; ++j) {
				//dest[j] = buffer[i].data.tuples[j];
				dest[j].value = buffer[i].data.tuples[j].value;
			}
		}
	}

}

void PartitionTask::store(void* to, void* from) {

	JOIN_ASSERT(to != NULL, "Local Partitioning", "Stream destination should not be NULL");
	JOIN_ASSERT(from != NULL, "Local Partitioning", "Stream source should not be NULL");

	JOIN_ASSERT(((uint64_t) to) % CACHELINE_SIZE == 0, "Local Partitioning", "Stream destination not aligned");
	JOIN_ASSERT(((uint64_t) from) % CACHELINE_SIZE == 0, "Local Partitioning", "Stream source not aligned");

	register __m256i * d1 = (__m256i *) to;
	register __m256i s1 = *((__m256i *) from);
	register __m256i * d2 = d1 + 1;
	register __m256i s2 = *(((__m256i *) from) + 1);

	_mm256_stream_si256(d1, s1);
	_mm256_stream_si256(d2, s2);

}

} /* namespace tasks */
} /* namespace hpcjoin */

