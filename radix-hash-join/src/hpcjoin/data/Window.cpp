/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "Window.h"

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>

#include <unistd.h>

namespace hpcjoin {
namespace data {

Window::Window(uint32_t numberOfNodes, uint32_t nodeId, uint32_t* assignment, uint64_t* localHistogram, uint64_t* globalHistogram, uint64_t* baseOffsets, uint64_t* writeOffsets) {

	this->numberOfNodes = numberOfNodes;
	this->nodeId = nodeId;
	this->assignment = assignment;
	this->localHistogram = localHistogram;
	this->globalHistogram = globalHistogram;
	this->baseOffsets = baseOffsets;
	this->writeOffsets = writeOffsets;
	this->writeCounters = (uint64_t *) calloc(hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, sizeof(uint64_t));
	this->localWindowSize = computeLocalWindowSize();

	#ifdef USE_FOMPI
	this->window = (foMPI_Win *) calloc(1, sizeof(foMPI_Win));
	#else
	this->window = (MPI_Win *) calloc(1, sizeof(MPI_Win));
	#endif


	MPI_Alloc_mem(localWindowSize * sizeof(hpcjoin::data::CompressedTuple), MPI_INFO_NULL, &(this->data));
	#ifdef USE_FOMPI
	foMPI_Win_create(this->data, localWindowSize * sizeof(hpcjoin::data::CompressedTuple), 1, MPI_INFO_NULL, MPI_COMM_WORLD, window);
	#else
	MPI_Win_create(this->data, localWindowSize * sizeof(hpcjoin::data::CompressedTuple), 1, MPI_INFO_NULL, MPI_COMM_WORLD, window);
	#endif

	JOIN_DEBUG("Window", "Window is at address %p to %p", this->data, this->data + localWindowSize);

}

Window::~Window() {
	#ifdef USE_FOMPI
	foMPI_Win_free(window);
	#else
	MPI_Win_free(window);
	#endif
	MPI_Free_mem(data);

	free(this->writeCounters);
	free(this->window);

}

void Window::start() {

	JOIN_DEBUG("Window", "Starting window");
	#ifdef USE_FOMPI
	foMPI_Win_lock_all(0, *window);
	#else
	MPI_Win_lock_all(0, *window);
	#endif

}

void Window::stop() {
	JOIN_DEBUG("Window", "Stopping window");
	#ifdef USE_FOMPI
	foMPI_Win_unlock_all(*window);
	#else
	MPI_Win_unlock_all(*window);
	#endif

}

void Window::write(uint32_t partitionId, CompressedTuple* tuples, uint64_t sizeInTuples, bool flush) {

	//JOIN_DEBUG("Window", "Initializing write for partition %d of %lu tuples", partitionId, sizeInTuples);

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::startNetworkPartitioningWindowPut();
#endif

	uint32_t targetProcess = this->assignment[partitionId];
	uint64_t targetOffset = this->writeOffsets[partitionId] + this->writeCounters[partitionId];

	//JOIN_DEBUG("Window", "Target %d and offset %lu (%lu + %lu)", targetProcess, targetOffset, this->writeOffsets[partitionId], this->writeCounters[partitionId]);

	#ifdef JOIN_DEBUG_PRINT
	uint64_t remoteSize = computeWindowSize(targetProcess);
	#endif
	JOIN_ASSERT(targetOffset <= remoteSize, "Window", "Target offset is outside window range");
	JOIN_ASSERT(targetOffset + sizeInTuples <= remoteSize, "Window", "Target offset and size is outside window range");

	#ifdef USE_FOMPI
	foMPI_Put(tuples, sizeInTuples * sizeof(CompressedTuple), MPI_BYTE, targetProcess, targetOffset * sizeof(CompressedTuple), sizeInTuples * sizeof(CompressedTuple), MPI_BYTE, *window);
	#else
	MPI_Put(tuples, sizeInTuples * sizeof(CompressedTuple), MPI_BYTE, targetProcess, targetOffset * sizeof(CompressedTuple), sizeInTuples * sizeof(CompressedTuple), MPI_BYTE, *window);
	#endif

	this->writeCounters[partitionId] += sizeInTuples;
	//JOIN_DEBUG("Window", "Partition %d has now %lu tuples", partitionId, this->writeCounters[partitionId]);

	JOIN_ASSERT(this->writeCounters[partitionId] <= this->localHistogram[partitionId], "Window",
			"Node %d is writing to partition %d. Has %lu tuples declared, but write counter is now %lu.", this->nodeId, partitionId, this->localHistogram[partitionId],
			this->writeCounters[partitionId]);

#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::stopNetworkPartitioningWindowPut();
#endif

	if (flush) {
#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::startNetworkPartitioningWindowWait();
#endif
		#ifdef USE_FOMPI
		foMPI_Win_flush_local(targetProcess, *window);
		#else
		MPI_Win_flush_local(targetProcess, *window);
		#endif
#ifdef MEASUREMENT_DETAILS_NETWORK
	hpcjoin::performance::Measurements::stopNetworkPartitioningWindowWait();
#endif
	}

}

CompressedTuple* Window::getPartition(uint32_t partitionId) {

	JOIN_ASSERT(this->nodeId == this->assignment[partitionId], "Window", "Cannot access non-assigned partition");

	return data + this->baseOffsets[partitionId];

}

uint64_t Window::getPartitionSize(uint32_t partitionId) {

	JOIN_ASSERT(this->nodeId == this->assignment[partitionId], "Window", "Should not access size of non-assigned partition");

	return this->globalHistogram[partitionId];

}

uint64_t Window::computeLocalWindowSize() {

	return computeWindowSize(this->nodeId);

}

uint64_t Window::computeWindowSize(uint32_t nodeId) {

	uint64_t sum = 0;
	for (uint32_t p = 0; p < hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++p) {
		if (this->assignment[p] == nodeId) {
			sum += this->globalHistogram[p];
		}
	}
	return sum;

}

void Window::assertAllTuplesWritten() {

	for (uint32_t p = 0; p < hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++p) {
		JOIN_ASSERT(this->localHistogram[p] == this->writeCounters[p], "Window", "Not all tuples submitted to window. Partition %d. Local size %lu tuples. Write size %lu tuples.",
				p, this->localHistogram[p], this->writeCounters[p]);
	}

}

void Window::flush() {

	#ifdef USE_FOMPI
	foMPI_Win_flush_local_all(*window);
	#else
	MPI_Win_flush_local_all(*window);
	#endif

}


} /* namespace data */
} /* namespace hpcjoin */

