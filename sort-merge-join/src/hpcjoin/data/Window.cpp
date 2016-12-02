/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "Window.h"

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>

#include <string.h>

#define MIN(a,b) (((a)<(b))?(a):(b))

namespace hpcjoin {
namespace data {

Window::Window(uint32_t numberOfNodes, uint64_t sizeInElements, uint64_t* numberOfElementsFromNode, uint64_t* writeOffsets, hpcjoin::data::Relation *relation) {

	this->numberOfNodes = numberOfNodes;
	this->sizeInElements = sizeInElements;
	this->numberOfElementsFromNode = numberOfElementsFromNode;
	this->writeOffsets = writeOffsets;
	this->writeCounters = (uint64_t *) calloc(numberOfNodes, sizeof(uint64_t));

	/**
	 * Create window
	 */

#ifdef USE_FOMPI
	memset(&window, 0, sizeof(foMPI_Win));
#else
	memset(&window, 0, sizeof(MPI_Win));
#endif

	//MPI_Alloc_mem(sizeInElements * sizeof(hpcjoin::data::CompressedTuple), MPI_INFO_NULL, &(this->data));
	// HACK: reuse memory
	this->data = relation->getFirstHalfData();
	JOIN_ALWAYS_ASSERT(sizeInElements*sizeof(hpcjoin::data::CompressedTuple) <= relation->secondHalfStartInBytes, "Window", "Window will overlap with second half of relation buffer.");
	JOIN_ALWAYS_ASSERT(sizeInElements*sizeof(hpcjoin::data::CompressedTuple) <= relation->secondHalfSizeInBytes, "Window", "Second half of relation buffer is not big enough to hold data.");

#ifdef USE_FOMPI
	foMPI_Win_create(this->data, sizeInElements * sizeof(hpcjoin::data::CompressedTuple), 1, MPI_INFO_NULL, MPI_COMM_WORLD, &window);
#else
	MPI_Win_create(this->data, sizeInElements * sizeof(hpcjoin::data::CompressedTuple), 1, MPI_INFO_NULL, MPI_COMM_WORLD, &window);
#endif

	JOIN_DEBUG("Window", "Allocated %lu bytes", sizeInElements * sizeof(hpcjoin::data::CompressedTuple));

	/**
	 * Run segmentation
	 */

	this->currentRunData = this->data;
	this->currentRunNode = 0;
	this->currentRunRemainingElements = numberOfElementsFromNode[0];

}

void Window::write(uint32_t targetNode, CompressedTuple* tuples, uint32_t sizeInTuples) {

	JOIN_DEBUG("Window", "Writing to window");

	uint32_t sizeInBytes = sizeInTuples*sizeof(hpcjoin::data::CompressedTuple);
	uint64_t targetOffset = (writeOffsets[targetNode]+writeCounters[targetNode]) * sizeof(hpcjoin::data::CompressedTuple);

	//JOIN_DEBUG("Window", "Writing %d bytes (%d tuples) to process %d to offset %lu (%lu + %lu)", sizeInBytes, sizeInTuples, targetNode, targetOffset, writeOffsets[targetNode], writeCounters[targetNode]);

#ifdef USE_FOMPI
	foMPI_Put(tuples, sizeInBytes, MPI_BYTE, targetNode, targetOffset, sizeInBytes, MPI_BYTE, window);
#else
	MPI_Put(tuples, sizeInBytes, MPI_BYTE, targetNode, targetOffset, sizeInBytes, MPI_BYTE, window);
#endif
	writeCounters[targetNode] += sizeInTuples;

	JOIN_DEBUG("Window", "Write completed");
}

bool Window::getNextRun(CompressedTuple** tuples, uint64_t* sizeInTuples) {

	bool validElement = (this->currentRunNode < this->numberOfNodes);
	if(validElement) {
		uint32_t elementsInRun  = MIN(this->currentRunRemainingElements, hpcjoin::core::Configuration::SORT_RUN_ELEMENT_COUNT);
		hpcjoin::data::CompressedTuple *runStart = this->currentRunData;

		this->currentRunData += elementsInRun;
		this->currentRunRemainingElements -= elementsInRun;

		if(this->currentRunRemainingElements == 0) {
			++(this->currentRunNode);
			if(this->currentRunNode < this->numberOfNodes) {
				this->currentRunRemainingElements = this->numberOfElementsFromNode[this->currentRunNode];
				if(this->numberOfElementsFromNode[this->currentRunNode-1] % 2 != 0) {
					++(this->currentRunData);
				}
			}
		}

		*tuples = runStart;
		*sizeInTuples = elementsInRun;

		return true;
	} else {
		return false;
	}
}

void Window::start() {

	JOIN_DEBUG("Window", "Starting window");
#ifdef USE_FOMPI
	foMPI_Win_lock_all(0, window);
#else
	MPI_Win_lock_all(0, window);
#endif

}

void Window::stop() {

	JOIN_DEBUG("Window", "Stopping window");
#ifdef USE_FOMPI
	foMPI_Win_unlock_all(window);
#else
	MPI_Win_unlock_all (window);
#endif

	for(uint32_t i=0; i<numberOfNodes; ++i) {
		JOIN_DEBUG("Window", "Written %lu elements to node %d", this->writeCounters[i], i)
	}
}

hpcjoin::data::CompressedTuple* Window::getData() {
	return this->data;
}


} /* namespace data */
} /* namespace hpcjoin */

