/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#include "SortTask.h"

#include <immintrin.h>
#include <algorithm>
#include <stdlib.h>
#include <hpcjoin/utils/Debug.h>
#include <mpi.h>
#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/balkesen/sort/avxsort.h>


namespace hpcjoin {
namespace tasks {

SortTask::SortTask(hpcjoin::data::CompressedTuple* tuples, uint64_t numberOfElements, hpcjoin::data::Window *window, uint32_t targetNode) {

	this->numberOfElements = numberOfElements;
	this->input = tuples;
	this->window = window;
	this->targetNode = targetNode;
	int returnValue = posix_memalign((void**) &output, hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES, numberOfElements * sizeof(hpcjoin::data::CompressedTuple));
	JOIN_ASSERT(returnValue == 0, "SortTask", "Could not allocate memory for %lu compressed tuples", numberOfElements);

}

SortTask::~SortTask() {
}

void SortTask::execute() {

	hpcjoin::performance::Measurements::startSortTask();

	hpcjoin::performance::Measurements::startSortingElements();
	avxsort_tuples((tuple_t **) &input, (tuple_t **) &output, numberOfElements);
	hpcjoin::performance::Measurements::stopSortingElements(numberOfElements);

/*	uint64_t oldValue = 0;
	for(uint64_t i=0; i<numberOfElements; ++i) {
		uint64_t value = output[i].value;
		if(!(oldValue <= value)) {
			printf("NOT SORTED ON WRITE\n");
			exit(-1);
		}
		oldValue = value;
	}*/

	hpcjoin::performance::Measurements::startPut();
	this->window->write(targetNode, this->output, numberOfElements);
	hpcjoin::performance::Measurements::stopPut();

	hpcjoin::performance::Measurements::stopSortTask();

}

} /* namespace tasks */
} /* namespace hpcjoin */

