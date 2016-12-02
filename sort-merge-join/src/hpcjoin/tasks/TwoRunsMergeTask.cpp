/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include <hpcjoin/tasks/TwoRunsMergeTask.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/balkesen/merge/merge.h>

#define CACHELINE_SIZE (64)

namespace hpcjoin {
namespace tasks {

TwoRunsMergeTask::TwoRunsMergeTask(hpcjoin::data::CompressedTuple* leftRun, uint64_t leftNumberOfElements, hpcjoin::data::CompressedTuple* rightRun,
		uint64_t rightNumberOfElements, hpcjoin::data::CompressedTuple *output) {

	this->leftRun = leftRun;
	this->leftNumberOfElements = leftNumberOfElements;
	this->rightRun = rightRun;
	this->rightNumberOfElements = rightNumberOfElements;

	this->outputNumberOfElements = leftNumberOfElements + rightNumberOfElements;

	this->output = output;
	JOIN_ASSERT(((uint64_t) output) % 16 == 0, "TwoRunMerging", "Output not aligned to 16 bytes");

	//int returnValue = posix_memalign((void **) &(this->output), CACHELINE_SIZE, this->outputNumberOfElements * sizeof(hpcjoin::data::CompressedTuple));
	//JOIN_ASSERT(returnValue == 0, "TwoRunMerging", "Cannot allocate output memory of %lu elements (Error %s)", this->outputNumberOfElements, strerror(errno));
	//memset(output, 0, this->outputNumberOfElements * sizeof(hpcjoin::data::CompressedTuple));

}

TwoRunsMergeTask::~TwoRunsMergeTask() {
}

void TwoRunsMergeTask::execute() {

	hpcjoin::performance::Measurements::startMergingTask();
	avx_merge_int64((int64_t *) leftRun, (int64_t *) rightRun, (int64_t *) output, leftNumberOfElements, rightNumberOfElements);
	hpcjoin::performance::Measurements::stopMergingTask(leftNumberOfElements + rightNumberOfElements);

	/*uint64_t oldValue = 0;
	for (uint64_t t = 0; t < outputNumberOfElements; ++t) {
		uint64_t value = output[t].value;
		if (!(oldValue <= value)) {
			printf("NOT SORTED AS 64 AFTER MERGE. Seq: %lu < %lu\n", oldValue, value);
			exit(-1);
		}
		oldValue = value;
	}*/

	//JOIN_DEBUG("TwowayMerging", "Output run is sorted as values")

}

hpcjoin::data::CompressedTuple* TwoRunsMergeTask::getOutput() {
	return this->output;
}

uint64_t TwoRunsMergeTask::getOutputSize() {
	return this->outputNumberOfElements;
}

} /* namespace tasks */
} /* namespace hpcjoin */

