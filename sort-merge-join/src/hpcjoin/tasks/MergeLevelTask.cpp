/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "MergeLevelTask.h"

#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/tasks/TwoRunsMergeTask.h>
#include <hpcjoin/tasks/MultiRunsMergeTask.h>
#include <hpcjoin/performance/Measurements.h>

#define MIN(a,b) (((a)<(b))?(a):(b))

namespace hpcjoin {
namespace tasks {

MergeLevelTask::MergeLevelTask(uint32_t numberOfInputRuns, hpcjoin::data::CompressedTuple** inputRuns, uint64_t* inputRunSizes, hpcjoin::data::CompressedTuple* output) {

	this->numberOfInputRuns = numberOfInputRuns;
	this->inputRuns = inputRuns;
	this->inputRunSizes = inputRunSizes;
	this->output = output;
	this->outputRuns = new hpcjoin::data::CompressedTuple* [numberOfInputRuns];
	this->outputRunSizes = new uint64_t[numberOfInputRuns];
	this->numberOfOutputRuns = 0;

}

MergeLevelTask::~MergeLevelTask() {
}

void MergeLevelTask::execute() {

	hpcjoin::performance::Measurements::startMergingLevel();
	uint32_t currentRunIndex = 0;
	uint32_t remainingRunCount = this->numberOfInputRuns;
	hpcjoin::data::CompressedTuple *currentOutput = this->output;

	while (remainingRunCount > 0) {
		uint32_t dequeueCounter = MIN(hpcjoin::core::Configuration::MAX_MERGE_FAN_IN, remainingRunCount);

		uint32_t outputSize = MERGE_OR_COPY(this->inputRuns + currentRunIndex, this->inputRunSizes + currentRunIndex, dequeueCounter, currentOutput);
		registerStartOfRun(currentOutput, outputSize);

		if (outputSize % 2 != 0) {
			++outputSize;
		}

		currentOutput += outputSize;
		currentRunIndex += dequeueCounter;
		remainingRunCount -= dequeueCounter;
	}
	hpcjoin::performance::Measurements::stopMergingLevel();

}

hpcjoin::data::CompressedTuple** MergeLevelTask::getOutputRuns() {
	return this->outputRuns;
}

uint64_t* MergeLevelTask::getOutputRunSizes() {
	return this->outputRunSizes;
}

void MergeLevelTask::registerStartOfRun(hpcjoin::data::CompressedTuple* run, uint64_t size) {
	this->outputRuns[this->numberOfOutputRuns] = run;
	this->outputRunSizes[this->numberOfOutputRuns] = size;
	++numberOfOutputRuns;
}

uint32_t MergeLevelTask::getNumberOfOutputRuns() {
	return this->numberOfOutputRuns;
}

uint64_t MergeLevelTask::MERGE_OR_COPY(hpcjoin::data::CompressedTuple** inputRuns, uint64_t* inputRunSizes, uint32_t numberOfInputRuns, hpcjoin::data::CompressedTuple* output) {

	if (numberOfInputRuns == 1) {

		memcpy(output, inputRuns[0], inputRunSizes[0] * sizeof(hpcjoin::data::CompressedTuple));

	} else if (numberOfInputRuns == 2 || numberOfInputRuns == 3) {

		bool reduceRunRequired = (numberOfInputRuns == 3);
		hpcjoin::data::CompressedTuple *twoRunBuffer = NULL;

		if (reduceRunRequired) {
			uint64_t twoRunSize = inputRunSizes[1] + inputRunSizes[2];
			uint32_t returnValue = posix_memalign((void **) &twoRunBuffer, hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES, twoRunSize * sizeof(hpcjoin::data::CompressedTuple));
			JOIN_ASSERT(returnValue == 0, "MergeLevel", "Cannot allocate temporary memory of %lu elements (Error %s)", twoRunSize, strerror(errno));

			hpcjoin::tasks::TwoRunsMergeTask *mergeReduceTask = new hpcjoin::tasks::TwoRunsMergeTask(inputRuns[1], inputRunSizes[1], inputRuns[2], inputRunSizes[2], twoRunBuffer);
			mergeReduceTask->execute();
			delete mergeReduceTask;

			inputRuns[1] = twoRunBuffer;
			inputRunSizes[1] = twoRunSize;
			numberOfInputRuns = 2;
		}

		hpcjoin::tasks::TwoRunsMergeTask *mergeTask = new hpcjoin::tasks::TwoRunsMergeTask(inputRuns[0], inputRunSizes[0], inputRuns[1], inputRunSizes[1], output);
		mergeTask->execute();
		delete mergeTask;

		if (reduceRunRequired) {
			free(twoRunBuffer);
		}

	} else {

		bool reduceRunRequired = (numberOfInputRuns % 2 != 0);
		hpcjoin::data::CompressedTuple *twoRunBuffer = NULL;

		if (reduceRunRequired) {
			uint64_t twoRunSize = inputRunSizes[numberOfInputRuns - 1] + inputRunSizes[numberOfInputRuns - 2];

			uint32_t returnValue = posix_memalign((void **) &twoRunBuffer, hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES, twoRunSize * sizeof(hpcjoin::data::CompressedTuple));
			JOIN_ASSERT(returnValue == 0, "MergeLevel", "Cannot allocate temporary memory of %lu elements (Error %s)", twoRunSize, strerror(errno));

			hpcjoin::tasks::TwoRunsMergeTask *mergeReduceTask = new hpcjoin::tasks::TwoRunsMergeTask(inputRuns[numberOfInputRuns-2], inputRunSizes[numberOfInputRuns-2], inputRuns[numberOfInputRuns-1], inputRunSizes[numberOfInputRuns-1], twoRunBuffer);
			mergeReduceTask->execute();
			delete mergeReduceTask;

			inputRuns[numberOfInputRuns - 2] = twoRunBuffer;
			inputRunSizes[numberOfInputRuns - 2] = twoRunSize;
			numberOfInputRuns -= 1;
		}

		hpcjoin::tasks::MultiRunsMergeTask *mergeTask = new hpcjoin::tasks::MultiRunsMergeTask(inputRuns, inputRunSizes, numberOfInputRuns, output);
		mergeTask->execute();
		delete mergeTask;

		if (reduceRunRequired) {
			free(twoRunBuffer);
		}

	}

	uint64_t elementSum = 0;
	for(uint32_t i=0; i<numberOfInputRuns; ++i) {
		elementSum += inputRunSizes[i];
	}

	return elementSum;

}

} /* namespace tasks */
} /* namespace hpcjoin */

