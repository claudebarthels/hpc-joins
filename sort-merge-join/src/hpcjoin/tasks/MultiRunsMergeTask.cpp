/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#include "MultiRunsMergeTask.h"

#include <errno.h>
#include <string.h>
#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/balkesen/merge/avx_multiwaymerge.h>

#define L2SIZE (256*1024)

namespace hpcjoin {
namespace tasks {

MultiRunsMergeTask::MultiRunsMergeTask(hpcjoin::data::CompressedTuple** runs, uint64_t* numberOfElements, uint32_t numberOfRuns, hpcjoin::data::CompressedTuple *output) {

	this->numberOfRuns = numberOfRuns;
	this->runs = runs;
	this->numberOfElements = numberOfElements;

	for (uint32_t i = 0; i < numberOfRuns; ++i) {
		JOIN_ASSERT(((uint64_t) runs[i]) % 16 == 0, "MultiwayMerging", "Run %d is not aligned", i);
	}

	outputSize = 0;
	for (uint32_t i = 0; i < numberOfRuns; ++i) {
		outputSize += numberOfElements[i];
	}

	this->output = output;
	JOIN_ASSERT(((uint64_t) output) % 16 == 0, "MultiwayMerging", "Output not aligned to 16 bytes");

	//int32_t returnValue = posix_memalign((void **) &output, hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES, outputSize * sizeof(hpcjoin::data::CompressedTuple));
	//JOIN_ASSERT(returnValue == 0, "MultiwayMerging", "Cannot allocate output memory of %lu elements (Error %s)", outputSize, strerror(errno));
	//memset(output, 0, outputSize * sizeof(hpcjoin::data::CompressedTuple));

	int32_t returnValue = posix_memalign((void **) &fifo, hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES, L2SIZE);
	JOIN_ASSERT(returnValue == 0, "MultiwayMerging", "Cannot allocate fifo memory");
	memset(fifo, 0, L2SIZE);

	JOIN_ASSERT(sizeof(hpcjoin::data::CompressedTuple) == sizeof(tuple_t), "MultiwayMerging", "Tupe sizes do not match");

}

MultiRunsMergeTask::~MultiRunsMergeTask() {
	free(fifo);
}

void MultiRunsMergeTask::execute() {
	hpcjoin::performance::Measurements::startMergingTask();

	JOIN_ASSERT(numberOfRuns % 2 == 0, "MultiwayMerging", "Even number of runs required");
	JOIN_ASSERT(sizeof(tuple_t) == sizeof(uint64_t), "MultiwayMerging", "Padding has been added to tuple struct");
	JOIN_ASSERT(sizeof(hpcjoin::data::CompressedTuple) == sizeof(uint64_t), "MultiwayMerging", "Padding has been added to compressed tuple");

	relation_t ** chunkptrs = new relation_t *[numberOfRuns];

	relation_t *runsAsRelation = new relation_t[this->numberOfRuns];
	for (uint32_t i = 0; i < numberOfRuns; ++i) {
		runsAsRelation[i].num_tuples = numberOfElements[i];
		runsAsRelation[i].tuples = (tuple_t *) runs[i];
		JOIN_ASSERT(((uint64_t) runs[i]) % 16 ==0, "MultiwayMerging", "Input buffers not aligned");
		chunkptrs[i] = &(runsAsRelation[i]);
	}

	/*for (uint32_t r = 0; r < numberOfRuns; ++r) {
		uint64_t oldValue = 0;
		for (uint64_t t = 0; t < runsAsRelation[r].num_tuples; ++t) {
			uint64_t value = runs[r][t].value;
			if (!(oldValue <= value)) {
				printf("NOT SORTED AS 64 ON MERGE. %d, %lu. Seq: %lu < %lu\n", r, t, oldValue, value);
				exit(-1);
			}
			oldValue = value;
		}
	}
	JOIN_DEBUG("MutiwayMerging", "All %d runs are sorted as values", numberOfRuns)*/

	JOIN_DEBUG("MutiwayMerging", "Starting merging");
	avx_multiway_merge((tuple_t *)output, chunkptrs, numberOfRuns, (tuple_t *)fifo, L2SIZE/sizeof(tuple_t));
	JOIN_DEBUG("MutiwayMerging", "Merging completed");

	delete chunkptrs;
	hpcjoin::performance::Measurements::stopMergingTask(outputSize);
}

hpcjoin::data::CompressedTuple* MultiRunsMergeTask::getOutput() {
	return output;
}

uint64_t MultiRunsMergeTask::getOutputSize() {
	return outputSize;
}

} /* namespace tasks */
} /* namespace hpcjoin */

