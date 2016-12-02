/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_TASKS_MULTIRUNSMERGETASK_H_
#define HPCJOIN_TASKS_MULTIRUNSMERGETASK_H_

#include <stdint.h>

#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace tasks {

class MultiRunsMergeTask: public Task {

public:

	MultiRunsMergeTask(hpcjoin::data::CompressedTuple **runs, uint64_t *numberOfElements, uint32_t numberOfRuns, hpcjoin::data::CompressedTuple *output);
	~MultiRunsMergeTask();

	void execute();

public:

	hpcjoin::data::CompressedTuple * getOutput();
	uint64_t getOutputSize();

protected:

	uint32_t numberOfRuns;
	hpcjoin::data::CompressedTuple** runs;
	uint64_t* numberOfElements;

	hpcjoin::data::CompressedTuple * fifo;
	hpcjoin::data::CompressedTuple * output;
	uint64_t outputSize;

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_MULTIRUNSMERGETASK_H_ */
