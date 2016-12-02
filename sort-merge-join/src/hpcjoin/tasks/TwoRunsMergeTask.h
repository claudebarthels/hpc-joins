/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#ifndef HPCJOIN_TASKS_TWORUNSMERGETASK_H_
#define HPCJOIN_TASKS_TWORUNSMERGETASK_H_

#include <stdint.h>

#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace tasks {

class TwoRunsMergeTask : public Task {

public:

	TwoRunsMergeTask(hpcjoin::data::CompressedTuple *leftRun, uint64_t leftNumberOfElements, hpcjoin::data::CompressedTuple *rightRun, uint64_t rightNumberOfElements, hpcjoin::data::CompressedTuple *output);
	~TwoRunsMergeTask();

	void execute();

public:

	hpcjoin::data::CompressedTuple * getOutput();
	uint64_t getOutputSize();

protected:

	hpcjoin::data::CompressedTuple *leftRun;
	uint64_t leftNumberOfElements;

	hpcjoin::data::CompressedTuple *rightRun;
	uint64_t rightNumberOfElements;

	hpcjoin::data::CompressedTuple *output;
	uint64_t outputNumberOfElements;

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_TWORUNSMERGETASK_H_ */
