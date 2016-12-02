/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#ifndef HPCJOIN_TASKS_MERGELEVELTASK_H_
#define HPCJOIN_TASKS_MERGELEVELTASK_H_

#include <stdint.h>

#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace tasks {

class MergeLevelTask: public Task  {

public:

	MergeLevelTask(uint32_t numberOfInputRuns, hpcjoin::data::CompressedTuple** inputRuns, uint64_t *inputRunSizes, hpcjoin::data::CompressedTuple* output);
	~MergeLevelTask();

	void execute();

public:

	hpcjoin::data::CompressedTuple** getOutputRuns();
	uint64_t * getOutputRunSizes();
	uint32_t getNumberOfOutputRuns();


protected:

	uint32_t numberOfInputRuns;
	hpcjoin::data::CompressedTuple** inputRuns;
	uint64_t *inputRunSizes;

	hpcjoin::data::CompressedTuple* output;
	hpcjoin::data::CompressedTuple** outputRuns;
	uint64_t *outputRunSizes;
	uint32_t numberOfOutputRuns;

protected:

	void registerStartOfRun(hpcjoin::data::CompressedTuple *run, uint64_t size);
	static uint64_t MERGE_OR_COPY(hpcjoin::data::CompressedTuple** inputRuns, uint64_t *inputRunSizes, uint32_t numberOfInputRuns, hpcjoin::data::CompressedTuple* output);

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_MERGELEVELTASK_H_ */
