/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#ifndef HPCJOIN_TASKS_MERGEJOINTASK_H_
#define HPCJOIN_TASKS_MERGEJOINTASK_H_

#include <stdint.h>

#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace tasks {

class MergeJoinTask : public Task {

public:

	MergeJoinTask(hpcjoin::data::CompressedTuple *leftRun, uint64_t leftNumberOfElements, hpcjoin::data::CompressedTuple *rightRun, uint64_t rightNumberOfElements, uint32_t numberOfNodes);
	~MergeJoinTask();

	void execute();

public:

	uint64_t getNumberOfMatchingTuples();

protected:

	uint32_t numberOfNodes;

	hpcjoin::data::CompressedTuple *leftRun;
	uint64_t leftNumberOfElements;

	hpcjoin::data::CompressedTuple *rightRun;
	uint64_t rightNumberOfElements;

	uint64_t matchingTuplesCount;

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_MERGEJOINTASK_H_ */
