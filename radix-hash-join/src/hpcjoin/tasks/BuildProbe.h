/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_TASKS_BUILDPROBE_H_
#define HPCJOIN_TASKS_BUILDPROBE_H_

#include <stdint.h>

#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace tasks {

class BuildProbe : public Task {

public:

	BuildProbe(uint64_t innerPartitionSize, hpcjoin::data::CompressedTuple *innerPartition, uint64_t outerPartitionSize, hpcjoin::data::CompressedTuple *outerPartition);
	~BuildProbe();

public:

	void execute();
	task_type_t getType();

protected:

	uint64_t innerPartitionSize;
	hpcjoin::data::CompressedTuple *innerPartition;
	uint64_t outerPartitionSize;
	hpcjoin::data::CompressedTuple *outerPartition;

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_BUILDPROBE_H_ */
