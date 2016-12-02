/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_TASKS_PARTITIONTASK_H_
#define HPCJOIN_TASKS_PARTITIONTASK_H_

#include <stdint.h>
#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/Relation.h>
#include <hpcjoin/data/Tuple.h>
#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace tasks {

class PartitionTask : public Task {

public:

	PartitionTask(hpcjoin::data::Relation *innerRelation, hpcjoin::data::Relation *outerRelation, uint32_t numberOfNodes);
	~PartitionTask();

	void execute();

protected:

	hpcjoin::data::Relation *innerRelation;
	hpcjoin::data::Relation *outerRelation;
	uint32_t numberOfNodes;

protected:

	static uint64_t * computeHistogram(hpcjoin::data::Relation *relation, uint32_t numberOfNodes);
	static uint64_t computeWindowSize(uint64_t *histogram, uint32_t numberOfNodes);
	static uint64_t * computeWriteOffsets(uint64_t *histogram, uint32_t numberOfNodes);
	static uint64_t * computeIncomingData(uint64_t *histogram, uint32_t numberOfNodes);

protected:

	static uint64_t * computeLocalWriteOffsets(uint64_t *histogram, uint32_t numberOfNodes);
	static void partitionData(hpcjoin::data::Relation *relation, hpcjoin::data::CompressedTuple *outputBuffer, uint64_t *localWriteOffsets, uint32_t numberOfNodes);
	static void store(void* to, void* from);

public:

	uint64_t * innerHistogram;
	uint64_t * outerHistogram;

	uint64_t innerWindowSize;
	uint64_t outerWindowSize;

	uint64_t * innerWriteOffsets;
	uint64_t * outerWriteOffsets;

	uint64_t * innerIncomingData;
	uint64_t * outerIncomingData;

public:

	uint64_t * innerLocalWriteOffsets;
	uint64_t * outerLocalWriteOffsets;

	hpcjoin::data::CompressedTuple *innerPartitionOutput;
	hpcjoin::data::CompressedTuple *outerPartitionOutput;


};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_PARTITIONTASK_H_ */
