/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#ifndef HPCJOIN_OPERATORS_SORTMERGEJOIN_H_
#define HPCJOIN_OPERATORS_SORTMERGEJOIN_H_

#include <stdint.h>
#include <queue>

#include <hpcjoin/data/Relation.h>
#include <hpcjoin/tasks/SortTask.h>

namespace hpcjoin {
namespace operators {

class SortMergeJoin {

public:

	SortMergeJoin(uint32_t numberOfNodes, uint32_t nodeId, hpcjoin::data::Relation *innerRelation, hpcjoin::data::Relation *outerRelation);
	~SortMergeJoin();

public:

	void join();

protected:

	uint32_t numberOfNodes;
	uint32_t nodeId;

	hpcjoin::data::Relation *innerRelation;
	hpcjoin::data::Relation *outerRelation;

public:

	static uint64_t RESULT_COUNTER;
	static std::queue<hpcjoin::tasks::SortTask *> SORT_TASK_QUEUE;

	static std::vector<hpcjoin::data::CompressedTuple*> INNER_SORTED_RUN_QUEUE;
	static std::vector<hpcjoin::data::CompressedTuple*> OUTER_SORTED_RUN_QUEUE;
	static std::vector<uint64_t> INNER_SORTED_RUN_SIZE_QUEUE;
	static std::vector<uint64_t> OUTER_SORTED_RUN_SIZE_QUEUE;

};

} /* namespace operators */
} /* namespace hpcjoin */

#endif /* HPCJOIN_OPERATORS_SORTMERGEJOIN_H_ */
