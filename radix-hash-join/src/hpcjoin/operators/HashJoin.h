/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef OPERATORS_JOIN_H_
#define OPERATORS_JOIN_H_

#include <stdint.h>
#include <queue>

#include <hpcjoin/data/Relation.h>
#include <hpcjoin/tasks/Task.h>


namespace hpcjoin {
namespace operators {

class HashJoin {

public:

	HashJoin(uint32_t numberOfNodes, uint32_t nodeId, hpcjoin::data::Relation *innerRelation, hpcjoin::data::Relation *outerRelation);
	~HashJoin();

public:

	void join();

protected:

	uint32_t numberOfNodes;
	uint32_t nodeId;

	hpcjoin::data::Relation *innerRelation;
	hpcjoin::data::Relation *outerRelation;

public:

	static uint64_t RESULT_COUNTER;
	static std::queue<hpcjoin::tasks::Task *> TASK_QUEUE;


};

} /* namespace operators */
} /* namespace hpcjoin */

#endif /* OPERATORS_JOIN_H_ */
