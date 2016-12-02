/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_TASKS_NETWORKPARTITIONING_H_
#define HPCJOIN_TASKS_NETWORKPARTITIONING_H_

#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/Window.h>
#include <hpcjoin/data/Relation.h>

namespace hpcjoin {
namespace tasks {

class NetworkPartitioning : public Task {

public:

	NetworkPartitioning(uint32_t nodeId, hpcjoin::data::Relation *innerRelation, hpcjoin::data::Relation *outerRelation, hpcjoin::data::Window *innerWindow, hpcjoin::data::Window *outerWindow);
	~NetworkPartitioning();

public:

	void execute();
	task_type_t getType();

protected:

	void partition(hpcjoin::data::Relation *relation, hpcjoin::data::Window *window);

protected:

	uint32_t nodeId;

	hpcjoin::data::Relation *innerRelation;
	hpcjoin::data::Relation *outerRelation;

	hpcjoin::data::Window *innerWindow;
	hpcjoin::data::Window *outerWindow;

protected:

	inline static void streamWrite(void *to, void *from)  __attribute__((always_inline));

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_NETWORKPARTITIONING_H_ */
