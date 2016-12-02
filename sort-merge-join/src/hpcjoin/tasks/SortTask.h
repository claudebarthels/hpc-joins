/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#ifndef HPCJOIN_TASKS_SORTTASK_H_
#define HPCJOIN_TASKS_SORTTASK_H_

#include <stdint.h>
#include <hpcjoin/data/CompressedTuple.h>
#include <hpcjoin/tasks/Task.h>
#include <hpcjoin/data/Window.h>

namespace hpcjoin {
namespace tasks {

class SortTask: public Task {

public:

	SortTask(hpcjoin::data::CompressedTuple *tuples, uint64_t numberOfElements, hpcjoin::data::Window *window, uint32_t targetNode);
	~SortTask();

	void execute();

protected:

	uint64_t numberOfElements;
	hpcjoin::data::CompressedTuple *input;
	hpcjoin::data::CompressedTuple *output;
	hpcjoin::data::Window *window;
	uint32_t targetNode;

protected:

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_SORTTASK_H_ */
