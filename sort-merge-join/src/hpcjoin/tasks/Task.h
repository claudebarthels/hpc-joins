/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#ifndef HPCJOIN_TASKS_TASK_H_
#define HPCJOIN_TASKS_TASK_H_

namespace hpcjoin {
namespace tasks {

class Task {

public:

	virtual ~Task() {};
	virtual void execute() = 0;

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_TASK_H_ */
