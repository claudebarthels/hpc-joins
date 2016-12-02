/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_TASKS_TASK_H_
#define HPCJOIN_TASKS_TASK_H_

enum task_type_t {
	TASK_HISTOGRAM,
	TASK_NET_PARTITION,
	TASK_PARTITION,
	TASK_BUILD_PROBE
} ;

namespace hpcjoin {
namespace tasks {

class Task {

public:

	virtual ~Task() {};

	virtual void execute() = 0;

	virtual task_type_t getType() = 0;

};

} /* namespace tasks */
} /* namespace hpcjoin */

#endif /* HPCJOIN_TASKS_TASK_H_ */
