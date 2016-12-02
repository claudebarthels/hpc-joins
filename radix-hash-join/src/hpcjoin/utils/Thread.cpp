/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "Thread.h"


#include <pthread.h>

namespace hpcjoin {
namespace utils {

void Thread::pin(uint32_t coreId) {

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(coreId, &cpuset);

	pthread_t current_thread = pthread_self();
	pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);

}


} /* namespace utils */
} /* namespace hpcjoin */
