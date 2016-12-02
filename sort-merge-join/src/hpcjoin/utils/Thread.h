/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef UTILS_THREAD_H_
#define UTILS_THREAD_H_

#include <stdint.h>

namespace hpcjoin {
namespace utils {

class Thread {

public:

		static void pin(uint32_t coreId);
};

} /* namespace utils */
} /* namespace hpcjoin */

#endif /* UTILS_THREAD_H_ */
