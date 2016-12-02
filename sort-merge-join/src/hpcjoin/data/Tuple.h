/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_DATA_TUPLE_H_
#define HPCJOIN_DATA_TUPLE_H_

#include <stdint.h>

namespace hpcjoin {
namespace data {

class Tuple {

public:

	uint64_t key;
	uint64_t rid;

};

} /* namespace data */
} /* namespace hpcjoin */

#endif /* HPCJOIN_DATA_TUPLE_H_ */
