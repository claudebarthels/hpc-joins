/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_DATA_RELATION_H_
#define HPCJOIN_DATA_RELATION_H_

#include <stdint.h>

#include <hpcjoin/data/Tuple.h>

namespace hpcjoin {
namespace data {

class Relation {

public:

	Relation(uint64_t localSize, uint64_t globalSize);
	~Relation();

public:

	uint64_t getLocalSize();
	uint64_t getGlobalSize();

public:

	hpcjoin::data::Tuple* getData();

public:

	void fillUniqueValues(uint64_t startKeyValue, uint64_t startRidValue);
	void fillModuloValues(uint64_t startKeyValue, uint64_t startRidValue, uint64_t innerRelationSize);

protected:

	void randomOrder();

public:

	void distribute(uint32_t nodeId, uint32_t numberOfNodes);

public:

	void debugKeyPrint();

protected:

	uint64_t localSize;
	uint64_t globalSize;

	hpcjoin::data::Tuple *data;

};

} /* namespace data */
} /* namespace hpcjoin */

#endif /* HPCJOIN_DATA_RELATION_H_ */
