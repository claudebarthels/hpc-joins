/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_HISTOGRAMS_LOCALHISTOGRAM_H_
#define HPCJOIN_HISTOGRAMS_LOCALHISTOGRAM_H_

#include <hpcjoin/data/Relation.h>

namespace hpcjoin {
namespace histograms {

class LocalHistogram {

public:

	LocalHistogram(hpcjoin::data::Relation *relation);
	~LocalHistogram();

public:

	void computeLocalHistogram();

	uint64_t *getLocalHistogram();

protected:

	hpcjoin::data::Relation *relation;
	uint64_t *values;

};

} /* namespace histograms */
} /* namespace hpcjoin */

#endif /* HPCJOIN_HISTOGRAMS_LOCALHISTOGRAM_H_ */
