/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_HISTOGRAMS_GLOBALHISTOGRAM_H_
#define HPCJOIN_HISTOGRAMS_GLOBALHISTOGRAM_H_

#include <hpcjoin/histograms/LocalHistogram.h>

namespace hpcjoin {
namespace histograms {

class GlobalHistogram {

public:

	GlobalHistogram(hpcjoin::histograms::LocalHistogram *localHistogram);
	~GlobalHistogram();

public:

	void computeGlobalHistogram();
	uint64_t *getGlobalHistogram();

protected:

	hpcjoin::histograms::LocalHistogram *localHistogram;
	uint64_t *values;

};

} /* namespace histograms */
} /* namespace hpcjoin */

#endif /* HPCJOIN_HISTOGRAMS_GLOBALHISTOGRAM_H_ */
