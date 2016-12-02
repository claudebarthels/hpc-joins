/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_HISTOGRAMS_OFFSETMAP_H_
#define HPCJOIN_HISTOGRAMS_OFFSETMAP_H_

#include <stdint.h>

#include <hpcjoin/histograms/LocalHistogram.h>
#include <hpcjoin/histograms/GlobalHistogram.h>
#include <hpcjoin/histograms/AssignmentMap.h>

namespace hpcjoin {
namespace histograms {

class OffsetMap {

public:

	OffsetMap(uint32_t numberOfProcesses, hpcjoin::histograms::LocalHistogram *localHistogram, hpcjoin::histograms::GlobalHistogram *globalHistogram, hpcjoin::histograms::AssignmentMap *assignment);
	~OffsetMap();

public:

	void computeOffsets();

public:

	uint64_t *getBaseOffsets();
	uint64_t *getRelativeWriteOffsets();
	uint64_t *getAbsoluteWriteOffsets();

protected:

	void computeBaseOffsets();
	void computeRelativePrivateOffsets();
	void computeAbsolutePrivateOffsets();

protected:

	uint32_t numberOfProcesses;
	hpcjoin::histograms::LocalHistogram *localHistogram;
	hpcjoin::histograms::GlobalHistogram *globalHistogram;
	hpcjoin::histograms::AssignmentMap *assignment;

	uint64_t *baseOffsets;
	uint64_t *relativeWriteOffsets;
	uint64_t *absoluteWriteOffsets;

};

} /* namespace histograms */
} /* namespace hpcjoin */

#endif /* HPCJOIN_HISTOGRAMS_OFFSETMAP_H_ */
