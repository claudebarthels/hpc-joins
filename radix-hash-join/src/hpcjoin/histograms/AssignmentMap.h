/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_HISTOGRAMS_ASSIGNMENTMAP_H_
#define HPCJOIN_HISTOGRAMS_ASSIGNMENTMAP_H_

#include <stdint.h>

#include <hpcjoin/histograms/GlobalHistogram.h>

namespace hpcjoin {
namespace histograms {

class AssignmentMap {

public:

	AssignmentMap(uint32_t numberOfNodes, hpcjoin::histograms::GlobalHistogram *innerRelationGlobalHistogram, hpcjoin::histograms::GlobalHistogram *outerRelationGlobalHistogram);
	~AssignmentMap();

public:

	void computePartitionAssignment();
	uint32_t *getPartitionAssignment();

protected:

	uint32_t numberOfNodes;
	hpcjoin::histograms::GlobalHistogram *innerRelationGlobalHistogram;
	hpcjoin::histograms::GlobalHistogram *outerRelationGlobalHistogram;

	uint32_t *assignment;

};

} /* namespace histograms */
} /* namespace hpcjoin */

#endif /* HPCJOIN_HISTOGRAMS_ASSIGNMENTMAP_H_ */
