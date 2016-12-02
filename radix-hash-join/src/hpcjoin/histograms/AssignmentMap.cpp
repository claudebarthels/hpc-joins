/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "AssignmentMap.h"

#include <stdlib.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/performance/Measurements.h>

namespace hpcjoin {
namespace histograms {

AssignmentMap::AssignmentMap(uint32_t numberOfNodes, hpcjoin::histograms::GlobalHistogram *innerRelationGlobalHistogram, hpcjoin::histograms::GlobalHistogram *outerRelationGlobalHistogram) {

	this->numberOfNodes = numberOfNodes;
	this->innerRelationGlobalHistogram = innerRelationGlobalHistogram;
	this->outerRelationGlobalHistogram = outerRelationGlobalHistogram;
	this->assignment = (uint32_t *) calloc(hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, sizeof(uint32_t));

}

AssignmentMap::~AssignmentMap() {

	free(this->assignment);

}

void AssignmentMap::computePartitionAssignment() {

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::startHistogramAssignmentComputation();
#endif

	for(uint32_t p=0; p<hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++p) {
		assignment[p] = p % this->numberOfNodes;
	}

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::stopHistogramAssignmentComputation();
#endif

}

uint32_t* AssignmentMap::getPartitionAssignment() {

	return this->assignment;

}

} /* namespace histograms */
} /* namespace hpcjoin */

