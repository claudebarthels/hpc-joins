/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "LocalHistogram.h"

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <mpi.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/performance/Measurements.h>

namespace hpcjoin {
namespace histograms {

#define HASH_BIT_MODULO(KEY, MASK, NBITS) (((KEY) & (MASK)) >> (NBITS))

LocalHistogram::LocalHistogram(hpcjoin::data::Relation* relation) {

	this->relation = relation;
	this->values = (uint64_t *) calloc(hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, sizeof(uint64_t));

}

LocalHistogram::~LocalHistogram() {

	free(values);

}

void LocalHistogram::computeLocalHistogram() {

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::startHistogramLocalHistogramComputation();
#endif

	uint64_t const numberOfElements = relation->getLocalSize();
	hpcjoin::data::Tuple * const data = relation->getData();

	for (uint64_t i = 0; i < numberOfElements; ++i) {
		uint32_t partitionIdx = HASH_BIT_MODULO(data[i].key, hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT - 1, 0);
		++(values[partitionIdx]);
	}

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::stopHistogramLocalHistogramComputation(numberOfElements);
#endif

}

uint64_t* LocalHistogram::getLocalHistogram() {

	return this->values;

}

} /* namespace histograms */
} /* namespace hpcjoin */
