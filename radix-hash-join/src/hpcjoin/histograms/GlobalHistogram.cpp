/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "GlobalHistogram.h"

#include <mpi.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>

namespace hpcjoin {
namespace histograms {


GlobalHistogram::GlobalHistogram(LocalHistogram* localHistogram) {

	this->localHistogram = localHistogram;
	this->values = (uint64_t *) calloc(hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, sizeof(uint64_t));

}

GlobalHistogram::~GlobalHistogram() {

	free(values);

}

void GlobalHistogram::computeGlobalHistogram() {

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::startHistogramGlobalHistogramComputation();
#endif

	MPI_Allreduce(this->localHistogram->getLocalHistogram(), this->values, hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::stopHistogramGlobalHistogramComputation();
#endif

}

uint64_t* GlobalHistogram::getGlobalHistogram() {

	return this->values;

}


} /* namespace histograms */
} /* namespace hpcjoin */
