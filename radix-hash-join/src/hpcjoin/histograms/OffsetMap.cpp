/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "OffsetMap.h"

#include <stdlib.h>
#include <mpi.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/performance/Measurements.h>

namespace hpcjoin {
namespace histograms {

OffsetMap::OffsetMap(uint32_t numberOfProcesses, LocalHistogram* localHistogram, GlobalHistogram* globalHistogram, AssignmentMap* assignment) {

	this->numberOfProcesses = numberOfProcesses;
	this->localHistogram = localHistogram;
	this->globalHistogram = globalHistogram;
	this->assignment = assignment;

	this->baseOffsets = (uint64_t *) calloc(hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, sizeof(uint64_t));
	this->relativeWriteOffsets = (uint64_t *) calloc(hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, sizeof(uint64_t));
	this->absoluteWriteOffsets = (uint64_t *) calloc(hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, sizeof(uint64_t));

}

OffsetMap::~OffsetMap() {

	free(this->baseOffsets);
	free(this->relativeWriteOffsets);
	free(this->absoluteWriteOffsets);

}

void OffsetMap::computeOffsets() {

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::startHistogramOffsetComputation();
#endif

	computeBaseOffsets();
	computeRelativePrivateOffsets();
	computeAbsolutePrivateOffsets();

#ifdef MEASUREMENT_DETAILS_HISTOGRAM
	hpcjoin::performance::Measurements::stopHistogramOffsetComputation();
#endif

}

void OffsetMap::computeBaseOffsets() {

	uint64_t *currentOffsets = (uint64_t *) calloc(this->numberOfProcesses, sizeof(uint64_t));
	uint32_t *partitionAssignment = this->assignment->getPartitionAssignment();
	uint64_t *histogram = this->globalHistogram->getGlobalHistogram();

	for (uint32_t i = 0; i < hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++i) {
		uint32_t assignedNode = partitionAssignment[i];
		this->baseOffsets[i] = currentOffsets[assignedNode];
		currentOffsets[assignedNode] += histogram[i];
	}

	free(currentOffsets);

}

void OffsetMap::computeRelativePrivateOffsets() {

	MPI_Scan(this->localHistogram->getLocalHistogram(), this->relativeWriteOffsets, hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT, MPI_UINT64_T, MPI_SUM,
			MPI_COMM_WORLD);

	uint64_t *histogram = this->localHistogram->getLocalHistogram();
	for (uint32_t i = 0; i < hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++i) {
		this->relativeWriteOffsets[i] -= histogram[i];
	}

}

void OffsetMap::computeAbsolutePrivateOffsets() {

	for (uint32_t i = 0; i < hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++i) {
		this->absoluteWriteOffsets[i] = this->baseOffsets[i] + this->relativeWriteOffsets[i];
	}

}

uint64_t* OffsetMap::getBaseOffsets() {

	return baseOffsets;

}

uint64_t* OffsetMap::getRelativeWriteOffsets() {

	return relativeWriteOffsets;

}

uint64_t* OffsetMap::getAbsoluteWriteOffsets() {

	return absoluteWriteOffsets;

}

} /* namespace histograms */
} /* namespace hpcjoin */
