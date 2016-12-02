/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "HistogramComputation.h"

#include <stdlib.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>

namespace hpcjoin {
namespace tasks {

HistogramComputation::HistogramComputation(uint32_t numberOfNodes, uint32_t nodeId, hpcjoin::data::Relation *innerRelation, hpcjoin::data::Relation *outerRelation) {

	this->nodeId = nodeId;
	this->numberOfNodes = numberOfNodes;

	this->innerRelation = innerRelation;
	this->outerRelation = outerRelation;

	this->innerRelationLocalHistogram = new hpcjoin::histograms::LocalHistogram(innerRelation);
	this->outerRelationLocalHistogram = new hpcjoin::histograms::LocalHistogram(outerRelation);

	this->innerRelationGlobalHistogram = new hpcjoin::histograms::GlobalHistogram(this->innerRelationLocalHistogram);
	this->outerRelationGlobalHistogram = new hpcjoin::histograms::GlobalHistogram(this->outerRelationLocalHistogram);

	this->assignment = new hpcjoin::histograms::AssignmentMap(this->numberOfNodes, this->innerRelationGlobalHistogram, this->outerRelationGlobalHistogram);

	this->innerOffsets = new hpcjoin::histograms::OffsetMap(this->numberOfNodes, this->innerRelationLocalHistogram, this->innerRelationGlobalHistogram, this->assignment);
	this->outerOffsets = new hpcjoin::histograms::OffsetMap(this->numberOfNodes, this->outerRelationLocalHistogram, this->outerRelationGlobalHistogram, this->assignment);

}

HistogramComputation::~HistogramComputation() {

	delete this->innerRelationLocalHistogram;
	delete this->outerRelationLocalHistogram;

	delete this->innerRelationGlobalHistogram;
	delete this->outerRelationGlobalHistogram;

	delete this->assignment;

	delete this->innerOffsets;
	delete this->outerOffsets;

}

void HistogramComputation::execute() {

	this->innerRelationLocalHistogram->computeLocalHistogram();
	this->outerRelationLocalHistogram->computeLocalHistogram();

	this->innerRelationGlobalHistogram->computeGlobalHistogram();
	this->outerRelationGlobalHistogram->computeGlobalHistogram();

	this->assignment->computePartitionAssignment();

	this->innerOffsets->computeOffsets();
	this->outerOffsets->computeOffsets();

}


uint32_t* HistogramComputation::getAssignment() {

	return this->assignment->getPartitionAssignment();

}

uint64_t* HistogramComputation::getInnerRelationLocalHistogram() {

	return this->innerRelationLocalHistogram->getLocalHistogram();

}

uint64_t* HistogramComputation::getOuterRelationLocalHistogram() {

	return this->outerRelationLocalHistogram->getLocalHistogram();

}

uint64_t* HistogramComputation::getInnerRelationGlobalHistogram() {

	return this->innerRelationGlobalHistogram->getGlobalHistogram();

}

uint64_t* HistogramComputation::getOuterRelationGlobalHistogram() {

	return this->outerRelationGlobalHistogram->getGlobalHistogram();

}

uint64_t* HistogramComputation::getInnerRelationBaseOffsets() {

	return this->innerOffsets->getBaseOffsets();

}

uint64_t* HistogramComputation::getOuterRelationBaseOffsets() {

	return this->outerOffsets->getBaseOffsets();

}


uint64_t* HistogramComputation::getInnerRelationWriteOffsets() {

	return this->innerOffsets->getAbsoluteWriteOffsets();

}


uint64_t* HistogramComputation::getOuterRelationWriteOffsets() {

	return this->outerOffsets->getAbsoluteWriteOffsets();

}

task_type_t HistogramComputation::getType() {
	return TASK_HISTOGRAM;
}

} /* namespace tasks */
} /* namespace hpcjoin */


