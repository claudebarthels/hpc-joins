/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#include "MergeJoinTask.h"

#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/core/Configuration.h>
#include <math.h>

namespace hpcjoin {
namespace tasks {

MergeJoinTask::MergeJoinTask(hpcjoin::data::CompressedTuple* leftRun, uint64_t leftNumberOfElements, hpcjoin::data::CompressedTuple* rightRun, uint64_t rightNumberOfElements, uint32_t numberOfNodes) {

	this->numberOfNodes = numberOfNodes;
	this->leftRun = leftRun;
	this->leftNumberOfElements = leftNumberOfElements;
	this->rightRun = rightRun;
	this->rightNumberOfElements = rightNumberOfElements;
	this->matchingTuplesCount = 0;

}

MergeJoinTask::~MergeJoinTask() {
}

void MergeJoinTask::execute() {

	hpcjoin::performance::Measurements::startMatchingTask();

	uint64_t i = 0;
	uint64_t j = 0;
	uint64_t matches = 0;

	uint64_t const numR = this->leftNumberOfElements;
	uint64_t const numS = this->rightNumberOfElements;
	uint32_t const shift =  hpcjoin::core::Configuration::PAYLOAD_BITS + log2(numberOfNodes); // TODO add node bits

	hpcjoin::data::CompressedTuple * const rtuples = this->leftRun;
	hpcjoin::data::CompressedTuple * const stuples = this->rightRun;

	while (i < numR && j < numS) {
		if ((rtuples[i].value >> shift) < (stuples[j].value >> shift))
			i++;
		else if ((rtuples[i].value >> shift) > (stuples[j].value >> shift))
			j++;
		else {

			uint64_t jj;
			do {
				jj = j;

				do {
					matches++;
					jj++;
				} while (jj < numS && (rtuples[i].value >> shift) == (stuples[jj].value >> shift));

				i++;

			} while (i < numR && (rtuples[i].value >> shift) == (stuples[j].value >> shift));

			j = jj;

		}
	}

	this->matchingTuplesCount = matches;

	hpcjoin::performance::Measurements::stopMatchingTask();

}

uint64_t MergeJoinTask::getNumberOfMatchingTuples() {
	return this->matchingTuplesCount;
}

} /* namespace tasks */
} /* namespace hpcjoin */

