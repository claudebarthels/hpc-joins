/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#include "SortMergeJoin.h"

#include <mpi.h>
#include <stdlib.h>
#include <string.h>

#include <hpcjoin/tasks/PartitionTask.h>
#include <hpcjoin/tasks/SortTask.h>
#include <hpcjoin/tasks/MergeLevelTask.h>
#include <hpcjoin/tasks/MergeJoinTask.h>
#include <hpcjoin/data/Window.h>
#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/core/Configuration.h>

#define MIN(a,b) (((a)<(b))?(a):(b))

namespace hpcjoin {
namespace operators {

uint64_t SortMergeJoin::RESULT_COUNTER;
std::queue<hpcjoin::tasks::SortTask *> SortMergeJoin::SORT_TASK_QUEUE;
std::vector<hpcjoin::data::CompressedTuple*> SortMergeJoin::INNER_SORTED_RUN_QUEUE;
std::vector<hpcjoin::data::CompressedTuple*> SortMergeJoin::OUTER_SORTED_RUN_QUEUE;
std::vector<uint64_t> SortMergeJoin::INNER_SORTED_RUN_SIZE_QUEUE;
std::vector<uint64_t> SortMergeJoin::OUTER_SORTED_RUN_SIZE_QUEUE;

SortMergeJoin::SortMergeJoin(uint32_t numberOfNodes, uint32_t nodeId, hpcjoin::data::Relation* innerRelation, hpcjoin::data::Relation* outerRelation) {

	this->nodeId = nodeId;
	this->numberOfNodes = numberOfNodes;
	this->innerRelation = innerRelation;
	this->outerRelation = outerRelation;

}

SortMergeJoin::~SortMergeJoin() {
}

void SortMergeJoin::join() {

	/**********************************************************************/

	MPI_Barrier(MPI_COMM_WORLD);
	hpcjoin::performance::Measurements::startJoin();

	/**********************************************************************/

	/**
	 * Partition data
	 */

	hpcjoin::performance::Measurements::startPartitioning();
	hpcjoin::tasks::PartitionTask *partitionTask = new hpcjoin::tasks::PartitionTask(this->innerRelation, this->outerRelation, this->numberOfNodes);
	partitionTask->execute();
	hpcjoin::performance::Measurements::stopPartitioning();

	/**********************************************************************/

	/**
	 * Create windows
	 */

	hpcjoin::performance::Measurements::startWindowAllocation();
	hpcjoin::data::Window *innerWindow = new hpcjoin::data::Window(this->numberOfNodes, partitionTask->innerWindowSize, partitionTask->innerIncomingData,
			partitionTask->innerWriteOffsets, innerRelation);
	hpcjoin::data::Window *outerWindow = new hpcjoin::data::Window(this->numberOfNodes, partitionTask->outerWindowSize, partitionTask->outerIncomingData,
			partitionTask->outerWriteOffsets, outerRelation);
	hpcjoin::performance::Measurements::stopWindowAllocation();

	/**********************************************************************/

	/**
	 * Sort data
	 */

	// Create sort tasks
	hpcjoin::performance::Measurements::startRunPreparations();
	for (uint32_t p = 0; p < numberOfNodes; ++p) {
		uint32_t partitionId = (nodeId + p) % numberOfNodes;

		uint64_t innerPartitionSize = partitionTask->innerHistogram[partitionId];
		hpcjoin::data::CompressedTuple *innerPartitionStart = partitionTask->innerPartitionOutput + partitionTask->innerLocalWriteOffsets[partitionId];

		uint64_t outerPartitionSize = partitionTask->outerHistogram[partitionId];
		hpcjoin::data::CompressedTuple *outerPartitionStart = partitionTask->outerPartitionOutput + partitionTask->outerLocalWriteOffsets[partitionId];

		uint64_t innerProcessCounter = 0;
		while (innerProcessCounter < innerPartitionSize) {
			uint64_t runSize = MIN(innerPartitionSize - innerProcessCounter, hpcjoin::core::Configuration::SORT_RUN_ELEMENT_COUNT);
			hpcjoin::data::CompressedTuple *runStart = innerPartitionStart + innerProcessCounter;
			hpcjoin::tasks::SortTask *sortTask = new hpcjoin::tasks::SortTask(runStart, runSize, innerWindow, partitionId);
			SORT_TASK_QUEUE.push(sortTask);
			innerProcessCounter += runSize;
		}

		uint64_t outerProcessCounter = 0;
		while (outerProcessCounter < outerPartitionSize) {
			uint64_t runSize = MIN(outerPartitionSize - outerProcessCounter, hpcjoin::core::Configuration::SORT_RUN_ELEMENT_COUNT);
			hpcjoin::data::CompressedTuple *runStart = outerPartitionStart + outerProcessCounter;
			hpcjoin::tasks::SortTask *sortTask = new hpcjoin::tasks::SortTask(runStart, runSize, outerWindow, partitionId);
			SORT_TASK_QUEUE.push(sortTask);
			outerProcessCounter += runSize;
		}

	}
	hpcjoin::performance::Measurements::stopRunPreparations();

	hpcjoin::performance::Measurements::startSorting();
	innerWindow->start();
	outerWindow->start();
	// Execute sort tasks
	while (!SORT_TASK_QUEUE.empty()) {
		hpcjoin::tasks::SortTask *sortTask = SORT_TASK_QUEUE.front();
		SORT_TASK_QUEUE.pop();
		sortTask->execute();
		delete sortTask;
	}
	hpcjoin::performance::Measurements::stopSorting();

	hpcjoin::performance::Measurements::startFlush();
	innerWindow->stop();
	outerWindow->stop();
	hpcjoin::performance::Measurements::stopFlush();

	// Free partitioned memory
	delete partitionTask;

	hpcjoin::performance::Measurements::startWaitIncoming();
	MPI_Barrier(MPI_COMM_WORLD);
	hpcjoin::performance::Measurements::stopWaitIncoming();

	/**********************************************************************/

	/**
	 * Merge data
	 */

	hpcjoin::data::CompressedTuple *innerRun = NULL;
	uint64_t innerElementsInRun = 0;
	uint64_t totalInnerReceiveElements = 0;

	hpcjoin::performance::Measurements::startMerging();

	while (innerWindow->getNextRun(&innerRun, &innerElementsInRun)) {
		INNER_SORTED_RUN_QUEUE.push_back(innerRun);
		INNER_SORTED_RUN_SIZE_QUEUE.push_back(innerElementsInRun);
		totalInnerReceiveElements += innerElementsInRun;
	}



	hpcjoin::data::CompressedTuple *outerRun = NULL;
	uint64_t outerElementsInRun = 0;
	uint64_t totalOuterReceiveElements = 0;

	while (outerWindow->getNextRun(&outerRun, &outerElementsInRun)) {
		OUTER_SORTED_RUN_QUEUE.push_back(outerRun);
		OUTER_SORTED_RUN_SIZE_QUEUE.push_back(outerElementsInRun);
		totalOuterReceiveElements += outerElementsInRun;
	}

	uint32_t numberOfInnerRuns = INNER_SORTED_RUN_QUEUE.size();
	uint32_t numberOfOuterRuns = OUTER_SORTED_RUN_QUEUE.size();

	hpcjoin::data::CompressedTuple **inputRuns = &(INNER_SORTED_RUN_QUEUE[0]);
	uint64_t *inputRunSizes = &(INNER_SORTED_RUN_SIZE_QUEUE[0]);

	hpcjoin::data::CompressedTuple *input = innerRelation->getFirstHalfData();
	hpcjoin::data::CompressedTuple *output = innerRelation->getSecondHalfData();

	JOIN_ASSERT(((uint64_t) input ) % 64 == 0, "SortMerge", "Inner input not aligned");
	JOIN_ASSERT(((uint64_t) output ) % 64 == 0, "SortMerge", "Inner output not aligned");

	while (numberOfInnerRuns > 1) {

		// Create task and execute merge
		hpcjoin::tasks::MergeLevelTask *mergingTask = new hpcjoin::tasks::MergeLevelTask(numberOfInnerRuns, inputRuns, inputRunSizes, output);
		mergingTask->execute();

		// Set up next iteration
		numberOfInnerRuns = mergingTask->getNumberOfOutputRuns();
		inputRuns = mergingTask->getOutputRuns();
		inputRunSizes = mergingTask->getOutputRunSizes();
		delete mergingTask;

		// Swap input and output buffer
		hpcjoin::data::CompressedTuple *tmp = output;
		output = input;
		input = tmp;

	}
	hpcjoin::data::CompressedTuple *innerSortedRelation = input;

	inputRuns = &(OUTER_SORTED_RUN_QUEUE[0]);
	inputRunSizes = &(OUTER_SORTED_RUN_SIZE_QUEUE[0]);

	input = outerRelation->getFirstHalfData();
	output = outerRelation->getSecondHalfData();

	JOIN_ASSERT(((uint64_t) input ) % 64 == 0, "SortMerge", "Outer input not aligned");
	JOIN_ASSERT(((uint64_t) output ) % 64 == 0, "SortMerge", "Outer output not aligned");

	while (numberOfOuterRuns > 1) {

		// Create task and execute merge
		hpcjoin::tasks::MergeLevelTask *mergingTask = new hpcjoin::tasks::MergeLevelTask(numberOfOuterRuns, inputRuns, inputRunSizes, output);
		mergingTask->execute();

		// Set up next iteration
		numberOfOuterRuns = mergingTask->getNumberOfOutputRuns();
		inputRuns = mergingTask->getOutputRuns();
		inputRunSizes = mergingTask->getOutputRunSizes();
		delete mergingTask;

		// Swap input and output buffer
		hpcjoin::data::CompressedTuple *tmp = output;
		output = input;
		input = tmp;

	}
	hpcjoin::data::CompressedTuple *outerSortedRelation = input;

	hpcjoin::performance::Measurements::stopMerging();

	/**********************************************************************/

	/**
	 * Merge join both relations
	 */

	hpcjoin::performance::Measurements::startMatching();

	hpcjoin::tasks::MergeJoinTask *mergeJoin = new hpcjoin::tasks::MergeJoinTask(innerSortedRelation, totalInnerReceiveElements, outerSortedRelation,
			totalOuterReceiveElements, numberOfNodes);
	mergeJoin->execute();

	RESULT_COUNTER = mergeJoin->getNumberOfMatchingTuples();

	hpcjoin::performance::Measurements::stopMatching();
	hpcjoin::performance::Measurements::stopJoin();

	MPI_Barrier(MPI_COMM_WORLD);

	/**********************************************************************/

}

} /* namespace operators */
} /* namespace hpcjoin */

