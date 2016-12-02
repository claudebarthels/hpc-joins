/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "HashJoin.h"

#include <stdlib.h>

#include <hpcjoin/data/Window.h>
#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/tasks/HistogramComputation.h>
#include <hpcjoin/tasks/NetworkPartitioning.h>
#include <hpcjoin/tasks/LocalPartitioning.h>
#include <hpcjoin/tasks/BuildProbe.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/memory/Pool.h>
#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace operators {

uint64_t HashJoin::RESULT_COUNTER = 0;
std::queue<hpcjoin::tasks::Task *> HashJoin::TASK_QUEUE;

HashJoin::HashJoin(uint32_t numberOfNodes, uint32_t nodeId, hpcjoin::data::Relation *innerRelation, hpcjoin::data::Relation *outerRelation) {

	this->nodeId = nodeId;
	this->numberOfNodes = numberOfNodes;
	this->innerRelation = innerRelation;
	this->outerRelation = outerRelation;

}

HashJoin::~HashJoin() {

}

void HashJoin::join() {

	/**********************************************************************/

	MPI_Barrier(MPI_COMM_WORLD);
	hpcjoin::performance::Measurements::startJoin();

	/**********************************************************************/

	/**
	 * Histogram computation
	 */

	hpcjoin::performance::Measurements::startHistogramComputation();
	hpcjoin::tasks::HistogramComputation *histogramComputation = new hpcjoin::tasks::HistogramComputation(this->numberOfNodes, this->nodeId, this->innerRelation,
			this->outerRelation);
	histogramComputation->execute();
	hpcjoin::performance::Measurements::stopHistogramComputation();
	JOIN_MEM_DEBUG("Histogram phase completed");

	/**********************************************************************/

	/**
	 * Window allocation
	 */

	hpcjoin::performance::Measurements::startWindowAllocation();
	hpcjoin::data::Window *innerWindow = new hpcjoin::data::Window(this->numberOfNodes, this->nodeId, histogramComputation->getAssignment(),
			histogramComputation->getInnerRelationLocalHistogram(), histogramComputation->getInnerRelationGlobalHistogram(), histogramComputation->getInnerRelationBaseOffsets(),
			histogramComputation->getInnerRelationWriteOffsets());

	hpcjoin::data::Window *outerWindow = new hpcjoin::data::Window(this->numberOfNodes, this->nodeId, histogramComputation->getAssignment(),
			histogramComputation->getOuterRelationLocalHistogram(), histogramComputation->getOuterRelationGlobalHistogram(), histogramComputation->getOuterRelationBaseOffsets(),
			histogramComputation->getOuterRelationWriteOffsets());
	hpcjoin::performance::Measurements::stopWindowAllocation();
	JOIN_MEM_DEBUG("Window allocated");

	/**********************************************************************/

	/**
	 * Network partitioning
	 */

	hpcjoin::performance::Measurements::startNetworkPartitioning();
	hpcjoin::tasks::NetworkPartitioning *networkPartitioning = new hpcjoin::tasks::NetworkPartitioning(this->nodeId, this->innerRelation, this->outerRelation, innerWindow,
			outerWindow);
	networkPartitioning->execute();
	hpcjoin::performance::Measurements::stopNetworkPartitioning();
	JOIN_MEM_DEBUG("Network phase completed");

	// OPTIMIZATION Save memory as soon as possible
	//delete this->innerRelation;
	//delete this->outerRelation;
	JOIN_MEM_DEBUG("Input relations deleted");

	/**********************************************************************/

	/**
	 * Main synchronization
	 */

	hpcjoin::performance::Measurements::startWaitingForNetworkCompletion();
	MPI_Barrier(MPI_COMM_WORLD);
	hpcjoin::performance::Measurements::stopWaitingForNetworkCompletion();

	/**********************************************************************/

	/**
	 * Prepare transition
	 */

	hpcjoin::performance::Measurements::startLocalProcessingPreparations();
	if (hpcjoin::core::Configuration::ENABLE_TWO_LEVEL_PARTITIONING) {
		//hpcjoin::memory::Pool::allocate((innerWindow->computeLocalWindowSize() + outerWindow->computeLocalWindowSize())*sizeof(hpcjoin::data::Tuple));
		hpcjoin::memory::Pool::reset();
	}
	// Create initial set of tasks
	uint32_t *assignment = histogramComputation->getAssignment();
	for (uint32_t p = 0; p < hpcjoin::core::Configuration::NETWORK_PARTITIONING_COUNT; ++p) {
		if (assignment[p] == this->nodeId) {
			hpcjoin::data::CompressedTuple *innerRelationPartition = innerWindow->getPartition(p);
			uint64_t innerRelationPartitionSize = innerWindow->getPartitionSize(p);
			hpcjoin::data::CompressedTuple *outerRelationPartition = outerWindow->getPartition(p);
			uint64_t outerRelationPartitionSize = outerWindow->getPartitionSize(p);

			if (hpcjoin::core::Configuration::ENABLE_TWO_LEVEL_PARTITIONING) {
				TASK_QUEUE.push(new hpcjoin::tasks::LocalPartitioning(innerRelationPartitionSize, innerRelationPartition, outerRelationPartitionSize, outerRelationPartition));
			} else {
				TASK_QUEUE.push(new hpcjoin::tasks::BuildProbe(innerRelationPartitionSize, innerRelationPartition, outerRelationPartitionSize, outerRelationPartition));
			}
		}
	}

	// Delete the network related computation
	delete histogramComputation;
	delete networkPartitioning;

	JOIN_MEM_DEBUG("Local phase prepared");

	hpcjoin::performance::Measurements::stopLocalProcessingPreparations();

	/**********************************************************************/

	/**
	 * Local processing
	 */

	// OPTIMIZATION Delete window as soon as possible
	bool windowsDeleted = false;

	// Execute tasks
	hpcjoin::performance::Measurements::startLocalProcessing();
	while (TASK_QUEUE.size() > 0) {

		hpcjoin::tasks::Task *task = TASK_QUEUE.front();
		TASK_QUEUE.pop();

		// OPTIMIZATION When second partitioning pass is completed, windows are no longer required
		if (hpcjoin::core::Configuration::ENABLE_TWO_LEVEL_PARTITIONING && windowsDeleted) {
			if (task->getType() == TASK_BUILD_PROBE) {
				delete innerWindow;
				delete outerWindow;
				windowsDeleted = true;
			}
		}

		task->execute();
		delete task;

	}
	hpcjoin::performance::Measurements::stopLocalProcessing();

	JOIN_MEM_DEBUG("Local phase completed");

	/**********************************************************************/

	hpcjoin::performance::Measurements::stopJoin();

	// OPTIMIZATION (see above)
	if (!windowsDeleted) {
		delete innerWindow;
		delete outerWindow;
	}

}

} /* namespace operators */
} /* namespace hpcjoin */
