/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include <mpi.h>

#ifdef USE_FOMPI
#include <fompi.h>
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <hpcjoin/data/Relation.h>
#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/operators/SortMergeJoin.h>
#include <hpcjoin/performance/Measurements.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/utils/Thread.h>

int main(int argc, char *argv[]) {

	JOIN_MEM_DEBUG("Main Start");

	JOIN_DEBUG("Main", "Initializing MPI");

	MPI_Init(NULL, NULL);

#ifdef USE_FOMPI
	foMPI_Init(NULL, NULL);
#endif

	JOIN_DEBUG("Main", "Gathering deployment information");

	int32_t numberOfNodes = -1;
	int32_t nodeId = -1;

	MPI_Comm_size(MPI_COMM_WORLD, &numberOfNodes);
	MPI_Comm_rank(MPI_COMM_WORLD, &nodeId);

	JOIN_DEBUG("Main", "There are %d nodes in total", numberOfNodes);
	JOIN_DEBUG("Main", "Node %d is preparing performance counters", nodeId);
	hpcjoin::performance::Measurements::init(nodeId, numberOfNodes, "experiment");

	hpcjoin::performance::Measurements::writeMetaData("NUMNODES", numberOfNodes);
	hpcjoin::performance::Measurements::writeMetaData("NODEID", nodeId);
	hpcjoin::performance::Measurements::writeMetaData("RUNSZ", hpcjoin::core::Configuration::SORT_RUN_ELEMENT_COUNT);

	char hostname[1024];
	memset(hostname, 0, 1024);
	gethostname(hostname, 1023);
	JOIN_DEBUG("main", "Node %d on machine %s\n", nodeId, hostname);
	hpcjoin::performance::Measurements::writeMetaData("HOST", hostname);

	JOIN_ASSERT(numberOfNodes > 0, "Main", "Number of nodes not set");
	JOIN_ASSERT(nodeId >= 0, "Main", "Node id not set");
	JOIN_ASSERT(nodeId < numberOfNodes, "Main", "Node id is not in range");

	JOIN_DEBUG("Main", "Node %d is loading relations", nodeId);

	JOIN_MEM_DEBUG("Init Completed");

	uint64_t globalInnerRelationSize = ((uint64_t) numberOfNodes) * 200000;
	uint64_t globalOuterRelationSize = ((uint64_t) numberOfNodes) * 200000;

	uint64_t localInnerRelationSize =
			(nodeId < numberOfNodes - 1) ? (globalInnerRelationSize / numberOfNodes) : (globalInnerRelationSize - (numberOfNodes - 1) * (globalInnerRelationSize / numberOfNodes));

	uint64_t localOuterRelationSize =
			(nodeId < numberOfNodes - 1) ? (globalOuterRelationSize / numberOfNodes) : (globalOuterRelationSize - (numberOfNodes - 1) * (globalOuterRelationSize / numberOfNodes));

	hpcjoin::data::Relation *innerRelation = new hpcjoin::data::Relation(localInnerRelationSize, globalInnerRelationSize);
	hpcjoin::data::Relation *outerRelation = new hpcjoin::data::Relation(localOuterRelationSize, globalOuterRelationSize);

	hpcjoin::performance::Measurements::writeMetaData("GISZ", globalInnerRelationSize);
	hpcjoin::performance::Measurements::writeMetaData("GOSZ", globalOuterRelationSize);
	hpcjoin::performance::Measurements::writeMetaData("LISZ", localInnerRelationSize);
	hpcjoin::performance::Measurements::writeMetaData("LOSZ", localOuterRelationSize);

	JOIN_MEM_DEBUG("Relations created");

	srand(time(NULL)+nodeId);
	innerRelation->fillUniqueValues(nodeId * (globalInnerRelationSize / numberOfNodes), nodeId * (globalInnerRelationSize / numberOfNodes));
	outerRelation->fillUniqueValues((numberOfNodes - nodeId - 1) * (globalOuterRelationSize / numberOfNodes), nodeId * (globalOuterRelationSize / numberOfNodes));

	if (numberOfNodes > 1) {
		innerRelation->distribute(nodeId, numberOfNodes);
		outerRelation->distribute(nodeId, numberOfNodes);
	}

	JOIN_MEM_DEBUG("Relations distributed");

	JOIN_DEBUG("Main", "Node %d is preparing join", nodeId);

	hpcjoin::operators::SortMergeJoin *sortMergeJoin = new hpcjoin::operators::SortMergeJoin(numberOfNodes, nodeId, innerRelation, outerRelation);

	MPI_Barrier(MPI_COMM_WORLD);

	JOIN_DEBUG("Main", "Node %d is starting join", nodeId);

	sortMergeJoin->join();

	JOIN_DEBUG("Main", "Node %d finished join", nodeId);

	MPI_Barrier(MPI_COMM_WORLD);

	JOIN_DEBUG("Main", "Node %d finalizing measurements", nodeId);

	if (nodeId != hpcjoin::core::Configuration::RESULT_AGGREGATION_NODE) {
		hpcjoin::performance::Measurements::sendMeasurementsToAggregator();
	} else {
		hpcjoin::performance::Measurements::printMeasurements(numberOfNodes, nodeId);
	}
	hpcjoin::performance::Measurements::storeAllMeasurements();

#ifdef USE_FOMPI
	foMPI_Finalize();
#endif

	MPI_Finalize();

	return 0;

}

