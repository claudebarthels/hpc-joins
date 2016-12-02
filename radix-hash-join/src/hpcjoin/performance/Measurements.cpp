/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "Measurements.h"

#define MSG_TAG_RESULTS 154895

#include <hpcjoin/operators/HashJoin.h>
#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h>
#include <papi.h>
#include <sys/stat.h>

struct timeval hpcjoin::performance::Measurements::joinStart;
struct timeval hpcjoin::performance::Measurements::joinStop;
struct timeval hpcjoin::performance::Measurements::histogramComputationStart;
struct timeval hpcjoin::performance::Measurements::histogramComputationStop;
struct timeval hpcjoin::performance::Measurements::networkPartitioningStart;
struct timeval hpcjoin::performance::Measurements::networkPartitioningStop;
struct timeval hpcjoin::performance::Measurements::localProcessingStart;
struct timeval hpcjoin::performance::Measurements::localProcessingStop;

struct timeval hpcjoin::performance::Measurements::windowAllocationStart;
struct timeval hpcjoin::performance::Measurements::windowAllocationStop;
struct timeval hpcjoin::performance::Measurements::waitingForNetworkCompletionStart;
struct timeval hpcjoin::performance::Measurements::waitingForNetworkCompletionStop;
struct timeval hpcjoin::performance::Measurements::localProcessingPreparationsStart;
struct timeval hpcjoin::performance::Measurements::localProcessingPreparationsStop;

struct timeval hpcjoin::performance::Measurements::histogramLocalHistogramComputationStart;
struct timeval hpcjoin::performance::Measurements::histogramLocalHistogramComputationStop;
struct timeval hpcjoin::performance::Measurements::histogramGlobalHistogramComputationStart;
struct timeval hpcjoin::performance::Measurements::histogramGlobalHistogramComputationStop;
struct timeval hpcjoin::performance::Measurements::histogramAssignmentStart;
struct timeval hpcjoin::performance::Measurements::histogramAssignmentStop;
struct timeval hpcjoin::performance::Measurements::histogramOffsetComputationStart;
struct timeval hpcjoin::performance::Measurements::histogramOffsetComputationStop;

struct timeval hpcjoin::performance::Measurements::networkPartitioningMemoryAllocationStart;
struct timeval hpcjoin::performance::Measurements::networkPartitioningMemoryAllocationStop;
struct timeval hpcjoin::performance::Measurements::networkPartitioningMainPartitioningStart;
struct timeval hpcjoin::performance::Measurements::networkPartitioningMainPartitioningStop;
struct timeval hpcjoin::performance::Measurements::networkPartitioningFlushPartitioningStart;
struct timeval hpcjoin::performance::Measurements::networkPartitioningFlushPartitioningStop;
struct timeval hpcjoin::performance::Measurements::networkPartitioningWindowPutStart;
struct timeval hpcjoin::performance::Measurements::networkPartitioningWindowPutStop;
struct timeval hpcjoin::performance::Measurements::networkPartitioningWindowWaitStart;
struct timeval hpcjoin::performance::Measurements::networkPartitioningWindowWaitStop;

struct timeval hpcjoin::performance::Measurements::localPartitioningTaskStart;
struct timeval hpcjoin::performance::Measurements::localPartitioningTaskStop;
struct timeval hpcjoin::performance::Measurements::localPartitioningHistogramComputationStart;
struct timeval hpcjoin::performance::Measurements::localPartitioningHistogramComputationStop;
struct timeval hpcjoin::performance::Measurements::localPartitioningOffsetComputationStart;
struct timeval hpcjoin::performance::Measurements::localPartitioningOffsetComputationStop;
struct timeval hpcjoin::performance::Measurements::localPartitioningMemoryAllocationStart;
struct timeval hpcjoin::performance::Measurements::localPartitioningMemoryAllocationStop;
struct timeval hpcjoin::performance::Measurements::localPartitioningPartitioningStart;
struct timeval hpcjoin::performance::Measurements::localPartitioningPartitioningStop;

struct timeval hpcjoin::performance::Measurements::buildProbeTaskStart;
struct timeval hpcjoin::performance::Measurements::buildProbeTaskStop;
struct timeval hpcjoin::performance::Measurements::buildProbeMemoryAllocationStart;
struct timeval hpcjoin::performance::Measurements::buildProbeMemoryAllocationStop;
struct timeval hpcjoin::performance::Measurements::buildProbeBuildStart;
struct timeval hpcjoin::performance::Measurements::buildProbeBuildStop;
struct timeval hpcjoin::performance::Measurements::buildProbeProbeStart;
struct timeval hpcjoin::performance::Measurements::buildProbeProbeStop;

uint64_t ** hpcjoin::performance::Measurements::serizlizedResults;

namespace hpcjoin {
namespace performance {

/************************************************************/

uint64_t Measurements::totalCycles;
uint64_t Measurements::totalTime;
uint64_t Measurements::phaseTimes[3];

void Measurements::startJoin() {
	gettimeofday(&joinStart, NULL);
	int event = PAPI_TOT_CYC;
	PAPI_start_counters(&event, 1);
}

void Measurements::stopJoin() {
	gettimeofday(&joinStop, NULL);
	totalTime = timeDiff(joinStop, joinStart);

	long_long value;
	int ret = 0;
	if ((ret = PAPI_stop_counters(&value, 1)) != PAPI_OK) {
		value = 0;
	}

	totalCycles = value;
}

void Measurements::startHistogramComputation() {
	gettimeofday(&histogramComputationStart, NULL);
}

void Measurements::stopHistogramComputation() {
	gettimeofday(&histogramComputationStop, NULL);
	phaseTimes[0] = timeDiff(histogramComputationStop, histogramComputationStart);
}

void Measurements::startNetworkPartitioning() {
	gettimeofday(&networkPartitioningStart, NULL);
}

void Measurements::stopNetworkPartitioning() {
	gettimeofday(&networkPartitioningStop, NULL);
	phaseTimes[1] = timeDiff(networkPartitioningStop, networkPartitioningStart);
}

void Measurements::startLocalProcessing() {
	gettimeofday(&localProcessingStart, NULL);
}

void Measurements::stopLocalProcessing() {
	gettimeofday(&localProcessingStop, NULL);
	phaseTimes[2] = timeDiff(localProcessingStop, localProcessingStart);
}

void Measurements::storePhaseData() {
	fprintf(performanceOutputFile, "CTOTAL\t%lu\tcycles\n", totalCycles);
	fprintf(performanceOutputFile, "JTOTAL\t%lu\tus\n", totalTime);
	fprintf(performanceOutputFile, "JHIST\t%lu\tus\n", phaseTimes[0]);
	fprintf(performanceOutputFile, "JMPI\t%lu\tus\n", phaseTimes[1]);
	fprintf(performanceOutputFile, "JPROC\t%lu\tus\n", phaseTimes[2]);
}

/************************************************************/

uint64_t Measurements::specialTimes[3];

void Measurements::startWindowAllocation() {
	gettimeofday(&windowAllocationStart, NULL);
}

void Measurements::stopWindowAllocation() {
	gettimeofday(&windowAllocationStop, NULL);
	specialTimes[0] = timeDiff(windowAllocationStop, windowAllocationStart);
}

void Measurements::startWaitingForNetworkCompletion() {
	gettimeofday(&waitingForNetworkCompletionStart, NULL);
}

void Measurements::stopWaitingForNetworkCompletion() {
	gettimeofday(&waitingForNetworkCompletionStop, NULL);
	specialTimes[1] = timeDiff(waitingForNetworkCompletionStop, waitingForNetworkCompletionStart);
}

void Measurements::startLocalProcessingPreparations() {
	gettimeofday(&localProcessingPreparationsStart, NULL);
}

void Measurements::stopLocalProcessingPreparations() {
	gettimeofday(&localProcessingPreparationsStop, NULL);
	specialTimes[2] = timeDiff(localProcessingPreparationsStop, localProcessingPreparationsStart);
}

void Measurements::storeSpecialData() {
	fprintf(performanceOutputFile, "SWINALLOC\t%lu\tus\n", specialTimes[0]);
	fprintf(performanceOutputFile, "SNETCOMPL\t%lu\tus\n", specialTimes[1]);
	fprintf(performanceOutputFile, "SLOCPREP\t%lu\tus\n", specialTimes[2]);
}

/************************************************************/

uint64_t Measurements::histogramLocalHistogramComputationIdx = 0;
uint64_t Measurements::histogramLocalHistogramComputationTimes[2];
uint64_t Measurements::histogramLocalHistogramComputationElements[2];

uint64_t Measurements::histogramGlobalHistogramComputationIdx = 0;
uint64_t Measurements::histogramGlobalHistogramComputationTimes[2];

uint64_t Measurements::histogramAssignmentComputationTime;

uint64_t Measurements::histogramOffsetComputationIdx = 0;
uint64_t Measurements::histogramOffsetComputationTimes[2];

void Measurements::startHistogramLocalHistogramComputation() {
	gettimeofday(&histogramLocalHistogramComputationStart, NULL);
}

void Measurements::stopHistogramLocalHistogramComputation(uint64_t numberOfElemenets) {
	gettimeofday(&histogramLocalHistogramComputationStop, NULL);
	uint64_t time = timeDiff(histogramLocalHistogramComputationStop, histogramLocalHistogramComputationStart);

	JOIN_ASSERT(histogramLocalHistogramComputationIdx < 2, "Performance", "Index out of bounds");
	histogramLocalHistogramComputationTimes[histogramLocalHistogramComputationIdx] = time;
	histogramLocalHistogramComputationElements[histogramLocalHistogramComputationIdx] = numberOfElemenets;
	++histogramLocalHistogramComputationIdx;
}

void Measurements::startHistogramGlobalHistogramComputation() {
	gettimeofday(&histogramGlobalHistogramComputationStart, NULL);
}

void Measurements::stopHistogramGlobalHistogramComputation() {
	gettimeofday(&histogramGlobalHistogramComputationStop, NULL);
	uint64_t time = timeDiff(histogramGlobalHistogramComputationStop, histogramGlobalHistogramComputationStart);

	JOIN_ASSERT(histogramGlobalHistogramComputationIdx < 2, "Performance", "Index out of bounds");
	histogramGlobalHistogramComputationTimes[histogramGlobalHistogramComputationIdx] = time;
	++histogramGlobalHistogramComputationIdx;
}

void Measurements::startHistogramAssignmentComputation() {
	gettimeofday(&histogramAssignmentStart, NULL);
}

void Measurements::stopHistogramAssignmentComputation() {
	gettimeofday(&histogramAssignmentStop, NULL);
	histogramAssignmentComputationTime = timeDiff(histogramAssignmentStop, histogramAssignmentStop);
}

void Measurements::startHistogramOffsetComputation() {
	gettimeofday(&histogramOffsetComputationStart, NULL);
}

void Measurements::stopHistogramOffsetComputation() {
	gettimeofday(&histogramOffsetComputationStop, NULL);
	uint64_t time = timeDiff(histogramOffsetComputationStop, histogramOffsetComputationStart);

	JOIN_ASSERT(histogramOffsetComputationIdx < 2, "Performance", "Index out of bounds");
	histogramOffsetComputationTimes[histogramOffsetComputationIdx] = time;
	++histogramOffsetComputationIdx;
}

void Measurements::storeHistogramComputationData() {
	fprintf(performanceOutputFile, "HILOCAL\t%lu\tus\n", histogramLocalHistogramComputationTimes[0]);
	fprintf(performanceOutputFile, "HOLOCELEM\t%lu\ttuples\n", histogramLocalHistogramComputationElements[0]);
	fprintf(performanceOutputFile, "HILOCRATE\t%.2f\tMbytes/sec\n",
			((double) histogramLocalHistogramComputationElements[0] * sizeof(hpcjoin::data::Tuple)) / histogramLocalHistogramComputationTimes[0]);
	fprintf(performanceOutputFile, "HOLOCAL\t%lu\tus\n", histogramLocalHistogramComputationTimes[1]);
	fprintf(performanceOutputFile, "HOLOCELEM\t%lu\ttuples\n", histogramLocalHistogramComputationElements[1]);
	fprintf(performanceOutputFile, "HOLOCRATE\t%.2f\tMbytes/sec\n",
			((double) histogramLocalHistogramComputationElements[1] * sizeof(hpcjoin::data::Tuple)) / histogramLocalHistogramComputationTimes[1]);
	fprintf(performanceOutputFile, "HIGLOBAL\t%lu\tus\n", histogramGlobalHistogramComputationTimes[0]);
	fprintf(performanceOutputFile, "HOGLOBAL\t%lu\tus\n", histogramGlobalHistogramComputationTimes[1]);
	fprintf(performanceOutputFile, "HASSIGN\t%lu\tus\n", histogramAssignmentComputationTime);
	fprintf(performanceOutputFile, "HIOFFCOMP\t%lu\tus\n", histogramOffsetComputationTimes[0]);
	fprintf(performanceOutputFile, "HOOFFCOMP\t%lu\tus\n", histogramOffsetComputationTimes[1]);
}

/************************************************************/

uint64_t Measurements::networkPartitioningMemoryAllocationIdx = 0;
uint64_t Measurements::networkPartitioningMemoryAllocationTimes[2];

uint64_t Measurements::networkPartitioningMainPartitioningIdx = 0;
uint64_t Measurements::networkPartitioningMainPartitioningTimes[2];

uint64_t Measurements::networkPartitioningFlushPartitioningIdx = 0;
uint64_t Measurements::networkPartitioningFlushPartitioningTimes[2];

uint64_t Measurements::networkPartitioningWindowPutCount = 0;
uint64_t Measurements::networkPartitioningWindowPutTimeSum = 0;

uint64_t Measurements::networkPartitioningWindowWaitCount = 0;
uint64_t Measurements::networkPartitioningWindowWaitTimeSum = 0;

void Measurements::startNetworkPartitioningMemoryAllocation() {
	gettimeofday(&networkPartitioningMemoryAllocationStart, NULL);
}

void Measurements::stopNetworkPartitioningMemoryAllocation(uint64_t numberOfElemenets) {
	gettimeofday(&networkPartitioningMemoryAllocationStop, NULL);
	uint64_t time = timeDiff(networkPartitioningMemoryAllocationStop, networkPartitioningMemoryAllocationStart);

	JOIN_ASSERT(networkPartitioningMemoryAllocationIdx < 2, "Performance", "Index out of bounds");
	networkPartitioningMemoryAllocationTimes[networkPartitioningMemoryAllocationIdx] = time;
	++networkPartitioningMemoryAllocationIdx;
}

void Measurements::startNetworkPartitioningMainPartitioning() {
	gettimeofday(&networkPartitioningMainPartitioningStart, NULL);
}

void Measurements::stopNetworkPartitioningMainPartitioning(uint64_t numberOfElemenets) {
	gettimeofday(&networkPartitioningMainPartitioningStop, NULL);
	uint64_t time = timeDiff(networkPartitioningMainPartitioningStop, networkPartitioningMainPartitioningStart);

	JOIN_ASSERT(networkPartitioningMainPartitioningIdx < 2, "Performance", "Index out of bounds");
	networkPartitioningMainPartitioningTimes[networkPartitioningMainPartitioningIdx] = time;
	++networkPartitioningMainPartitioningIdx;
}

void Measurements::startNetworkPartitioningFlushPartitioning() {
	gettimeofday(&networkPartitioningFlushPartitioningStart, NULL);
}

void Measurements::stopNetworkPartitioningFlushPartitioning() {
	gettimeofday(&networkPartitioningFlushPartitioningStop, NULL);
	uint64_t time = timeDiff(networkPartitioningFlushPartitioningStop, networkPartitioningFlushPartitioningStart);

	JOIN_ASSERT(networkPartitioningFlushPartitioningIdx < 2, "Performance", "Index out of bounds");
	networkPartitioningFlushPartitioningTimes[networkPartitioningFlushPartitioningIdx] = time;
	++networkPartitioningFlushPartitioningIdx;
}

void Measurements::startNetworkPartitioningWindowPut() {
	gettimeofday(&networkPartitioningWindowPutStart, NULL);
}

void Measurements::stopNetworkPartitioningWindowPut() {
	gettimeofday(&networkPartitioningWindowPutStop, NULL);
	uint64_t time = timeDiff(networkPartitioningWindowPutStop, networkPartitioningWindowPutStart);
	networkPartitioningWindowPutTimeSum += time;
	++networkPartitioningWindowPutCount;
}

void Measurements::startNetworkPartitioningWindowWait() {
	gettimeofday(&networkPartitioningWindowWaitStart, NULL);
}

void Measurements::stopNetworkPartitioningWindowWait() {
	gettimeofday(&networkPartitioningWindowWaitStop, NULL);
	uint64_t time = timeDiff(networkPartitioningWindowWaitStop, networkPartitioningWindowWaitStart);
	networkPartitioningWindowWaitTimeSum += time;
	++networkPartitioningWindowWaitCount;
}

void Measurements::storeNetworkPartitioningData() {
	fprintf(performanceOutputFile, "MIMEMALLOC\t%lu\tus\n", networkPartitioningMemoryAllocationTimes[0]);
	fprintf(performanceOutputFile, "MIMAINPART\t%lu\tus\n", networkPartitioningMainPartitioningTimes[0]);
	fprintf(performanceOutputFile, "MIFLUSHPART\t%lu\tus\n", networkPartitioningFlushPartitioningTimes[0]);
	fprintf(performanceOutputFile, "MOMEMALLOC\t%lu\tus\n", networkPartitioningMemoryAllocationTimes[1]);
	fprintf(performanceOutputFile, "MOMAINPART\t%lu\tus\n", networkPartitioningMainPartitioningTimes[1]);
	fprintf(performanceOutputFile, "MOFLUSHPART\t%lu\tus\n", networkPartitioningFlushPartitioningTimes[1]);
	fprintf(performanceOutputFile, "MWINPUT\t%lu\tus\n", networkPartitioningWindowPutTimeSum);
	fprintf(performanceOutputFile, "MWINPUTCNT\t%lu\tcalls\n", networkPartitioningWindowPutCount);
	fprintf(performanceOutputFile, "MWINWAIT\t%lu\tus\n", networkPartitioningWindowWaitTimeSum);
	fprintf(performanceOutputFile, "MWINWAITCNT\t%lu\tcalls\n", networkPartitioningWindowWaitCount);
}

/************************************************************/

uint64_t Measurements::localPartitioningTaskCount = 0;
uint64_t Measurements::localPartitioningTaskTimeSum = 0;

uint64_t Measurements::localPartitioningHistogramComputationCount = 0;
uint64_t Measurements::localPartitioningHistogramComputationTimeSum = 0;
uint64_t Measurements::localPartitioningHistogramComputationElementSum = 0;

uint64_t Measurements::localPartitioningOffsetComputationCount = 0;
uint64_t Measurements::localPartitioningOffsetComputationTimeSum = 0;

uint64_t Measurements::localPartitioningMemoryAllocationCount = 0;
uint64_t Measurements::localPartitioningMemoryAllocationTimeSum = 0;
uint64_t Measurements::localPartitioningMemoryAllocationSizeSum = 0;

uint64_t Measurements::localPartitioningPartitioningCount = 0;
uint64_t Measurements::localPartitioningPartitioningTimeSum = 0;
uint64_t Measurements::localPartitioningPartitioningElementSum = 0;

void Measurements::startLocalPartitioningTask() {
	gettimeofday(&localPartitioningTaskStart, NULL);
}

void Measurements::stopLocalPartitioningTask() {
	gettimeofday(&localPartitioningTaskStop, NULL);
	uint64_t time = timeDiff(localPartitioningTaskStop, localPartitioningTaskStart);
	localPartitioningTaskTimeSum += time;
	++localPartitioningTaskCount;
}

void Measurements::startLocalPartitioningHistogramComputation() {
	gettimeofday(&localPartitioningHistogramComputationStart, NULL);
}

void Measurements::stopLocalPartitioningHistogramComputation(uint64_t numberOfElemenets) {
	gettimeofday(&localPartitioningHistogramComputationStop, NULL);
	uint64_t time = timeDiff(localPartitioningHistogramComputationStop, localPartitioningHistogramComputationStart);
	localPartitioningHistogramComputationTimeSum += time;
	++localPartitioningHistogramComputationCount;
	localPartitioningHistogramComputationElementSum += numberOfElemenets;
}

void Measurements::startLocalPartitioningOffsetComputation() {
	gettimeofday(&localPartitioningOffsetComputationStart, NULL);
}

void Measurements::stopLocalPartitioningOffsetComputation() {
	gettimeofday(&localPartitioningOffsetComputationStop, NULL);
	uint64_t time = timeDiff(localPartitioningOffsetComputationStop, localPartitioningOffsetComputationStart);
	localPartitioningOffsetComputationTimeSum += time;
	++localPartitioningOffsetComputationCount;
}

void Measurements::startLocalPartitioningMemoryAllocation() {
	gettimeofday(&localPartitioningMemoryAllocationStart, NULL);
}

void Measurements::stopLocalPartitioningMemoryAllocation(uint64_t bufferSize) {
	gettimeofday(&localPartitioningMemoryAllocationStop, NULL);
	uint64_t time = timeDiff(localPartitioningMemoryAllocationStop, localPartitioningMemoryAllocationStart);
	localPartitioningMemoryAllocationTimeSum += time;
	++localPartitioningMemoryAllocationCount;
	localPartitioningMemoryAllocationSizeSum += bufferSize;
}

void Measurements::startLocalPartitioningPartitioning() {
	gettimeofday(&localPartitioningPartitioningStart, NULL);
}

void Measurements::stopLocalPartitioningPartitioning(uint64_t numberOfElemenets) {
	gettimeofday(&localPartitioningPartitioningStop, NULL);
	uint64_t time = timeDiff(localPartitioningPartitioningStop, localPartitioningPartitioningStart);
	localPartitioningPartitioningTimeSum += time;
	++localPartitioningPartitioningCount;
	localPartitioningPartitioningElementSum += numberOfElemenets;
}

void Measurements::storeLocalPartitioningData() {
	fprintf(performanceOutputFile, "LPTASKTIME\t%lu\tus\n", localPartitioningTaskTimeSum);
	fprintf(performanceOutputFile, "LPTASKCOUNT\t%lu\ttasks\n", localPartitioningTaskCount);
	fprintf(performanceOutputFile, "LPHISTCOMP\t%lu\tus\n", localPartitioningHistogramComputationTimeSum);
	fprintf(performanceOutputFile, "LPHISTELEM\t%lu\ttuples\n", localPartitioningHistogramComputationElementSum);
	fprintf(performanceOutputFile, "LPOFFSET\t%lu\tus\n", localPartitioningOffsetComputationTimeSum);
	fprintf(performanceOutputFile, "LPMEMALLOC\t%lu\tus\n", localPartitioningMemoryAllocationTimeSum);
	fprintf(performanceOutputFile, "LPMEMSIZE\t%lu\tbytes\n", localPartitioningMemoryAllocationSizeSum);
	fprintf(performanceOutputFile, "LPPART\t%lu\tus\n", localPartitioningPartitioningTimeSum);
	fprintf(performanceOutputFile, "LPELEMENTS\t%lu\ttuples\n", localPartitioningPartitioningElementSum);
}

/************************************************************/

uint64_t Measurements::buildProbeTaskCount = 0;
uint64_t Measurements::buildProbeTaskTimeSum = 0;

uint64_t Measurements::buildProbeMemoryAllocationCount = 0;
uint64_t Measurements::buildProbeMemoryAllocationTimeSum = 0;
uint64_t Measurements::buildProbeMemoryAllocationSizeSum = 0;

uint64_t Measurements::buildProbeBuildCount = 0;
uint64_t Measurements::buildProbeBuildTimeSum = 0;
uint64_t Measurements::buildProbeBuildElementSum = 0;

uint64_t Measurements::buildProbeProbeCount = 0;
uint64_t Measurements::buildProbeProbeTimeSum = 0;
uint64_t Measurements::buildProbeProbeElementSum = 0;

void Measurements::startBuildProbeTask() {
	gettimeofday(&buildProbeTaskStart, NULL);
}

void Measurements::stopBuildProbeTask() {
	gettimeofday(&buildProbeTaskStop, NULL);
	uint64_t time = timeDiff(buildProbeTaskStop, buildProbeTaskStart);
	buildProbeTaskTimeSum += time;
	++buildProbeTaskCount;
}

void Measurements::startBuildProbeMemoryAllocation() {
	gettimeofday(&buildProbeMemoryAllocationStart, NULL);
}

void Measurements::stopBuildProbeMemoryAllocation(uint64_t bufferSize) {
	gettimeofday(&buildProbeMemoryAllocationStop, NULL);
	uint64_t time = timeDiff(buildProbeMemoryAllocationStop, buildProbeMemoryAllocationStart);
	buildProbeMemoryAllocationTimeSum += time;
	++buildProbeMemoryAllocationCount;
	buildProbeMemoryAllocationSizeSum += bufferSize;
}

void Measurements::startBuildProbeBuild() {
	gettimeofday(&buildProbeBuildStart, NULL);
}

void Measurements::stopBuildProbeBuild(uint64_t numberOfElemenets) {
	gettimeofday(&buildProbeBuildStop, NULL);
	uint64_t time = timeDiff(buildProbeBuildStop, buildProbeBuildStart);
	buildProbeBuildTimeSum += time;
	++buildProbeBuildCount;
	buildProbeBuildElementSum += numberOfElemenets;
}

void Measurements::startBuildProbeProbe() {
	gettimeofday(&buildProbeProbeStart, NULL);
}

void Measurements::stopBuildProbeProbe(uint64_t numberOfElemenets) {
	gettimeofday(&buildProbeProbeStop, NULL);
	uint64_t time = timeDiff(buildProbeProbeStop, buildProbeProbeStart);
	buildProbeProbeTimeSum += time;
	++buildProbeProbeCount;
	buildProbeProbeElementSum += numberOfElemenets;
}

void Measurements::storeBuildProbeData() {
	fprintf(performanceOutputFile, "BPTASKTIME\t%lu\tus\n", buildProbeTaskTimeSum);
	fprintf(performanceOutputFile, "BPTASKCOUNT\t%lu\ttasks\n", buildProbeTaskCount);
	fprintf(performanceOutputFile, "BPMEMALLOC\t%lu\tus\n", buildProbeMemoryAllocationTimeSum);
	fprintf(performanceOutputFile, "BPMEMSIZE\t%lu\tbytes\n", buildProbeMemoryAllocationSizeSum);
	fprintf(performanceOutputFile, "BPBUILD\t%lu\tus\n", buildProbeBuildTimeSum);
	fprintf(performanceOutputFile, "BPBUILDELEM\t%lu\ttuples\n", buildProbeBuildElementSum);
	fprintf(performanceOutputFile, "BPPROBE\t%lu\tus\n", buildProbeProbeTimeSum);
	fprintf(performanceOutputFile, "BPPROBEELEM\t%lu\ttuples\n", buildProbeProbeElementSum);
}

/************************************************************/

#define NUM_OF_RESULT_ELEMENTS 10

uint64_t* Measurements::serializeResults() {

	uint64_t *result = (uint64_t *) calloc(NUM_OF_RESULT_ELEMENTS, sizeof(uint64_t));

	result[0] = hpcjoin::operators::HashJoin::RESULT_COUNTER;
	result[1] = totalTime;
	result[2] = phaseTimes[0];
	result[3] = phaseTimes[1];
	result[4] = phaseTimes[2];
	result[5] = specialTimes[0];
	result[6] = specialTimes[1];
	result[7] = specialTimes[2];
	result[8] = localPartitioningTaskTimeSum;
	result[9] = buildProbeTaskTimeSum;

	return result;

}

void Measurements::sendMeasurementsToAggregator() {

	uint32_t resultSize = NUM_OF_RESULT_ELEMENTS * sizeof(uint64_t);
	uint64_t *results = serializeResults();
	MPI_Send(results, resultSize, MPI_BYTE, hpcjoin::core::Configuration::RESULT_AGGREGATION_NODE, MSG_TAG_RESULTS, MPI_COMM_WORLD);

}

void Measurements::receiveAllMeasurments(uint32_t numberOfNodes, uint32_t nodeId) {

	serizlizedResults = new uint64_t*[numberOfNodes];
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		if (n == nodeId) {
			serizlizedResults[n] = serializeResults();
		} else {
			uint32_t resultSize = NUM_OF_RESULT_ELEMENTS * sizeof(uint64_t);
			serizlizedResults[n] = (uint64_t *) calloc(resultSize, 1);
			MPI_Recv(serizlizedResults[n], resultSize, MPI_BYTE, n, MSG_TAG_RESULTS, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	}

}

void Measurements::printMeasurements(uint32_t numberOfNodes, uint32_t nodeId) {

	JOIN_ASSERT(nodeId == hpcjoin::core::Configuration::RESULT_AGGREGATION_NODE, "Measurements", "Only coordinator should print results");

	receiveAllMeasurments(numberOfNodes, nodeId);

	printf("[RESULTS] Tuples:\t");
	uint64_t totalNumberOfTuples = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t numberOfTuples = serizlizedResults[n][0];
		printf("%lu\t", numberOfTuples);
		totalNumberOfTuples += numberOfTuples;
	}
	printf("\n");

	printf("[RESULTS] Join:\t");
	uint64_t averageJoinTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t joinTime = serizlizedResults[n][1];
		printf("%.3f\t", ((double) joinTime) / 1000);
		averageJoinTime += joinTime;
	}
	printf("\n");
	averageJoinTime /= numberOfNodes;

	printf("[RESULTS] Histogram:\t");\
	uint64_t averageHistogramTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t histogramTime = serizlizedResults[n][2];
		printf("%.3f\t", ((double) histogramTime) / 1000);
		averageHistogramTime += histogramTime;
	}
	printf("\n");
	averageHistogramTime /= numberOfNodes;

	printf("[RESULTS] Network:\t");
	uint64_t averageNetworkTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t networkTime = serizlizedResults[n][3];
		printf("%.3f\t", ((double) networkTime) / 1000);
		averageNetworkTime += networkTime;
	}
	printf("\n");
	averageNetworkTime /= numberOfNodes;

	printf("[RESULTS] Local:\t");
	uint64_t averageLocalTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t localTime = serizlizedResults[n][4];
		printf("%.3f\t", ((double) localTime) / 1000);
		averageLocalTime += localTime;
	}
	printf("\n");
	averageLocalTime /= numberOfNodes;

	printf("[RESULTS] WinAlloc:\t");
	uint64_t averageWindowAllocTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t windowAllocTime = serizlizedResults[n][5];
		printf("%.3f\t", ((double) windowAllocTime) / 1000);
		averageWindowAllocTime += windowAllocTime;
	}
	printf("\n");
	averageWindowAllocTime /= numberOfNodes;

	printf("[RESULTS] PartWait:\t");
	uint64_t averagePartitionWaitingTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t partitionWaitingTime = serizlizedResults[n][6];
		printf("%.3f\t", ((double) partitionWaitingTime) / 1000);
		averagePartitionWaitingTime += partitionWaitingTime;
	}
	printf("\n");
	averagePartitionWaitingTime /= numberOfNodes;

	printf("[RESULTS] LocalPrep:\t");
	uint64_t averageLocalPreparationTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t localPreparationTime = serizlizedResults[n][7];
		printf("%.3f\t", ((double) localPreparationTime) / 1000);
		averageLocalPreparationTime += localPreparationTime;
	}
	printf("\n");
	averageLocalPreparationTime /= numberOfNodes;

	printf("[RESULTS] LocalPart:\t");
	uint64_t averageLocalPartitioningTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t localPartitioningTime = serizlizedResults[n][8];
		printf("%.3f\t", ((double) localPartitioningTime) / 1000);
		averageLocalPartitioningTime += localPartitioningTime;
	}
	printf("\n");
	averageLocalPartitioningTime /= numberOfNodes;

	printf("[RESULTS] LocalBP:\t");
	uint64_t averageLocalBuildProbeTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t localBuildProbeTime = serizlizedResults[n][9];
		printf("%.3f\t", ((double) localBuildProbeTime) / 1000);
		averageLocalBuildProbeTime += localBuildProbeTime;
	}
	printf("\n");
	averageLocalBuildProbeTime /= numberOfNodes;

	printf("[RESULTS] Summary:\t%lu\t%.3f\t%.3f\t%.3f\t%.3f\n", totalNumberOfTuples, ((double) averageJoinTime) / 1000, ((double) averageHistogramTime) / 1000,
			((double) averageNetworkTime) / 1000, ((double) averageLocalTime) / 1000);

}

FILE * Measurements::performanceOutputFile = NULL;
FILE * Measurements::metaDataOutputFile = NULL;

void Measurements::init(uint32_t nodeId, uint32_t numberOfNodes, std::string tag) {

	char cwdPath[1024];
	memset(cwdPath, 0, 1024);

	char experimentFullPath[1024];
	memset(experimentFullPath, 0, 1024);

	char performanceFullPath[1024];
	memset(performanceFullPath, 0, 1024);

	char metaDataFullPath[1024];
	memset(metaDataFullPath, 0, 1024);

	char *unused __attribute__((unused));
	unused = getcwd(cwdPath, sizeof(cwdPath));

	uint64_t experimentId = 0;
	if (nodeId == hpcjoin::core::Configuration::RESULT_AGGREGATION_NODE) {
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		experimentId = currentTime.tv_sec * 1000000L + currentTime.tv_usec;
		sprintf(experimentFullPath, "%s/%s-%d-%lu", cwdPath, tag.c_str(), numberOfNodes, experimentId);
		mkdir(experimentFullPath, S_IRUSR | S_IWUSR | S_IXUSR);
	}

	MPI_Bcast(&experimentId, 1, MPI_UINT64_T, hpcjoin::core::Configuration::RESULT_AGGREGATION_NODE, MPI_COMM_WORLD);

	sprintf(performanceFullPath, "%s/%s-%d-%lu/%d.perf", cwdPath, tag.c_str(), numberOfNodes, experimentId, nodeId);
	performanceOutputFile = fopen(performanceFullPath, "w");

	sprintf(metaDataFullPath, "%s/%s-%d-%lu/%d.info", cwdPath, tag.c_str(), numberOfNodes, experimentId, nodeId);
	metaDataOutputFile = fopen(metaDataFullPath, "w");

	if (nodeId == hpcjoin::core::Configuration::RESULT_AGGREGATION_NODE) {
		printf("[INFO] Experiment data located at %s\n", experimentFullPath);
	}

}

void Measurements::writeMetaData(const char* key, char* value) {
	fprintf(metaDataOutputFile, "%s\t%s\n", key, value);
}

void Measurements::writeMetaData(const char* key, uint64_t value) {
	fprintf(metaDataOutputFile, "%s\t%lu\n", key, value);
}

void Measurements::storeAllMeasurements() {
	storePhaseData();
	storeSpecialData();
	storeHistogramComputationData();
	storeNetworkPartitioningData();
	storeLocalPartitioningData();
	storeBuildProbeData();
	fflush(performanceOutputFile);
	fclose(performanceOutputFile);
	fflush(metaDataOutputFile);
	fclose(metaDataOutputFile);
}

uint64_t Measurements::timeDiff(struct timeval stop, struct timeval start) {
	return (stop.tv_sec * 1000000L + stop.tv_usec) - (start.tv_sec * 1000000L + start.tv_usec);
}

/***************************************************************/

#define PAPI_COUNT 2
void Measurements::startHardwareCounters() {

	/*
	 int events[] = {   PAPI_L2_TCA, PAPI_L2_TCR, PAPI_L2_TCW,
	 PAPI_L3_TCA, PAPI_L3_TCR, PAPI_L3_TCW,
	 PAPI_L2_DCA, PAPI_L2_DCR, PAPI_L2_DCW,
	 PAPI_L3_DCA, PAPI_L3_DCR, PAPI_L3_DCW,
	 PAPI_L1_TCM, PAPI_L1_DCM, PAPI_L1_ICM, PAPI_L1_LDM, PAPI_L1_STM,
	 PAPI_L2_TCM, PAPI_L2_DCM, PAPI_L2_ICM, PAPI_L2_LDM, PAPI_L2_STM,
	 PAPI_L3_TCM, PAPI_L3_LDM,
	 PAPI_TOT_CYC, PAPI_MEM_WCY, PAPI_RES_STL,
	 PAPI_BR_CN, PAPI_BR_MSP, PAPI_BR_PRC,
	 PAPI_LD_INS, PAPI_SR_INS,
	 PAPI_TLB_DM, PAPI_TLB_IM};*/

	int events[PAPI_COUNT] = { PAPI_L1_TCM, PAPI_L1_DCM };

	int ret = 0;
	if ((ret = PAPI_start_counters(events, PAPI_COUNT)) != PAPI_OK) {
		fprintf(stderr, "PAPI failed to start counters: %s\n", PAPI_strerror(ret));
		exit(1);
	}

}

void Measurements::printHardwareCounters(const char* name) {

	long_long values[PAPI_COUNT];

	int ret = 0;
	if ((ret = PAPI_stop_counters(values, PAPI_COUNT)) != PAPI_OK) {
		fprintf(stderr, "PAPI failed to read counters: %s\n", PAPI_strerror(ret));
		exit(1);
	}

	printf("[REPORT][%s] PAPI Values:", name);
	for (uint32_t i = 0; i < PAPI_COUNT; ++i) {
		printf("\t%lld", values[i]);
	}
	printf("\n");

}

/***************************************************************/

int32_t parseInfoLine(char* line) {
	int32_t i = strlen(line);
	while (*line < '0' || *line > '9')
		line++;
	line[i - 3] = '\0';
	i = atoi(line);
	return i;
}

void Measurements::printMemoryUtilization(const char* name) {
	FILE* file = fopen("/proc/self/status", "r");
	uint64_t memory = -1;
	char line[128];

	while (fgets(line, 128, file) != NULL) {
		if (strncmp(line, "VmSize:", 7) == 0) {
			memory = parseInfoLine(line);
			break;
		}
	}
	fclose(file);

	memory *= 1024;
	fprintf(stdout, "[MEMORY][%s] %lu bytes\n", name, memory);
	fflush(stdout);

}

} /* namespace performance */
} /* namespace hpcjoin */

