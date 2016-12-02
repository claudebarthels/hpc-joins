/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "Measurements.h"

#define MSG_TAG_RESULTS 448524

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h>
#include <papi.h>
#include <sys/stat.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/operators/SortMergeJoin.h>

namespace hpcjoin {
namespace performance {

/************************************************************/

struct timeval Measurements::joinStart;
struct timeval Measurements::joinStop;
struct timeval Measurements::partitioningStart;
struct timeval Measurements::partitioningStop;
struct timeval Measurements::sortingStart;
struct timeval Measurements::sortingStop;
struct timeval Measurements::mergingStart;
struct timeval Measurements::mergingStop;
struct timeval Measurements::matchingStart;
struct timeval Measurements::matchingStop;

uint64_t Measurements::totalCycles;
uint64_t Measurements::totalTime;
uint64_t Measurements::partitioningTime;
uint64_t Measurements::sortingTime;
uint64_t Measurements::mergingTime;
uint64_t Measurements::matchingTime;

/************************************************************/

struct timeval Measurements::localHistogramStart;
struct timeval Measurements::localHistogramStop;
struct timeval Measurements::windowPreparationStart;
struct timeval Measurements::windowPreparationStop;
struct timeval Measurements::partitioningElementsStart;
struct timeval Measurements::partitioningElementsStop;
struct timeval Measurements::windowAllocationStart;
struct timeval Measurements::windowAllocationStop;

uint64_t Measurements::localHistogramIdx = 0;
uint64_t Measurements::localHistogramTimes[2];
uint64_t Measurements::localHistogramElements[2];
uint64_t Measurements::windowPreparationTime;
uint64_t Measurements::partitioningElementsIdx = 0;
uint64_t Measurements::partitioningElementsTimes[2];
uint64_t Measurements::partitioningElementsElements[2];
uint64_t Measurements::windowAllocationTime;

/************************************************************/

struct timeval Measurements::runPreparationsStart;
struct timeval Measurements::runPreparationsStop;
struct timeval Measurements::sortTaskStart;
struct timeval Measurements::sortTaskStop;
struct timeval Measurements::sortElementsStart;
struct timeval Measurements::sortElementsStop;
struct timeval Measurements::putStart;
struct timeval Measurements::putStop;
struct timeval Measurements::flushStart;
struct timeval Measurements::flushStop;
struct timeval Measurements::waitIncomingStart;
struct timeval Measurements::waitIncomingStop;

uint64_t Measurements::runPreparationsTime;
uint64_t Measurements::sortTaskTimeSum = 0;
uint64_t Measurements::sortTaskCount = 0;
uint64_t Measurements::sortElementsTimeSum = 0;
uint64_t Measurements::sortElementCount = 0;
uint64_t Measurements::putTimeSum = 0;
uint64_t Measurements::putCount = 0;
uint64_t Measurements::flushTime;
uint64_t Measurements::waitIncomingTime;

/************************************************************/

struct timeval Measurements::mergingLevelStart;
struct timeval Measurements::mergingLevelStop;
struct timeval Measurements::mergingTaskStart;
struct timeval Measurements::mergingTaskStop;

uint64_t Measurements::mergingLevelTimeSum = 0;
uint64_t Measurements::mergingLevelCount = 0;
uint64_t Measurements::mergingTaskTimeSum = 0;
uint64_t Measurements::mergingTaskCount = 0;

/************************************************************/

struct timeval Measurements::matchingTaskStart;
struct timeval Measurements::matchingTaskStop;

uint64_t Measurements::matchingTaskTime;

/************************************************************/

FILE * Measurements::performanceOutputFile;
FILE * Measurements::metaDataOutputFile;

uint64_t ** hpcjoin::performance::Measurements::serizlizedResults;

/************************************************************/

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

void Measurements::startPartitioning() {
	gettimeofday(&partitioningStart, NULL);
}

void Measurements::stopPartitioning() {
	gettimeofday(&partitioningStop, NULL);
	partitioningTime = timeDiff(partitioningStop, partitioningStart);
}

void Measurements::startSorting() {
	gettimeofday(&sortingStart, NULL);
}

void Measurements::stopSorting() {
	gettimeofday(&sortingStop, NULL);
	sortingTime = timeDiff(sortingStop, sortingStart);
}


void Measurements::startWindowAllocation() {
	gettimeofday(&windowAllocationStart, NULL);
}

void Measurements::stopWindowAllocation() {
	gettimeofday(&windowAllocationStop, NULL);
	windowAllocationTime = timeDiff(windowAllocationStop, windowAllocationStart);
}


void Measurements::startMerging() {
	gettimeofday(&mergingStart, NULL);
}

void Measurements::stopMerging() {
	gettimeofday(&mergingStop, NULL);
	mergingTime = timeDiff(mergingStop, mergingStart);
}

void Measurements::startMatching() {
	gettimeofday(&matchingStart, NULL);
}

void Measurements::stopMatching() {
	gettimeofday(&matchingStop, NULL);
	matchingTime = timeDiff(matchingStop, matchingStart);
}

void Measurements::storePhaseData() {
	fprintf(performanceOutputFile, "CTOTAL\t%lu\tcycles\n", totalCycles);
	fprintf(performanceOutputFile, "JTOTAL\t%lu\tus\n", totalTime);
	fprintf(performanceOutputFile, "JPART\t%lu\tus\n", partitioningTime);
	fprintf(performanceOutputFile, "JWINALLOC\t%lu\tus\n", windowAllocationTime);
	fprintf(performanceOutputFile, "JSORT\t%lu\tus\n", sortingTime);
	fprintf(performanceOutputFile, "JMERG\t%lu\tus\n", mergingTime);
	fprintf(performanceOutputFile, "JMATCH\t%lu\tus\n", matchingTime);
}

/************************************************************/

void Measurements::startLocalHistogram() {
	gettimeofday(&localHistogramStart, NULL);
}

void Measurements::stopLocalHistogram(uint64_t numberOfElemenets) {
	gettimeofday(&localHistogramStop, NULL);
	localHistogramTimes[localHistogramIdx] = timeDiff(localHistogramStop, localHistogramStart);
	localHistogramElements[localHistogramIdx] = numberOfElemenets;
	++localHistogramIdx;
}

void Measurements::startWindowPreparationComputation() {
	gettimeofday(&windowPreparationStart, NULL);
}

void Measurements::stopWindowPreparationComputation() {
	gettimeofday(&windowPreparationStop, NULL);
	windowPreparationTime = timeDiff(windowPreparationStop, windowPreparationStart);
}

void Measurements::startPartitioningElements() {
	gettimeofday(&partitioningElementsStart, NULL);
}

void Measurements::stopPartitioningElements(uint64_t numberOfElemenets) {
	gettimeofday(&partitioningElementsStop, NULL);
	partitioningElementsTimes[partitioningElementsIdx] = timeDiff(partitioningElementsStop, partitioningElementsStart);
	partitioningElementsElements[partitioningElementsIdx] = numberOfElemenets;
	++partitioningElementsIdx;
}

void Measurements::storePartitioningData() {
	fprintf(performanceOutputFile, "ILOCHIST\t%lu\tus\n", localHistogramTimes[0]);
	fprintf(performanceOutputFile, "ILOCHISTE\t%lu\ttuples\n", localHistogramElements[0]);
	fprintf(performanceOutputFile, "OLOCHIST\t%lu\tus\n", localHistogramTimes[1]);
	fprintf(performanceOutputFile, "OLOCHISTE\t%lu\ttuples\n", localHistogramElements[1]);
	fprintf(performanceOutputFile, "WINPREP\t%lu\tus\n", windowPreparationTime);
	fprintf(performanceOutputFile, "IPART\t%lu\tus\n", partitioningElementsTimes[0]);
	fprintf(performanceOutputFile, "IPARTE\t%lu\ttuples\n", partitioningElementsElements[0]);
	fprintf(performanceOutputFile, "OPART\t%lu\tus\n", partitioningElementsTimes[1]);
	fprintf(performanceOutputFile, "OPARTE\t%lu\ttuples\n", partitioningElementsElements[1]);
}

/***************************************************************/

void Measurements::startRunPreparations() {
	gettimeofday(&runPreparationsStart, NULL);
}

void Measurements::stopRunPreparations() {
	gettimeofday(&runPreparationsStop, NULL);
	runPreparationsTime = timeDiff(runPreparationsStop, runPreparationsStart);
}

void Measurements::startSortTask() {
	gettimeofday(&sortTaskStart, NULL);
}

void Measurements::stopSortTask() {
	gettimeofday(&sortTaskStop, NULL);
	sortTaskTimeSum += timeDiff(sortTaskStop, sortTaskStart);
	++sortTaskCount;
}

void Measurements::startSortingElements() {
	gettimeofday(&sortElementsStart, NULL);
}

void Measurements::stopSortingElements(uint64_t numberOfElemenets) {
	gettimeofday(&sortElementsStop, NULL);
	sortElementsTimeSum += timeDiff(sortElementsStop, sortElementsStart);
	sortElementCount += numberOfElemenets;
}

void Measurements::startPut() {
	gettimeofday(&putStart, NULL);
}

void Measurements::stopPut() {
	gettimeofday(&putStop, NULL);
	putTimeSum += timeDiff(putStop, putStart);
	++putCount;
}

void Measurements::startFlush() {
	gettimeofday(&flushStart, NULL);
}

void Measurements::stopFlush() {
	gettimeofday(&flushStop, NULL);
	flushTime = timeDiff(flushStop, flushStart);
}

void Measurements::startWaitIncoming() {
	gettimeofday(&waitIncomingStart, NULL);
}

void Measurements::stopWaitIncoming() {
	gettimeofday(&waitIncomingStop, NULL);
	waitIncomingTime = timeDiff(waitIncomingStop, waitIncomingStart);
}

void Measurements::storeSortingData() {
	fprintf(performanceOutputFile, "RUNPREP\t%lu\tus\n", runPreparationsTime);
	fprintf(performanceOutputFile, "SORTTSUM\t%lu\tus\n", sortTaskTimeSum);
	fprintf(performanceOutputFile, "SORTTCNT\t%lu\ttasks\n", sortTaskCount);
	fprintf(performanceOutputFile, "SORTESUM\t%lu\tus\n", sortElementsTimeSum);
	fprintf(performanceOutputFile, "SORTECNT\t%lu\ttuples\n", sortElementCount);
	fprintf(performanceOutputFile, "PUTSUM\t%lu\tus\n", putTimeSum);
	fprintf(performanceOutputFile, "PUTCNT\t%lu\tputs\n", putCount);
	fprintf(performanceOutputFile, "FLUSH\t%lu\tus\n", flushTime);
	fprintf(performanceOutputFile, "WAIT\t%lu\tus\n", waitIncomingTime);
}

/***************************************************************/

void Measurements::startMergingLevel() {
	gettimeofday(&mergingLevelStart, NULL);
}

void Measurements::stopMergingLevel() {
	gettimeofday(&mergingLevelStop, NULL);
	mergingLevelTimeSum += timeDiff(mergingLevelStop, mergingLevelStart);
	++mergingLevelCount;
}

void Measurements::startMergingTask() {
	gettimeofday(&mergingTaskStart, NULL);
}

void Measurements::stopMergingTask(uint64_t numberOfElemenets) {
	gettimeofday(&mergingTaskStop, NULL);
	mergingTaskTimeSum += timeDiff(mergingTaskStop, mergingTaskStart);
	++mergingTaskCount;
}

void Measurements::storeMergingData() {
	fprintf(performanceOutputFile, "MERGLTIME\t%lu\tus\n", mergingLevelTimeSum);
	fprintf(performanceOutputFile, "MERGLCNT\t%lu\ttasks\n", mergingLevelCount);
	fprintf(performanceOutputFile, "MERGTTIME\t%lu\tus\n", mergingTaskTimeSum);
	fprintf(performanceOutputFile, "MERGTCNT\t%lu\ttasks\n", mergingTaskCount);
}

/***************************************************************/

void Measurements::startMatchingTask() {
	gettimeofday(&matchingTaskStart, NULL);
}

void Measurements::stopMatchingTask() {
	gettimeofday(&matchingTaskStop, NULL);
	matchingTaskTime = timeDiff(matchingTaskStop, matchingTaskStart);
}

void Measurements::storeMatchingData() {
	fprintf(performanceOutputFile, "MATCHTTIME\t%lu\tus\n", matchingTaskTime);
}

/***************************************************************/

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
	storePartitioningData();
	storeSortingData();
	storeMergingData();
	storeMatchingData();
	fflush(performanceOutputFile);
	fclose(performanceOutputFile);
	fflush(metaDataOutputFile);
	fclose(metaDataOutputFile);
}

uint64_t Measurements::timeDiff(struct timeval stop, struct timeval start) {
	return (stop.tv_sec * 1000000L + stop.tv_usec) - (start.tv_sec * 1000000L + start.tv_usec);
}

/************************************************************/

#define NUM_OF_RESULT_ELEMENTS 7

uint64_t* Measurements::serializeResults() {

	uint64_t *result = (uint64_t *) calloc(NUM_OF_RESULT_ELEMENTS, sizeof(uint64_t));

	result[0] = hpcjoin::operators::SortMergeJoin::RESULT_COUNTER;
	result[1] = totalTime;
	result[2] = partitioningTime;
	result[3] = sortingTime;
	result[4] = waitIncomingTime;
	result[5] = mergingTime;
	result[6] = matchingTime;

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

	printf("[RESULTS] Partition:\t");\
	uint64_t averagePartitionTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t partitionTime = serizlizedResults[n][2];
		printf("%.3f\t", ((double) partitionTime) / 1000);
		averagePartitionTime += partitionTime;
	}
	printf("\n");
	averagePartitionTime /= numberOfNodes;

	printf("[RESULTS] Sorting:\t");
	uint64_t averageSortingTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t sortingTime = serizlizedResults[n][3];
		printf("%.3f\t", ((double) sortingTime) / 1000);
		averageSortingTime += sortingTime;
	}
	printf("\n");
	averageSortingTime /= numberOfNodes;

	printf("[RESULTS] Waiting:\t");
	uint64_t averageWaitingTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t waitingTime = serizlizedResults[n][4];
		printf("%.3f\t", ((double) waitingTime) / 1000);
		averageWaitingTime += waitingTime;
	}
	printf("\n");
	averageWaitingTime /= numberOfNodes;

	printf("[RESULTS] Merging:\t");
	uint64_t averageMergingTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t mergingTime = serizlizedResults[n][5];
		printf("%.3f\t", ((double) mergingTime) / 1000);
		averageMergingTime += mergingTime;
	}
	printf("\n");
	averageMergingTime /= numberOfNodes;

	printf("[RESULTS] Matching:\t");
	uint64_t averageMatchingTime = 0;
	for (uint32_t n = 0; n < numberOfNodes; ++n) {
		uint64_t matchingTime = serizlizedResults[n][6];
		printf("%.3f\t", ((double) matchingTime) / 1000);
		averageMatchingTime += matchingTime;
	}
	printf("\n");
	averageMatchingTime /= numberOfNodes;

	printf("[RESULTS] Summary:\t%lu\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n", totalNumberOfTuples, ((double) averageJoinTime) / 1000, ((double) averagePartitionTime) / 1000, ((double) averageSortingTime) / 1000, ((double) averageWaitingTime) / 1000, ((double) averageMergingTime) / 1000, ((double) averageMatchingTime) / 1000);

}

} /* namespace performance */
} /* namespace hpcjoin */

