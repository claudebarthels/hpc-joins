/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_PERFORMANCE_MEASUREMENTS_H_
#define HPCJOIN_PERFORMANCE_MEASUREMENTS_H_

#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdio.h>
#include <string>

namespace hpcjoin {
namespace performance {

class Measurements {

	/**
	 * Timings for section
	 */

public:

	static void startJoin();
	static void stopJoin();
	static void startPartitioning();
	static void stopPartitioning();
	static void startSorting();
	static void stopSorting();
	static void startMerging();
	static void stopMerging();
	static void startMatching();
	static void stopMatching();
	static void storePhaseData();

protected:

	static struct timeval joinStart;
	static struct timeval joinStop;
	static struct timeval partitioningStart;
	static struct timeval partitioningStop;
	static struct timeval sortingStart;
	static struct timeval sortingStop;
	static struct timeval mergingStart;
	static struct timeval mergingStop;
	static struct timeval matchingStart;
	static struct timeval matchingStop;

	static uint64_t totalCycles;
	static uint64_t totalTime;
	static uint64_t partitioningTime;
	static uint64_t sortingTime;
	static uint64_t mergingTime;
	static uint64_t matchingTime;

	/**
	 * Timing for partitioning
	 */

public:

	static void startLocalHistogram();
	static void stopLocalHistogram(uint64_t numberOfElemenets);
	static void startWindowPreparationComputation();
	static void stopWindowPreparationComputation();
	static void startPartitioningElements();
	static void stopPartitioningElements(uint64_t numberOfElemenets);
	static void startWindowAllocation();
	static void stopWindowAllocation();
	static void storePartitioningData();

protected:

	static struct timeval localHistogramStart;
	static struct timeval localHistogramStop;
	static struct timeval windowPreparationStart;
	static struct timeval windowPreparationStop;
	static struct timeval partitioningElementsStart;
	static struct timeval partitioningElementsStop;
	static struct timeval windowAllocationStart;
	static struct timeval windowAllocationStop;

	static uint64_t localHistogramIdx;
	static uint64_t localHistogramTimes[2];
	static uint64_t localHistogramElements[2];
	static uint64_t windowPreparationTime;
	static uint64_t partitioningElementsIdx;
	static uint64_t partitioningElementsTimes[2];
	static uint64_t partitioningElementsElements[2];
	static uint64_t windowAllocationTime;

	/**
	 * Timing for sorting
	 */

public:

	static void startRunPreparations();
	static void stopRunPreparations();
	static void startSortTask();
	static void stopSortTask();
	static void startSortingElements();
	static void stopSortingElements(uint64_t numberOfElemenets);
	static void startPut();
	static void stopPut();
	static void startFlush();
	static void stopFlush();
	static void startWaitIncoming();
	static void stopWaitIncoming();
	static void storeSortingData();

protected:

	static struct timeval runPreparationsStart;
	static struct timeval runPreparationsStop;
	static struct timeval sortTaskStart;
	static struct timeval sortTaskStop;
	static struct timeval sortElementsStart;
	static struct timeval sortElementsStop;
	static struct timeval putStart;
	static struct timeval putStop;
	static struct timeval flushStart;
	static struct timeval flushStop;
	static struct timeval waitIncomingStart;
	static struct timeval waitIncomingStop;

	static uint64_t runPreparationsTime;
	static uint64_t sortTaskTimeSum;
	static uint64_t sortTaskCount;
	static uint64_t sortElementsTimeSum;
	static uint64_t sortElementCount;
	static uint64_t putTimeSum;
	static uint64_t putCount;
	static uint64_t flushTime;
	static uint64_t waitIncomingTime;

	/**
	 * Timing for merging
	 */

public:

	static void startMergingLevel();
	static void stopMergingLevel();
	static void startMergingTask();
	static void stopMergingTask(uint64_t numberOfElemenets);
	static void storeMergingData();

protected:

	static struct timeval mergingLevelStart;
	static struct timeval mergingLevelStop;
	static struct timeval mergingTaskStart;
	static struct timeval mergingTaskStop;

	static uint64_t mergingLevelTimeSum;
	static uint64_t mergingLevelCount;
	static uint64_t mergingTaskTimeSum;
	static uint64_t mergingTaskCount;

	/**
	 * Timing for merging
	 */

public:

	static void startMatchingTask();
	static void stopMatchingTask();
	static void storeMatchingData();

protected:

	static struct timeval matchingTaskStart;
	static struct timeval matchingTaskStop;

	static uint64_t matchingTaskTime;

	/**
	 * Result aggregation
	 */

public:

	static void init(uint32_t nodeId, uint32_t numberOfNodes, std::string tag);
	static uint64_t *serializeResults();
	static void sendMeasurementsToAggregator();
	static void receiveAllMeasurments(uint32_t numberOfNodes, uint32_t nodeId);
	static void printMeasurements(uint32_t numberOfNodes, uint32_t nodeId);
	static void storeAllMeasurements();

protected:

	static uint64_t timeDiff(struct timeval stop, struct timeval start);
	static uint64_t **serizlizedResults;

protected:

	static FILE * performanceOutputFile;
	static FILE * metaDataOutputFile;

public:

	static void writeMetaData(const char *key, char *value);
	static void writeMetaData(const char *key, uint64_t value);

public:

	static void startHardwareCounters();
	static void printHardwareCounters(const char* name);

public:

	static void printMemoryUtilization(const char* name);

};

} /* namespace performance */
} /* namespace hpcjoin */

#endif /* HPCJOIN_PERFORMANCE_MEASUREMENTS_H_ */
