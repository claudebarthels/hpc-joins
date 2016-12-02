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
	static void startHistogramComputation();
	static void stopHistogramComputation();
	static void startNetworkPartitioning();
	static void stopNetworkPartitioning();
	static void startLocalProcessing();
	static void stopLocalProcessing();
	static void storePhaseData();

protected:

	static struct timeval joinStart;
	static struct timeval joinStop;
	static struct timeval histogramComputationStart;
	static struct timeval histogramComputationStop;
	static struct timeval networkPartitioningStart;
	static struct timeval networkPartitioningStop;
	static struct timeval localProcessingStart;
	static struct timeval localProcessingStop;

	static uint64_t totalCycles;
	static uint64_t totalTime;
	static uint64_t phaseTimes[3];

	/**
	 * Timing for synchronization and preparations
	 */

public:

	static void startWindowAllocation();
	static void stopWindowAllocation();
	static void startWaitingForNetworkCompletion();
	static void stopWaitingForNetworkCompletion();
	static void startLocalProcessingPreparations();
	static void stopLocalProcessingPreparations();
	static void storeSpecialData();

protected:

	static struct timeval windowAllocationStart;
	static struct timeval windowAllocationStop;
	static struct timeval waitingForNetworkCompletionStart;
	static struct timeval waitingForNetworkCompletionStop;
	static struct timeval localProcessingPreparationsStart;
	static struct timeval localProcessingPreparationsStop;

	static uint64_t specialTimes[3];

	/**
	 * Specific counters for histogram computation
	 */

public:

	static void startHistogramLocalHistogramComputation();
	static void stopHistogramLocalHistogramComputation(uint64_t numberOfElemenets);
	static void startHistogramGlobalHistogramComputation();
	static void stopHistogramGlobalHistogramComputation();
	static void startHistogramAssignmentComputation();
	static void stopHistogramAssignmentComputation();
	static void startHistogramOffsetComputation();
	static void stopHistogramOffsetComputation();
	static void storeHistogramComputationData();

protected:

	/**
	 * Specific counters for network partitioning
	 */

	static struct timeval histogramLocalHistogramComputationStart;
	static struct timeval histogramLocalHistogramComputationStop;
	static struct timeval histogramGlobalHistogramComputationStart;
	static struct timeval histogramGlobalHistogramComputationStop;
	static struct timeval histogramAssignmentStart;
	static struct timeval histogramAssignmentStop;
	static struct timeval histogramOffsetComputationStart;
	static struct timeval histogramOffsetComputationStop;

	static uint64_t histogramLocalHistogramComputationIdx;
	static uint64_t histogramLocalHistogramComputationTimes[2];
	static uint64_t histogramLocalHistogramComputationElements[2];
	static uint64_t histogramGlobalHistogramComputationIdx;
	static uint64_t histogramGlobalHistogramComputationTimes[2];
	static uint64_t histogramAssignmentComputationTime;
	static uint64_t histogramOffsetComputationIdx;
	static uint64_t histogramOffsetComputationTimes[2];



public:

	static void startNetworkPartitioningMemoryAllocation();
	static void stopNetworkPartitioningMemoryAllocation(uint64_t bufferSize);
	static void startNetworkPartitioningMainPartitioning();
	static void stopNetworkPartitioningMainPartitioning(uint64_t numberOfElemenets);
	static void startNetworkPartitioningFlushPartitioning();
	static void stopNetworkPartitioningFlushPartitioning();
	static void startNetworkPartitioningWindowPut();
	static void stopNetworkPartitioningWindowPut();
	static void startNetworkPartitioningWindowWait();
	static void stopNetworkPartitioningWindowWait();
	static void storeNetworkPartitioningData();

protected:

	static struct timeval networkPartitioningMemoryAllocationStart;
	static struct timeval networkPartitioningMemoryAllocationStop;
	static struct timeval networkPartitioningMainPartitioningStart;
	static struct timeval networkPartitioningMainPartitioningStop;
	static struct timeval networkPartitioningFlushPartitioningStart;
	static struct timeval networkPartitioningFlushPartitioningStop;
	static struct timeval networkPartitioningWindowPutStart;
	static struct timeval networkPartitioningWindowPutStop;
	static struct timeval networkPartitioningWindowWaitStart;
	static struct timeval networkPartitioningWindowWaitStop;

	static uint64_t networkPartitioningMemoryAllocationIdx;
	static uint64_t networkPartitioningMemoryAllocationTimes[2];
	static uint64_t networkPartitioningMainPartitioningIdx;
	static uint64_t networkPartitioningMainPartitioningTimes[2];
	static uint64_t networkPartitioningFlushPartitioningIdx;
	static uint64_t networkPartitioningFlushPartitioningTimes[2];
	static uint64_t networkPartitioningWindowPutCount;
	static uint64_t networkPartitioningWindowPutTimeSum;
	static uint64_t networkPartitioningWindowWaitCount;
	static uint64_t networkPartitioningWindowWaitTimeSum;

	/**
	 * Specific counters for local partitioning
	 */

public:

	static void startLocalPartitioningTask();
	static void stopLocalPartitioningTask();
	static void startLocalPartitioningHistogramComputation();
	static void stopLocalPartitioningHistogramComputation(uint64_t numberOfElemenets);
	static void startLocalPartitioningOffsetComputation();
	static void stopLocalPartitioningOffsetComputation();
	static void startLocalPartitioningMemoryAllocation();
	static void stopLocalPartitioningMemoryAllocation(uint64_t bufferSize);
	static void startLocalPartitioningPartitioning();
	static void stopLocalPartitioningPartitioning(uint64_t numberOfElemenets);
	static void storeLocalPartitioningData();

protected:

	static struct timeval localPartitioningTaskStart;
	static struct timeval localPartitioningTaskStop;
	static struct timeval localPartitioningHistogramComputationStart;
	static struct timeval localPartitioningHistogramComputationStop;
	static struct timeval localPartitioningOffsetComputationStart;
	static struct timeval localPartitioningOffsetComputationStop;
	static struct timeval localPartitioningMemoryAllocationStart;
	static struct timeval localPartitioningMemoryAllocationStop;
	static struct timeval localPartitioningPartitioningStart;
	static struct timeval localPartitioningPartitioningStop;

	static uint64_t localPartitioningTaskCount;
	static uint64_t localPartitioningTaskTimeSum;
	static uint64_t localPartitioningHistogramComputationCount;
	static uint64_t localPartitioningHistogramComputationTimeSum;
	static uint64_t localPartitioningHistogramComputationElementSum;
	static uint64_t localPartitioningOffsetComputationCount;
	static uint64_t localPartitioningOffsetComputationTimeSum;
	static uint64_t localPartitioningMemoryAllocationCount;
	static uint64_t localPartitioningMemoryAllocationTimeSum;
	static uint64_t localPartitioningMemoryAllocationSizeSum;
	static uint64_t localPartitioningPartitioningCount;
	static uint64_t localPartitioningPartitioningTimeSum;
	static uint64_t localPartitioningPartitioningElementSum;

	/**
	 * Specific counters for build-probe phase
	 */

public:

	static void startBuildProbeTask();
	static void stopBuildProbeTask();
	static void startBuildProbeMemoryAllocation();
	static void stopBuildProbeMemoryAllocation(uint64_t numberOfElemenets);
	static void startBuildProbeBuild();
	static void stopBuildProbeBuild(uint64_t numberOfElemenets);
	static void startBuildProbeProbe();
	static void stopBuildProbeProbe(uint64_t numberOfElemenets);
	static void storeBuildProbeData();

protected:

	static struct timeval buildProbeTaskStart;
	static struct timeval buildProbeTaskStop;
	static struct timeval buildProbeMemoryAllocationStart;
	static struct timeval buildProbeMemoryAllocationStop;
	static struct timeval buildProbeBuildStart;
	static struct timeval buildProbeBuildStop;
	static struct timeval buildProbeProbeStart;
	static struct timeval buildProbeProbeStop;

	static uint64_t buildProbeTaskCount;
	static uint64_t buildProbeTaskTimeSum;
	static uint64_t buildProbeMemoryAllocationCount;
	static uint64_t buildProbeMemoryAllocationTimeSum;
	static uint64_t buildProbeMemoryAllocationSizeSum;
	static uint64_t buildProbeBuildCount;
	static uint64_t buildProbeBuildTimeSum;
	static uint64_t buildProbeBuildElementSum;
	static uint64_t buildProbeProbeCount;
	static uint64_t buildProbeProbeTimeSum;
	static uint64_t buildProbeProbeElementSum;

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
