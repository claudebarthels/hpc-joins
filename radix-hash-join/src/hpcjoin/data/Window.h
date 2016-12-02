/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_DATA_WINDOW_H_
#define HPCJOIN_DATA_WINDOW_H_

#include <mpi.h>

#ifdef USE_FOMPI
#include <fompi.h>
#endif

#include <stdint.h>

#include <hpcjoin/data/CompressedTuple.h>

namespace hpcjoin {
namespace data {

class Window {

public:

	Window(uint32_t numberOfNodes, uint32_t nodeId, uint32_t *assignment, uint64_t *localHistogram, uint64_t *globalHistogram, uint64_t *baseOffsets, uint64_t *writeOffsets);
	~Window();

public:

	void start();
	void stop();

	void write(uint32_t partitionId, CompressedTuple *tuples, uint64_t sizeInTuples, bool flush = true);

	void flush();

public:

	CompressedTuple *getPartition(uint32_t partitionId);
	uint64_t getPartitionSize(uint32_t partitionId);

public:

	uint64_t computeLocalWindowSize();
	uint64_t computeWindowSize(uint32_t nodeId);

public:

	void assertAllTuplesWritten();

protected:

	uint64_t localWindowSize;
	hpcjoin::data::CompressedTuple *data;

	#ifdef USE_FOMPI
	foMPI_Win *window;
	#else
	MPI_Win *window;
	#endif


protected:

	uint32_t numberOfNodes;
	uint32_t nodeId;

	uint32_t *assignment;
	uint64_t *localHistogram;
	uint64_t *globalHistogram;
	uint64_t *baseOffsets;
	uint64_t *writeOffsets;

protected:

	uint64_t *writeCounters;

};

} /* namespace data */
} /* namespace hpcjoin */

#endif /* HPCJOIN_DATA_WINDOW_H_ */
