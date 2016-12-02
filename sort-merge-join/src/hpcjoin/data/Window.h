/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */


#ifndef HPCJOIN_DATA_WINDOW_H_
#define HPCJOIN_DATA_WINDOW_H_

#include <stdint.h>

#include <mpi.h>

#ifdef USE_FOMPI
#include <fompi.h>
#endif

#include <hpcjoin/data/CompressedTuple.h>
#include <hpcjoin/data/Relation.h>

namespace hpcjoin {
namespace data {

class Window {

public:

	Window(uint32_t numberOfNodes, uint64_t sizeInElements, uint64_t *numberOfElementsFromNode, uint64_t *writeOffsets, hpcjoin::data::Relation *relation);
	~Window();

public:

	void start();
	void stop();

	void write(uint32_t targetNode, CompressedTuple *tuples, uint32_t sizeInTuples);
	bool getNextRun(CompressedTuple **tuples, uint64_t *sizeInTuples);

	hpcjoin::data::CompressedTuple * getData();

protected:

	uint32_t numberOfNodes;
	uint64_t sizeInElements;
	uint64_t *numberOfElementsFromNode;
	uint64_t *writeOffsets;
	uint64_t *writeCounters;

	hpcjoin::data::CompressedTuple *data;

protected:

	hpcjoin::data::CompressedTuple *currentRunData;
	uint32_t currentRunNode;
	uint64_t currentRunRemainingElements;

protected:

#ifdef USE_FOMPI
	foMPI_Win window;
#else
	MPI_Win window;
#endif

};

} /* namespace data */
} /* namespace hpcjoin */

#endif /* HPCJOIN_DATA_WINDOW_H_ */
