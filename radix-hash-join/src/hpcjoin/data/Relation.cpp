/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#include "Relation.h"

#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h>

#include <hpcjoin/core/Configuration.h>
#include <hpcjoin/utils/Debug.h>
#include <hpcjoin/memory/Pool.h>

#define RAND_RANGE(N) (((double) rand() / ((double) RAND_MAX + 1)) * (N))

#define EXCHANGE_DATA_TAG 456378

namespace hpcjoin {
namespace data {

Relation::Relation(uint64_t localSize, uint64_t globalSize) {

	this->localSize = localSize;
	this->globalSize = globalSize;

	//int result = posix_memalign((void **) &(this->data), hpcjoin::core::Configuration::CACHELINE_SIZE_BYTES, localSize * sizeof(hpcjoin::data::Tuple));
	//JOIN_ASSERT(result == 0, "Relation", "Could not allocate memory for %lu tuples", localSize);
	this->data = (hpcjoin::data::Tuple *) hpcjoin::memory::Pool::getMemory(localSize * sizeof(hpcjoin::data::Tuple));

	memset(this->data, 0, localSize * sizeof(hpcjoin::data::Tuple));

}

Relation::~Relation() {

	free(this->data);

}

uint64_t Relation::getLocalSize() {

	return this->localSize;

}

uint64_t Relation::getGlobalSize() {

	return this->globalSize;

}

hpcjoin::data::Tuple* Relation::getData() {

	return this->data;

}

void Relation::fillUniqueValues(uint64_t startKeyValue, uint64_t startRidValue) {

	for (uint64_t i = 0; i < this->localSize; ++i) {
		this->data[i].key = startKeyValue + i;
	}
	randomOrder();
	for (uint64_t i = 0; i < this->localSize; ++i) {
		this->data[i].rid = startRidValue + i;
	}

}

void Relation::fillModuloValues(uint64_t startKeyValue, uint64_t startRidValue, uint64_t innerRelationSize) {

	for (uint64_t i = 0; i < this->localSize; ++i) {
		this->data[i].key = startKeyValue + (i % innerRelationSize);
	}
	randomOrder();
	for (uint64_t i = 0; i < this->localSize; ++i) {
		this->data[i].rid = startRidValue + i;
	}

}

void Relation::randomOrder() {

	hpcjoin::data::Tuple temp;
	for (uint64_t i = this->localSize - 1; i > 0; --i) {
		uint64_t j = (uint64_t) RAND_RANGE(i);
		temp.key = data[i].key;
		data[i].key = data[j].key;
		data[j].key = temp.key;
	}

}

void Relation::distribute(uint32_t nodeId, uint32_t numberOfNodes) {

	uint64_t incomingDataSize = (localSize - (numberOfNodes - 1) * (localSize / numberOfNodes));
	hpcjoin::data::Tuple *incomingData = (hpcjoin::data::Tuple *) calloc(incomingDataSize, sizeof(hpcjoin::data::Tuple));

	for (uint32_t i = 0; i < nodeId; ++i) {
		// Receive from node i
		// Swap with section nodeId+i
		// Send to node i
		uint32_t section = (nodeId + i) % numberOfNodes;
		uint64_t sectionStart = section * (localSize / numberOfNodes);
		uint64_t sectionSize = (section == numberOfNodes - 1) ? (localSize - (numberOfNodes - 1) * (localSize / numberOfNodes)) : (localSize / numberOfNodes);
		JOIN_DEBUG("SWAP", "%d (%lu) <--> %d (%lu)\n", nodeId, section, i, section);
		MPI_Recv(incomingData, sectionSize * sizeof(hpcjoin::data::Tuple), MPI_BYTE, i, EXCHANGE_DATA_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		MPI_Send(this->data + sectionStart, sectionSize * sizeof(hpcjoin::data::Tuple), MPI_BYTE, i, EXCHANGE_DATA_TAG, MPI_COMM_WORLD);
		memcpy(data + sectionStart, incomingData, sectionSize * sizeof(hpcjoin::data::Tuple));
	}

	for (uint32_t i = nodeId + 1; i < numberOfNodes; ++i) {
		// Send to node i
		// Send section nodeId+i
		// Receive section nodeId+i
		uint32_t section = (nodeId + i) % numberOfNodes;
		uint64_t sectionStart = section * (localSize / numberOfNodes);
		uint64_t sectionSize = (section == numberOfNodes - 1) ? (localSize - (numberOfNodes - 1) * (localSize / numberOfNodes)) : (localSize / numberOfNodes);
		JOIN_DEBUG("SWAP", "%d (%lu) <--> %d (%lu)\n", nodeId, section, i, section);
		MPI_Send(this->data + sectionStart, sectionSize * sizeof(hpcjoin::data::Tuple), MPI_BYTE, i, EXCHANGE_DATA_TAG, MPI_COMM_WORLD);
		MPI_Recv(incomingData, sectionSize * sizeof(hpcjoin::data::Tuple), MPI_BYTE, i, EXCHANGE_DATA_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		memcpy(data + sectionStart, incomingData, sectionSize * sizeof(hpcjoin::data::Tuple));
	}

	free(incomingData);
	randomOrder();

}

void Relation::debugKeyPrint() {
	for (uint64_t i = 0; i < this->localSize; ++i) {
		fprintf(stdout, "%lu, ", this->data[i].key);
	}
	fprintf(stdout, "\n");
	fflush(stdout);
}

} /* namespace data */
} /* namespace hpcjoin */

