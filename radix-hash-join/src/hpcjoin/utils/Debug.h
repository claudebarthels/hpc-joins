/**
 * @author  Claude Barthels <claudeb@inf.ethz.ch>
 * (c) 2016, ETH Zurich, Systems Group
 *
 */

#ifndef HPCJOIN_UTILS_DEBUG_H_
#define HPCJOIN_UTILS_DEBUG_H_

#include <stdio.h>
#include <stdlib.h>
#include <hpcjoin/performance/Measurements.h>

/*********************************/

#ifdef JOIN_DEBUG_PRINT

#define JOIN_DEBUG(A, B, ...) { \
	fprintf(stdout, "["); \
	fprintf(stdout, A); \
	fprintf(stdout, "] "); \
	fprintf(stdout, B, ##__VA_ARGS__); \
	fprintf(stdout, "\n"); \
	fflush(stdout); \
}

#define JOIN_ASSERT(C, A, B, ...) { \
	if(!(C)) { \
		fprintf(stdout, "["); \
		fprintf(stdout, A); \
		fprintf(stdout, "] "); \
		fprintf(stdout, B, ##__VA_ARGS__); \
		fprintf(stdout, "\n"); \
		fflush(stdout); \
		exit(-1); \
	} \
}



#else

#define JOIN_DEBUG(A, B, ...) {}
#define JOIN_ASSERT(C, A, B, ...) {}

#endif

/*********************************/

#ifdef JOIN_MEMORY_PRINT

#define JOIN_MEM_DEBUG(A) { \
	hpcjoin::performance::Measurements::printMemoryUtilization(A); \
}

#else

#define JOIN_MEM_DEBUG(A) {}

#endif

#endif /* HPCJOIN_UTILS_DEBUG_H_ */
