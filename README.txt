===========================
Distributed Join Algorithms
===========================

This documentation descibes how to build/configure/run the distributed radix hash
and sort-merge join algorithms.

===============
1. Dependencies
===============

The distributed join algorithms can be compiled for x86 CPUs. The processor needs
to support AVX2 vector instructions.

MPI is used as a communication library. Version 3 or above is required: previous
versions of MPI do not offer one-sided remote memory access (RMA) operations. Specify
the MPI location in the "MPI_FOLDER" variable in the Makefile. The code has been tested
with  openMPI version "1.10.2".

On Cray XC30 and XC40 machines, we recommend to use Fast-One-Sided MPI [1] (foMPI).
The code has been tested with foMPI version "0.2.1". To compile the join with foMPI,
use the following compile time option "-D USE_FOMPI". Specify the location of the
foMPI headers and implementation files in the "FOMPI_LOCATION" variable in the
Makefile. Rename/use the file called "MakefileCray".

Performance counters are accessed through the PAPI [2] performance counter interface.
The code has been tested with PAPI version "5.4.3". Specify the location of the
headers and implementation files in the "PAPI_FOLDER" variable in the Makefile.

[1] https://spcl.inf.ethz.ch/Research/Parallel_Programming/foMPI/
[2] http://icl.cs.utk.edu/papi/


============
2. Compiling
============

Specify the location of the dependencies (see Section 1). Be sure that the following
variables are set correctly:

* MPI_FOLDER: Location of the MPI library
* PAPI_FOLDER: Location of the PAPI library
* FOMPI_LOCATION: Location of the foMPI library (only for Cray machines)

Use the "COMPILER_FLAGS" variable to pass any additional flags. Make sure to add the
option "-D USE_FOMPI" if you to compile the code against foMPI.

In the project folder call the following Make commands:

* make all: Builds the program
* make clean: Removes the build and output folders

After a successful build, the project should contain the following folder structure:

* ./src: The source code
* ./build: The object files
* ./release: The assembled program


===================
3. Running the join
===================

The binary can be executed in a single thread by invoking it directly. For a multi-
threaded execution, use the following command:

* mpiexec --np N ./release/join-binary

Replace "join-binary" with the name of the binary (by default casm-join/cahj-join).
Replace "N" by the number of processes.


=====================
4. Join configuration
=====================

The primary configuration file is located under "./src/hpcjoin/core/Configuration.h".

4.1. Sort-Merge Join:
---------------------

* SORT_RUN_ELEMENT_COUNT: The size (in tuples) of a sorted run which is transmitted
over the network during the reshuffling phase.

* MAX_MERGE_FAN_IN: The maximum fan-in used for merging sorted runs.

4.2. Hash Join:
---------------

* CACHELINES_PER_MEMORY_BUFFER: Size of a network buffer in number of cacheline

* MEMORY_BUFFERS_PER_PARTITION: Number of network buffers per partition

* ENABLE_TWO_LEVEL_PARTITIONING: Configures the algorithm to use 1 or 2 passes.

* NETWORK_PARTITIONING_FANOUT: Fan-out of the first (network) partitioning pass

* LOCAL_PARTITIONING_FANOUT: Fan-out of the second (local) partitioning pass

4.3. Common elements:
---------------------

* RESULT_AGGREGATION_NODE: The process which will return the performance data after
the join has completed.

* CACHELINE_SIZE_BYTES: The size of a cacheline in bytes.

* ALLOCATION_FACTOR: A scaling factor for preallocated memory.

* PAYLOAD_BITS: The number of non-zero bits of the payload/key data used for data
compression.


==========
5. Output
==========

The join algorithms output the performance data into a newly created subfolder. For
this you will need permission to write to the folder from which you call the binary.
Each of the will create a {id}.perf and a {id}.info file.

5.1. Info File:
---------------

The information file contains the following data points:

NUMNODES: 	number of processes
NODEID:		process id
HOST:		name of the machine
GISZ:		global size of the inner relation
GOSZ:		global size of the outer relation
LISZ:		local size of the inner relation
LOSZ:		local size of the outer relation

5.2. Hash Join:
---------------

The performance numbers captured by the hash join are as follows:

CTOTAL: 	total number of CPU cycles of the join
JTOTAL:		total execution time of the join

JHIST:		time required to compute the histogram
HILOCAL:	time required for creating a local histrogram of the inner relation
HILOCELEM:	number of histrogram elements of inner relation
HILOCRATE:	histrogram processing rate for inner relation
HOLOCAL:	time required for creating a local histrogram of the outer relation
HOLOCELEM	number of histrogram elements of outer relation
HOLOCRATE	histrogram processing rate for outer relation
HIGLOBAL	time required for computing a global histrogram of the inner relation
HOGLOBAL	time required for computing a global histrogram of the outer relation
HASSIGN:	time required to compute a partition-node assignment
HIOFFCOMP:	time required to compute the partitioning offsets for the inner relation
HOOFFCOMP:	time required to compute the partitioning offsets for the outer relation

SWINALLOC:	time required to allocate the MPI windows
JMPI:		time required to partition the data
MIMEMALLOC: buffer memory allocation time for inner relation partitioning
MIMAINPART: time required to partition the data of inner relation
MIFLUSHPART:time required to flush data of inner relation
MOMEMALLOC:	buffer memory allocation time for outer relation partitioning
MOMAINPART:	time required to partition the data of outer relation
MOFLUSHPART:time required to flush data of outer relation
MWINPUT:	time needed for setting up PUT requests
MWINPUTCNT:	number of PUT requests
MWINWAIT:	time spent in FLUSH call
MWINWAITCNT:number of FLUSH requests
SNETCOMPL:	waiting time for incoming data

SLOCPREP:	time required to initialize data structures after the partitioning phase
JPROC:		time required to process the data after all the data has been received

LPPART:		total time of the local partitioning phase
LPTASKTIME:	time required to partition the data
LPTASKCOUNT:number of partitioning tasks
LPHISTCOMP:	time required to compute fine-grained histograms
LPHISTELEM:	number of histogram elements
LPOFFSET:	time required to compute sub-partition offsets
LPMEMALLOC:	time spent in memory pool malloc call
LPMEMSIZE:	requested memory size during local partitioning phase
LPELEMENTS:	number of partitioned elements

BPTASKTIME:	total time to execute build/probe tasks
BPTASKCOUNT:number of build/probe tasks
BPMEMALLOC:	time spent in the malloc call
BPMEMSIZE:	requested memory size during build/probe phase
BPBUILD:	time required to build hash tables
BPBUILDELEM:number of hash table elements
BPPROBE:	time required to probe hash tables
BPPROBEELEM:number of probe elements

5.3. Sort-Merge Join:
---------------------


CTOTAL: 	total number of CPU cycles of the join
JTOTAL:		total execution time of the join

JPART:		time required to partition the data
ILOCHIST:	time required for creating a local histrogram of the inner relation
ILOCHISTE:	number of histrogram elements of inner relation
OLOCHIST:	time required for creating a local histrogram of the outer relation
OLOCHISTE:	number of histrogram elements of outer relation
IPART:		time required to partition the data of inner relation
IPARTE:		number of partitioned element of the inner relation
OPART:		time required to partition the data of outer relation
OPARTE:		number of partitioned element of the outer relation

JWINALLOC:	time required to allocate the MPI windows
WINPREP:	time required to prepare window object

JSORT:		time required to sort the data
RUNPREP:	time required to compute run sizes and prepare sort tasks
SORTTSUM:	time required to complete all sort tasks
SORTTCNT:	number of sort tasks
SORTECNT:	total number of sorted elements
PUTSUM:		time spent in PUT calls
PUTCNT:		number of PUT calls
FLUSH:		time spent in FLUSH calls
WAIT:		waiting time for incoming data

JMERG:		time required to merge runs
MERGLTIME:	time required to process the levels of the merge tree
MERGLCNT:	merge tree levels
MERGTTIME:	time required to exeute merge tasks
MERGTCNT:	number of merge tasks

JMATCH:		time required to find matching tuples
MATCHTTIME:	time required to scan through relations


===========
6. Various:
===========

6.1. Compression:
-----------------

The input data tuples are composed of a 64-bit key and an 64-bit record-identifier.
Hence, they are 128 bytes wide. Since the data range of 64 bit integers is not fully
used, several bits of the key and the payload are always zero. Data is compressed by
removing these zero bits. During the partitioning pass, data is compressed into
64-bit tuples.

Partitioning is done by looking at a subset of N bits. Since each node needs to have
at least one partition assigned to it, we can assume that #nodes <= 2^N. All N bits
of the same partition are identical for the join key and can therefore be dropped. As
a result, we are only required to store PAYLOAD_BITS for the key and (PAYLOAD_BITS+N)
bits for the record-identifier.

For the compression to work, the following two conditions need to hold:

(1) N+2*PAYLOAD_BITS <= 64
and
(2) The key/payload data is contained in the range between 0 and 2^(PAYLOAD_BITS+N).

Example: For 1024 (=2^10) nodes, the maximum payload value is 27. The join can support
input keys and record-identifiers between 0 and 2^(27+10) = 2^37 = 137.43 billion.

6.2. Peformance:
----------------

The join algorithms are dependent on a well-tuned communication library. Please take the
time to tune your MPI implementation. Select the MPI implementation with care. Make sure
it supports at least:

* RDMA / OS by-passing / Zero-copy messages
* Asynchronous processing

Not all implementations interleave the processing and the communication. which results in
poor performance causing a significant slow-down of the join algorithm.

Try to pin threads/processes to CPU cores. Avoid sharing the same core or hyperthreads as
both algorithms are sensitive to interference on the last-level caches.

6.3. Sorting/Merging Implementation:
------------------------------------

The sort and merge implementation of the sort-merge join is based on previous work and has
been written by Balkesen et al. [3] as part of the "parallel joins" project at ETH Zurich.

[3] http://www.systems.ethz.ch/projects/paralleljoins

6.4. Tested MPI Version:
------------------------

* openMPI 1.10.1 and 1.20.2
* foMPI 0.2.1

Other versions should be compatible as well. Use the "-D JOIN_DEBUG_PRINT" flag to enable
debug output in case you experience troubles.


========================
7. Copyright & Contacts:
========================

Parts of the partitioning routine of the radix hash join and the sort implementation of the
sort-merge join are modified versions of the code published by Cagri Balkesen. You can find
the full source code and documentation at https://www.systems.ethz.ch/projects/paralleljoins.

Copyright (c) 2016, Claude Barthels
Copyright (c) 2016, Systems Group, ETH Zurich

Contacts: Claude Barthels - claudeb@inf.ethz.ch

www.systems.ethz.ch





