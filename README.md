# HPC-Joins - Two scalable join implementations using MPI

HPC-Joins consists of two join implementations: A distributed radix hash join and a distributed sort-merge join. Both algorithms have been designed from the ground up to use MPI-3 as their communication library and rely heavily on one-sided RMA (Remote Memory Access) operations. MPI is the primary communication interface used in HPC (High-Performance Computing) systems and is available on a large array of different architectures, ranging from small compute clusters to large supercomputers.

## Installation

Each algorithm comes with two Makefiles. The first one can be used on regular x86 machines. It compiles the join implementation and links it to the default MPI implementation that is available on your system. The second Makefile,called ''MakefileCray'', is intended to be used on Cray Supercomputers where foMPI is available. foMPI is a MPI RMA library that, for intra-node communication, uses XPMEM, a Linux kernel module that enables mapping memory of one process into the virtual address space of another, and, for inter-node communication, uses DMAPP, a low-level networking interface of the Aries network. For performance reasons, it is highly recommended to use foMPI over the default MPI implementation. Detailed step-by-step instructions can be found in the ''INSTRUCTIONS.txt'' file.

```sh
$ make all # Builds the program
$ make clean # Removes the build and output folders
```
## Citing HPC-Joins in Academic Publications

These join algorithms have been created in the context of my work on parallel and distributed join algorithms. Detailed project descriptions can be found in two papers published at ACM SIGMOD 2015 and VLDB 2017. Further publications concerning the use of MPI and RDMA have been submitted to several leading systems conferences and are currently under review. Therefore, for the time being, please refer to the publications listed below when referring to this library.

Claude Barthels, Simon Loesing, Gustavo Alonso, Donald Kossmann.
**Rack-Scale In-Memory Join Processing using RDMA.**
*Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data, June 2015.*  
**PDF:** http://barthels.net/publications/barthels-sigmod-2015.pdf


Claude Barthels, Ingo Müller, Timo Schneider, Gustavo Alonso, Torsten Hoefler.
**Distributed Join Algorithms on Thousands of Cores.**
*Proceedings of the VLDB Endowment, Volume 10, Issue 5, January 2017.*  
**PDF:** http://barthels.net/publications/barthels-vldb-2017.pdf

---

```
@inproceedings{barthels-sigmod-2015,
  author    = {Claude Barthels and
               Simon Loesing and
               Gustavo Alonso and
               Donald Kossmann},
  title     = {Rack-Scale In-Memory Join Processing using {RDMA}},
  booktitle = {{SIGMOD}},
  pages     = {1463--1475},
  year      = {2015},
  url       = {http://doi.acm.org/10.1145/2723372.2750547}
}
```



```
@article{barthels-pvldb-2017,
  author    = {Claude Barthels and
               Ingo M{\"{u}}ller and
               Timo Schneider and
               Gustavo Alonso and
               Torsten Hoefler},
  title     = {Distributed Join Algorithms on Thousands of Cores},
  journal   = {{PVLDB}},
  volume    = {10},
  number    = {5},
  pages     = {517--528},
  year      = {2017},
  url       = {http://www.vldb.org/pvldb/vol10/p517-barthels.pdf}
}
```