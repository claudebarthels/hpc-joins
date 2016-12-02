
########################################

SOURCE_FILES		= 	src/hpcjoin/main.cpp \
						src/hpcjoin/utils/Thread.cpp \
						src/hpcjoin/data/Relation.cpp \
						src/hpcjoin/data/Window.cpp \
						src/hpcjoin/operators/SortMergeJoin.cpp \
						src/hpcjoin/performance/Measurements.cpp \
						src/hpcjoin/tasks/PartitionTask.cpp \
						src/hpcjoin/tasks/MergeJoinTask.cpp \
						src/hpcjoin/tasks/TwoRunsMergeTask.cpp \
						src/hpcjoin/tasks/MultiRunsMergeTask.cpp \
						src/hpcjoin/tasks/SortTask.cpp \
						src/hpcjoin/tasks/MergeLevelTask.cpp \
						src/hpcjoin/balkesen/sort/avxsort.cpp \
						src/hpcjoin/balkesen/merge/merge.cpp \
						src/hpcjoin/balkesen/merge/avx_multiwaymerge.cpp
						

HEADER_FILES		= 	src/hpcjoin/utils/Debug.h \
						src/hpcjoin/utils/Thread.h \
						src/hpcjoin/core/Configuration.h \
						src/hpcjoin/data/Tuple.h \
						src/hpcjoin/data/CompressedTuple.h \
						src/hpcjoin/data/Relation.h \
						src/hpcjoin/data/Window.h \
						src/hpcjoin/operators/SortMergeJoin.h \
						src/hpcjoin/performance/Measurements.h \
						src/hpcjoin/tasks/Task.h \
						src/hpcjoin/tasks/PartitionTask.h \
						src/hpcjoin/tasks/MergeJoinTask.h \
						src/hpcjoin/tasks/TwoRunsMergeTask.h \
						src/hpcjoin/tasks/MultiRunsMergeTask.h \
						src/hpcjoin/tasks/SortTask.h \
						src/hpcjoin/tasks/MergeLevelTask.h \
						src/hpcjoin/balkesen/sort/avxcommon.h \
						src/hpcjoin/balkesen/sort/avxsort_core.h \
						src/hpcjoin/balkesen/sort/avxsort.h \
						src/hpcjoin/balkesen/merge/merge.h \
						src/hpcjoin/balkesen/merge/avx_multiwaymerge.h
						
				
########################################

PROJECT_NAME		= casm-bin

########################################

MPI_FOLDER			= /opt/openmpi-1.10.2/
COMPILER_FLAGS 		= -O3 -std=c++0x -mavx -lpthread -lpapi -g -ggdb
PAPI_FOLDER			= /opt/papi-5.4.3/

########################################

SOURCE_FOLDER		= src
BUILD_FOLER			= build
RELEASE_FOLDER		= release

########################################
			
OBJECT_FILES		= $(patsubst $(SOURCE_FOLDER)/%.cpp,$(BUILD_FOLER)/%.o,$(SOURCE_FILES))
SOURCE_DIRECTORIES	= $(dir $(HEADER_FILES))
BUILD_DIRECTORIES	= $(patsubst $(SOURCE_FOLDER)/%,$(BUILD_FOLER)/%,$(SOURCE_DIRECTORIES))



########################################

all: program

########################################

$(BUILD_FOLER)/%.o:  $(SOURCE_FILES) $(HEADER_FILES)
	mkdir -p $(BUILD_FOLER)
	mkdir -p $(BUILD_DIRECTORIES)
	$(MPI_FOLDER)/bin/mpic++ $(COMPILER_FLAGS) -c $(SOURCE_FOLDER)/$*.cpp -I $(SOURCE_FOLDER) -I $(PAPI_FOLDER) -o $(BUILD_FOLER)/$*.o

########################################

program: $(OBJECT_FILES)
	mkdir -p $(RELEASE_FOLDER)
	$(MPI_FOLDER)/bin/mpic++ $(OBJECT_FILES) $(COMPILER_FLAGS) -L $(PAPI_FOLDER) -o $(RELEASE_FOLDER)/$(PROJECT_NAME)
	

########################################

clean:
	rm -rf $(BUILD_FOLER)
	rm -rf $(RELEASE_FOLDER)
	
	
########################################

public:
	chmod 777 -R $(BUILD_FOLER)
	chmod 777 -R $(RELEASE_FOLDER)
