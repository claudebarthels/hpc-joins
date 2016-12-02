
########################################

SOURCE_FILES		= 	src/hpcjoin/main.cpp \
						src/hpcjoin/utils/Thread.cpp \
						src/hpcjoin/data/Relation.cpp \
						src/hpcjoin/data/Window.cpp \
						src/hpcjoin/histograms/LocalHistogram.cpp \
						src/hpcjoin/histograms/GlobalHistogram.cpp \
						src/hpcjoin/histograms/AssignmentMap.cpp \
						src/hpcjoin/histograms/OffsetMap.cpp \
						src/hpcjoin/memory/Pool.cpp \
						src/hpcjoin/operators/HashJoin.cpp \
						src/hpcjoin/performance/Measurements.cpp \
						src/hpcjoin/tasks/HistogramComputation.cpp \
						src/hpcjoin/tasks/NetworkPartitioning.cpp \
						src/hpcjoin/tasks/LocalPartitioning.cpp \
						src/hpcjoin/tasks/BuildProbe.cpp

HEADER_FILES		= 	src/hpcjoin/utils/Debug.h \
						src/hpcjoin/utils/Thread.h \
						src/hpcjoin/core/Configuration.h \
						src/hpcjoin/data/CompressedTuple.h \
						src/hpcjoin/data/Tuple.h \
						src/hpcjoin/data/Relation.h \
						src/hpcjoin/data/Window.h \
						src/hpcjoin/histograms/LocalHistogram.h \
						src/hpcjoin/histograms/GlobalHistogram.h \
						src/hpcjoin/histograms/AssignmentMap.h \
						src/hpcjoin/histograms/OffsetMap.h \
						src/hpcjoin/memory/Pool.h \
						src/hpcjoin/operators/HashJoin.h \
						src/hpcjoin/performance/Measurements.h \
						src/hpcjoin/tasks/Task.h \
						src/hpcjoin/tasks/HistogramComputation.h \
						src/hpcjoin/tasks/NetworkPartitioning.h \
						src/hpcjoin/tasks/LocalPartitioning.h \
						src/hpcjoin/tasks/BuildProbe.h
				
########################################

PROJECT_NAME		= cahj-bin

########################################

MPI_FOLDER			= /opt/openmpi-1.10.2/
COMPILER_FLAGS 		= -O3 -std=c++0x -mavx -lpthread -lpapi -D MEASUREMENT_DETAILS_HISTOGRAM -D MEASUREMENT_DETAILS_NETWORK -D MEASUREMENT_DETAILS_LOCALPART -D MEASUREMENT_DETAILS_LOCALBP
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
	make public
	

########################################

clean:
	rm -rf $(BUILD_FOLER)
	rm -rf $(RELEASE_FOLDER)
	
	
########################################

public:
	chmod 777 -R $(BUILD_FOLER)
	chmod 777 -R $(RELEASE_FOLDER)
