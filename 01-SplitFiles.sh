#!/bin/bash
# ------------------------------------------------------------------- #
#                                                                     #
#	MASTER EN BIG DATA ANALYTICS                                      #
#                                                                     #
#	TRABAJO FINAL DE MASTER                                           #
#                                                                     #
#                                                                     #
#	- CARLOS MARTINEZ GOMEZ                                           #
#	- ENRIQUE CASTELLO FERRE                                          #
#                                                                     #
# ------------------------------------------------------------------- #

# ....................................................................
# Script to split Dataset into small datasets files
# ....................................................................

# Load project utils 
source ./tfm_utils.sh

# Set split suffix length for splited files
suffixLen=3

if [ $verbose -eq 1 ]; then
	verboseOp="--verbose"
	echo "$(date) - Spliting dataset file"
	echo "Suffix length: ${suffixLen}"
	echo "Lines per split: ${splitLines}"
	echo "Dataset Filename: ${absoluteRawDatasetFilename}"
	echo "Output directory: ${absoluteSplitDir}/"
else
	verboseOp=""
fi

# Create result directory if not exists
createDirIfNotExists ${absoluteSplitDir}

# Remove header if dataset file has it
if [ $hasHeader -eq 1 ]; then
	auxDatasetDir=$(dirname ${absoluteRawDatasetFilename})
	auxDatasetFile=$(basename ${absoluteRawDatasetFilename})
	head -1 $absoluteRawDatasetFilename > "${auxDatasetDir}/HEADER-${auxDatasetFile}"
	tail -n +2 $absoluteRawDatasetFilename > "${auxDatasetDir}/DATA-${auxDatasetFile}"
	auxAbsoluteRawDatasetFilename="${auxDatasetDir}/DATA-${auxDatasetFile}"
else
	auxAbsoluteRawDatasetFilename=$absoluteRawDatasetFilename
fi

# Split dataset in small dataset files
split --numeric-suffixes=1 --suffix-length=${suffixLen} --lines=${splitLines} $auxAbsoluteRawDatasetFilename ""/${absoluteSplitDir}/

number_splits=$(find ${absoluteSplitDir} -maxdepth 1 -type f | wc -l )

if [ $verbose -eq 1 ]; then
	echo "$(date) - Dataset splited in files ${number_splits}"
fi
