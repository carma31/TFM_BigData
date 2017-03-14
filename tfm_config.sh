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
# Config file with shared configuratin to launch bash scripts
# ....................................................................

#####################################################################
# Directories configurations
#####################################################################

# Main Project Directory
baseDir="/home/kike/Escritorio/TFM"

# Data Directory
dataDir="/Data"

# Data Directory With Absolute Path
absoluteDataDir="${baseDir}${dataDir}"

# Raw Dataset Filename
rawDatasetFilename="CD_INTERVAL_READING_ALL_NO_QUOTES.csv"
# Raw Dataset Filename With Absolute Path
absoluteRawDatasetFilename="${absoluteDataDir}/${rawDatasetFilename}"

# Data Directory with Splited Raw Dataset
splitDir="/Splited"
# Data Directory With Splited Raw Dataset With Absolute Path
absoluteSplitDir="${absoluteDataDir}${splitDir}"


#####################################################################
# Aux variables configuration
#####################################################################
verbose=1

hasHeader=1
splitLines=1000000

