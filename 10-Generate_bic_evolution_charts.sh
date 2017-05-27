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
# Script to generate all BIC Evolution charts
# ....................................................................

# Load project utils 
source ./tfm_utils.sh


#reclusteringLogDirName = "ReClustering/ReClusteringLog"
# Clustering Logs Directory Absolute Path

# Get Log Directories
logFiles=$(ls ${absoluteResultsDirectory}/ReClustering | grep Log)

# Create Results Directory if not existss
createDirIfNotExists "${absoluteResultsDirectory}/ReClustering/Charts"

for f in $logFiles; do

	# Generate Chart
	./00-BIC_Evolution_Chart.sh "${absoluteResultsDirectory}/ReClustering/${f}/OUT" "ReClustering/Charts/${f:16}.ps"

	if [ $verbose -eq 1 ]; then
		echo "$(date) - Generated $f chart"
	fi

done
