#!/bin/bash

# Load project utils 
source ./tfm_utils.sh

# Check number of parameters provided
if [[ $# -lt 2 ]]; then 
	echo "ERROR!!! You must specify clusterling-log-filename and chart-filename"
	echo "- Usage: $(basename $0) <clustering_log_filename> <chart_filename>"
	exit 1
elif [[ $# -gt 2 ]]; then
	echo "WARNING!!! More than 2 parameters provided. From third parameter will be ignored"
fi

# Check if clustering log file exists
if [ ! -f $1 ]; then
	echo "$1 is not a file"
	exit 1
fi

# Create Results Directory if not existss
createDirIfNotExists ${absoluteResultsDirectory}

# Parse clustereing log file to get num of components and bic value
cat $1 | grep n_components | awk '{print $2 " " $9}' > aux.dat

# Plot chart
echo "unset log
unset label

set terminal postscript eps enhanced color
set output '${absoluteResultsDirectory}/$2'

set title 'Evolucion BIC'
set ytics auto

set autoscale

set key off

plot 'aux.dat' using 2:xtic(1) with line " | gnuplot

# Remove tmp data file
rm -rf aux.dat

