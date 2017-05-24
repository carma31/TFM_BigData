#!/bin/bash

if [ ! -f $1 ]; then
	echo "$1 is not a file"
	exit 1
fi

cat $1 | grep n_components | awk '{print $2 " " $9}' > aux.dat

echo "unset log
unset label

set terminal postscript eps enhanced color
set output 'bic_evolution.ps'

set title 'Evolucion BIC'
set ytics auto

set autoscale

set key off

plot 'aux.dat' using 2:xtic(1) with line " | gnuplot

rm -rf aux.dat


