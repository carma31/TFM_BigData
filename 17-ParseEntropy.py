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
# 
# ....................................................................


# Load project utils script
from tfm_utils import *
import math

# Get current time to monitorize execution time
executionStartTime = time.time()

# Import Python Spark Context
from pyspark import SparkContext


# Get current time to monitorize execution time
executionStartTime = time.time()

# Spark Context Inizalization
sc = SparkContext(appName = TFM_appName)

# Read entropy file
rawData = sc.textFile(absoluteReClusteringEntropyFileName)
splitedData = rawData.map(lambda r: r.split(csvDelimiter))
parsedData = splitedData.map(lambda r: [int(r[0]), int(r[1]), str(r[2]), int(r[3]), float(r[4])])
allData = parsedData.map(lambda r: ((r[0], r[1], r[2]), (r[3], r[4])))
compTupleData = parsedData.map(lambda r: ((r[0], r[1], r[2]), r[3]))
entTupleData = parsedData.map(lambda r: ((r[0], r[1], r[2]), r[4]))
max2NComp = compTupleData.reduceByKey(lambda a, b: max(a, b))
minEnt = entTupleData.reduceByKey(lambda a, b: min(a, b))

joinedData = max2NComp.join(minEnt)
allJoinedData = allData.join(joinedData)

setIndicatorsData = allJoinedData.map(lambda r: ((r[0][0], r[0][1], r[0][2], r[1][0][0]),\
	(r[1][0][1], 1 if r[1][0][0] == r[1][1][0] else 0, 1 if r[1][0][1] == r[1][1][1] else 0)))
sortedData = setIndicatorsData.sortByKey()


parsedSortedData = sortedData.map(lambda r: [str(r[0][0]), str(r[0][1]), 'Completa' if r[0][2] == 'f' else 'Diagonal', str(r[0][3]),\
	("%.6f")%r[1][0], str(r[1][1]), str(r[1][2])])
csvLineData = parsedSortedData.map(toCSVLine)

result = csvLineData.collect()

# Open result file
parsedEntropy = open(absoluteReClusteringParsedEntropyFileName, "w")

for line in result:
	parsedEntropy.write(line + "\n")

parsedEntropy.close()

"""
flag = True
prevSig = 0
prevClustComp = 0
prevMt = 'n'
prevReClustComp = 0
prevEntropy = 0







for line in f:
	
	fields = line.split(",")
	significancy = int(fields[0])
	clustComp = int(fields[1])
	mt = 'Diagonal' if fields[2] == 'd' else 'Completa'
	reClustComp = int(fields[3])
	entropy = float(fields[4])


	if flag:
		flag = False
	else:	
		if prevSig != significancy or prevClustComp != clustComp or prevMt != mt:
			is_max = 1	
		else:
			is_max = 0

		result.write(str(prevSig) + " " + str(prevClustComp) + " " + prevMt + " " + str(prevReClustComp) + " ")
		result.write(("%.6f"% prevEntropy) + " " + str(is_max) + "\n")

	prevSig = significancy
	prevClustComp = clustComp
	prevMt = mt
	prevReClustComp = reClustComp
	prevEntropy = entropy

result.write(str(significancy) + " " + str(clustComp) + " " + mt + " " + str(reClustComp) + " ")
result.write(("%.6f"% entropy) + " 1\n")

f.close()
result.close()
#result2.close()
"""

# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
	print getExecutionTimeMsg(executionStartTime, executionEndTime)
