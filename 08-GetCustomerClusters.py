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
# Reduce classifications per customer for each model
# ....................................................................


# Load project utils script
from tfm_utils import *

# Import Python Spark Context
from pyspark import SparkContext

# Import aux modules
from operator import is_not
from functools import partial

# Get current time to monitorize execution time
executionStartTime = time.time()


# Spark Context Inizalization
sc = SparkContext()

# Get all models evalutions
mEvaluations = sorted(getFilesInDir(absoluteClusteringClassResDir))

# For each model evaluation reduce by Customer Id to get all Clusters of customer
for modelEval in mEvaluations:
	
	# Get current time to monitorize parse model evaluation time
	modelEvalStartTime = time.time()

	if verbose:
		print getCurrentDateTimeString() + " - Reducing model evaluation " + modelEval + " results"

	# Get cluster components
	nClusters = int(modelEval.split(".")[0])

	# Read model evalutaion
	rawData = sc.textFile(absoluteClusteringClassResDir + "/" + modelEval)

	# Split csv data
	data = rawData.map(lambda row: row.split(csvDelimiter))

	# Get tuple (CUSTOMER_ID, LIST(COUNTER_SAMPLES_IN_COMPONENT))
	dataTuple = data.map(lambda row: (row[0], [1 if x == int(row[1]) else 0 for x in xrange(nClusters)]))
	
	# Reduce data per CUSTOMER_ID
	customerData = dataTuple.reduceByKey(lambda a, b: tuple([a[x] + b[x] for x in xrange(nClusters)]))
	
	# Transform tuples in CSV lines
	customerDataCSV = customerData.map(toCSVLine)
	
	# Get data
	res = customerDataCSV.collect()

	# Write result data 
	writeCSV(absoluteClusteringClassResDir + '/' +  perCustomerClustFileName + '-' + modelEval, res)

	# Get customers clusters
	customerClustersData = customerData.map(lambda row: (row[0], filter(partial(is_not, None), [i if row[1][i] != 0 else None for i in xrange(nClusters)]),\
		filter(None, [row[1][i] if row[1][i] != 0 else None for i in xrange(nClusters)])\
	))
	
	# Transform tuples in CSV lines
	customerClusterDataCSV = customerClustersData.map(toCSVLine) 

	# Get data
	res = customerClusterDataCSV.collect()

	# Write result data
	writeCSV(absoluteClusteringClassResDir + '/' + clustersPerCustomerClustFileName +'-' + modelEval, res)

	# Add counter to customer samples with clusters with key
	groupCont = customerClustersData.map(lambda row: (str(row[1]), 1))
	
	# Count occurriencies of group of clusters
	numGroups = groupCont.reduceByKey(lambda a, b: a + b)

	# Transform result in CSV lines	
	numGroupsCSV = numGroups.map(toCSVLine)

	# Get data
	res = numGroupsCSV.collect()

	# Wirte result data
	writeCSV(absoluteClusteringClassResDir + '/' + clustersStatisticsFileName +'-' + modelEval, res)

	# Get current time to monitorize parse model evaluation time
	modelEvalEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(modelEvalStartTime, modelEvalEndTime)

# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
		print getExecutionTimeMsg(executionStartTime, executionEndTime)
