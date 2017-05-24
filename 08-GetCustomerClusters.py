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


# Results Directory Creation
createDirectoryIfNotExists(absoluteClusteringStatisticsDir)

# Delete previus executions data
deleteDirectoryData(absoluteClusteringStatisticsDir)

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
	#customerDataCSV = customerData.map(toCSVLine)
	
	# Get data
	#res = customerDataCSV.collect()

	# Write result data 
	#writeCSV(absoluteClusteringStatisticsDir + '/' +  perCustomerClustFileName + '-100-' + modelEval, res)
	

	# Get customers clusters
	customerClustersData = customerData.map(lambda row: (row[0], filter(partial(is_not, None), [i if row[1][i] != 0 else None for i in xrange(nClusters)]),\
		filter(None, [row[1][i] if row[1][i] != 0 else None for i in xrange(nClusters)])\
	))
	
	# Transform tuples in CSV lines
	customerClusterDataCSV = customerClustersData.map(toCSVLine) 

	# Get data
	res = customerClusterDataCSV.collect()

	# Write result data
	writeCSV(absoluteClusteringStatisticsDir + '/' + clustersPerCustomerClustFileName + '-100-' + modelEval, res)


	# Normalize cluster occurriencies
	auxRdd = customerData.map(lambda row: (row[0], row[1], sum(row[1])))
	normalizedData = auxRdd.map(lambda row: (row[0], [float(i)/row[2] for i in row[1]]))

	# Transform tuples in CSV lines
	normalizedDataCSV = normalizedData.map(toCSVLine)
	
	# Get data
	res = normalizedDataCSV.collect()

	# Write result data
	writeCSV(absoluteClusteringStatisticsDir + '/' + perCustomerClustFileName + '-100-' + modelEval, res)

 	
	# Zip cluster and ocurrencies data
	zippedCustClusterData = customerData.map(lambda row: (row[0], [(i, row[1][i]) for i in xrange(len(row[1]))], sorted(row[1], key=lambda x: -x), sum(row[1])))
	
	# Sort cluster occurrencies
	orderedZippedCustClusterData = zippedCustClusterData.map(lambda row: (row[0], sorted(row[1], key = lambda x: -x[1]), row[2], row[3]))

	# Get significant clusters with i% amount of data
	for i in xrange(5, 100, 5):
		# Get accumulate list
		auxClusterData = orderedZippedCustClusterData.map(lambda row: (row[0], row[1], row[3], ((row[3] * i)/100),\
			[sum(row[2][0:(j+1)]) for j in xrange(len(row[2]))]))

		# Get significant data
		indexAuxClusterData = auxClusterData.map(lambda row: (row[0], row[1], \
			[1 if row[4][j] < row[3] else 0 for j in xrange(len(row[4]))].index(0)+1))
		signicantClusterData = indexAuxClusterData.map(lambda row: (row[0], row[1][:row[2]], row[1]))
		
		# Covert to List
		listSignificantClusterData = signicantClusterData.map(lambda row: (row[0], [row[1][j][0] for j in xrange(len(row[1]))],\
			[row[1][j][1] for j in xrange(len(row[1]))]))

		# Normalize Data
		auxRDD = signicantClusterData.map(lambda row: (row[0], [j[1] if j in row[1] else 0 for j in sorted(row[2], key=lambda x: x[0])]))
		auxRDD = auxRDD.map(lambda row: (row[0], row[1], sum(row[1])))
		normalizedData = auxRDD.map(lambda row: (row[0], [float(j)/row[2] for j in row[1]]))
		
		# Transform tuples in CSV lines
		normalizedDataCSV = normalizedData.map(toCSVLine)
	
		# Get data
		res = normalizedDataCSV.collect()

		# Write result data
		writeCSV(absoluteClusteringStatisticsDir + '/' + perCustomerClustFileName + '-' + str(i).zfill(3) +'-' + modelEval, res)


		# Transform tuples in CSV lines
		listSignificantClusterDataCSV = listSignificantClusterData.map(toCSVLine)

		# Get data
		res = listSignificantClusterDataCSV.collect()

		# Write result data
		writeCSV(absoluteClusteringStatisticsDir + '/' + clustersPerCustomerClustFileName + '-' + str(i).zfill(3) + '-' + modelEval, res)
		
		# Add counter to customer samples with clusters with key
		groupCont = listSignificantClusterData.map(lambda row: (str(sorted(row[1])), 1))
	
		# Count occurriencies of group of clusters
		numGroups = groupCont.reduceByKey(lambda a, b: a + b)

		# Transform result in CSV lines	
		numGroupsCSV = numGroups.map(toCSVLine)

		# Get data
		res = numGroupsCSV.collect()

		# Wirte result data
		writeCSV(absoluteClusteringStatisticsDir + '/' + clustersStatisticsFileName + '-' + str(i).zfill(3) + '-' + modelEval, res)


	# Add counter to customer samples with clusters with key
	groupCont = customerClustersData.map(lambda row: (str(row[1]), 1))
	
	# Count occurriencies of group of clusters
	numGroups = groupCont.reduceByKey(lambda a, b: a + b)

	# Transform result in CSV lines	
	numGroupsCSV = numGroups.map(toCSVLine)

	# Get data
	res = numGroupsCSV.collect()

	# Wirte result data
	writeCSV(absoluteClusteringStatisticsDir + '/' + clustersStatisticsFileName + '-100-' + modelEval, res)

	# Get current time to monitorize parse model evaluation time
	modelEvalEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(modelEvalStartTime, modelEvalEndTime)

# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
	print getExecutionTimeMsg(executionStartTime, executionEndTime)
