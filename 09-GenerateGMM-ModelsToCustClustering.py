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

# Load numpy
import numpy
# Load custom maching_learning
import machine_learning

# Load project utils script
from tfm_utils import *

# Import Python Spark Context
from pyspark import SparkContext



# Get current time to monitorize execution time
executionStartTime = time.time()


# Results Directory Creation
createDirectoryIfNotExists(absoluteReclusteringDir)


# Delete previus executions data
deleteDirectoryData(absoluteReclusteringDir)

 
normalizedDatasets = sorted(getFilesInDir(absoluteClusteringStatisticsDir))

for f in normalizedDatasets:
	if "perCustomer-" in f:
		if "-0001" in f:
			continue

		rawData = spark_context.textFile(absoluteClusteringStatisticsDir + "/" + f)
		removeBraket = rawData.map(lambda row: row.replace("[", "").replace("]", ""))
		parsedData = removeBraket.map(lambda row: row.split(csvDelimiter)) 
		removeCust = parsedData.map(lambda row: tuple([round(float(row[i]), normalizationPrecision) for i in xrange(1, len(row))]))

		reclusteringDataCSV = removeCust.map(toCSVLine)

		res = reclusteringDataCSV.collect()

		percentageSigIdx = f.index("-") + 1
		compNumIdx = f.index(".")
		auxFileName = f[percentageSigIdx:compNumIdx]

		writeCSV(absoluteReclusteringDir + '/' + auxFileName, res)

		if verbose:
			print getCurrentDateTimeString() + " - Parsed file " + auxFileName


# Get training data
trainingFiles = sorted(getFilesInDir(absoluteReclusteringDir))


for trainingFile in trainingFiles:

	# Get current time to parsing file
	parsingStartTime = time.time()

	# Spark Context Inizalization
	spark_context = SparkContext(appName = TFM_appName)

	if verbose:
		print getCurrentDateTimeString() + " - Parsing file " + trainingFile


	"""
		Load all the lines in a file (or files in a directory) into an RDD of text lines.

		It is assumed there is no header, each text file contains an undefined number or lines.
		- Each line represents a sample.
		- All the lines **must** contain the same number of values.
		- All the values **must** be numeric, integers or real values.
	"""

	text_lines = spark_context.textFile(absoluteReclusteringDir + "/" + trainingFile)


	if verbose:
		print getCurrentDateTimeString() + " - file(s) loaded"

	# Get number of samples in dataset
	text_lines.persist()
	num_samples = text_lines.count()
	text_lines.unpersist()

	if verbose:
		print getCurrentDateTimeString() + " - loaded " + str(num_samples) + " samples"


	"""
		Convert the text lines into numpy arrays.

		Taking as input the RDD text_lines, a map operation is applied to each text line in order
		to convert it into a numpy array, as a result a new RDD of numpy arrays is obtained.
	
		Nevertheless, as we need an RDD with blocks of samples instead of single samples, we 
		associate with each sample a random integer number in a specific range.
	
		So, instead of an RDD with of numpy arrays we get an RDD with tuples [ int, numpy.array ]
	"""

	K = (num_samples + reclust_batch_size - 1)/reclust_batch_size
	K = ((K//reclust_slices) + 1) * reclust_slices
	samples = text_lines.map(lambda line: (numpy.random.randint(K), numpy.array([float(x) for x in line.split(csvDelimiter)])))

	"""
	# Shows an example of each element in the temporary RDD of tuples [key, sample]
	if verbose:
		print getCurrentDateTimeString() + " - Example of element in the temporay RDD of tuples [key, sample]"
		print("	" + str(samples.first()))
		print("	" + str(type(samples.first())))
	"""


	"""
		Thanks to the random integer number used as key we can build a new RDD of blocks
		of samples, where each block contains approximately the number of samples specified
		in batch_size.
	"""
	samples = samples.reduceByKey(lambda x, y: numpy.vstack([x, y]))


	# Repartition if necessary
	if samples.getNumPartitions() < reclust_slices:
		samples = samples.repartition(reclust_slices)

	"""
		if verbose:
			print getCurrentDateTimeString() + " - rdd repartitioned to " + str(samples.getNumPartitions()) + " partitions"

	# Shows an example of each element in the temporary RDD of tuples [key, block of samples]
	if verbose:
		print getCurrentDateTimeString() + " - Example of element in the temporay RDD of tuples [key, block of sample]"
		print("	" + str(samples.first()))
		print("	" + str(type(samples.first())))
	"""

	"""
		Convert the RDD of tuples to the definitive RDD of blocks of samples
	"""
	samples = samples.map(lambda x: x[1])

	"""
	# Shows an example of each element in the temporary RDD of blocks of samples
	if verbose:
		print getCurrentDateTimeString() + " - Example of element in the temporay RDD of blocks of samples"
		print("	" + str(samples.first()))
		print("	" + str(type(samples.first())))
	
	"""

	samples.persist()
	
	"""
	if verbose:
		print getCurrentDateTimeString() + " - we are working with " + str(samples.count()) + " blocks of approximately " + str(reclust_batch_size) + " samples"
		# Shows an example of shape of the elements in the temporary RDD of blocks of samples
		print getCurrentDateTimeString() + " - " + str(samples.first().shape)
	"""


	# Gets the dimensionality of samples in order to create the object of the class MLE.
	dim_x = samples.first().shape[1]

	for cType in reclust_covar_types:
		covarType = cType[0]
 
		auxLogDirName = absoluteReclusteringLogDirName + "-" + trainingFile + "-" + cType[1]
		auxModelsDirName = absoluteReclusteringModelsDirName + "-" + trainingFile + "-" + cType[1]

		# Models and Logs Directories Creation
		createDirectoryIfNotExists(auxLogDirName)
		createDirectoryIfNotExists(auxModelsDirName)

		# Delete previus executions data
		deleteDirectoryData(auxLogDirName)
		deleteDirectoryData(auxModelsDirName)

		if verbose:
			print getCurrentDateTimeString() + " - Working with " + covarType + " matrix covariance type"


		# Create MLE class
		mle = machine_learning.MLE(covar_type = covarType, dim = dim_x, log_dir = auxLogDirName, models_dir = auxModelsDirName)
	
		try:
			# Fit clusters
			mle.fit_with_spark(spark_context = spark_context, samples = samples, max_components = reclust_max_components )
		
		except Exception:
			print getCurrentDateTimeString() + " - An exception has been thrown"

	samples.unpersist()
	spark_context.stop()
	

	parsingEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(parsingStartTime, parsingEndTime)


# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
	print getExecutionTimeMsg(executionStartTime, executionEndTime)
