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

# Spark Context Inizalization
spark_context = SparkContext(appName = TFM_appName)


mle = machine_learning.MLE(covar_type = clust_covar_type, dim = 1, log_dir = absoluteclusteringLogDir, models_dir = absoluteClusteringModelsDirName)

createDirectoryIfNotExists(absoluteClusteringClassResDir)
deleteDirectoryData(absoluteClusteringClassResDir)

models = sorted(getFilesInDir(absoluteClusteringModelsDirName))
for model in models:
	if ".txt" in model:
		if verbose:
			print getCurrentDateTimeString() + " - Evaluating model " + model

		executionModelStartTime = time.time()

		mle.gmm.load_from_text(absoluteClusteringModelsDirName + "/" + model)
		n_components = mle.gmm.n_components
		dataset = open(absoluteCustomerClusterDataset, "r")
		result = open(absoluteClusteringClassResDir + "/" + str(n_components) + ".csv", "w")
		
		for line in dataset:
			sample = line.split(csvDelimiter)
			customer = sample[0]
			sample = numpy.array([float(x) for x in sample[1:]])
			res = mle.gmm.classify(sample)
			result.write(toCSVLine([customer, str(res)]) + "\n")
		
		dataset.close()
		result.close()

		executionModelEndTime = time.time()
		if verbose:
			print getExecutionTimeMsg(executionModelStartTime, executionModelEndTime)



# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
		print getExecutionTimeMsg(executionStartTime, executionEndTime)
