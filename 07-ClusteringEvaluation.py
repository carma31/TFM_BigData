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
# Evaluate GMM models to get the cluster of each sample on echa model
# ....................................................................


# Load numpy
import numpy
# Load custom maching_learning
import machine_learning


# Load project utils script
from tfm_utils import *

# Get current time to monitorize execution time
executionStartTime = time.time()

# Initialize MLE Class
gmm = machine_learning.GMM()

# Results Directory Creation
createDirectoryIfNotExists(absoluteClusteringClassResDir)

# Delete previus executions data
deleteDirectoryData(absoluteClusteringClassResDir)

# Get all generated models
models = sorted(getFilesInDir(absoluteClusteringModelsDirName))

# For each model classify each sample
for model in models:
	if ".txt" in model:
		if verbose:
			print getCurrentDateTimeString() + " - Evaluating model " + model

		# Get current time to monitorize model evaluation time
		executionModelStartTime = time.time()

		# Load generated model from text file
		gmm.load_from_text(absoluteClusteringModelsDirName + "/" + model)

		# Get sample components
		n_components = gmm.n_components

		dataset = open(absoluteFullClusterDataset, "r")
		result = open(absoluteClusteringClassResDir + "/" + str(n_components).zfill(4) + ".csv", "w")

		# For each sample classify on model		
		for line in dataset:
			sample = line.split(csvDelimiter)
			# Get Customer Id
			customer = sample[0]
			# Create numpy array with sample data
			sample = numpy.array([float(x) for x in sample[5:]])
			# Classify sample
			res = gmm.classify(sample)
			# Write result in result file
			result.write(toCSVLine([customer, str(res)]) + "\n")
		
		dataset.close()
		result.close()

		# Get current time to monitorize model evaluation time
		executionModelEndTime = time.time()
		if verbose:
			print getExecutionTimeMsg(executionModelStartTime, executionModelEndTime)



# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
		print getExecutionTimeMsg(executionStartTime, executionEndTime)
