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

# Results Directory Creation
createDirectoryIfNotExists(absoluteReClusteringClassificationsDir)

# Delete previus executions data
deleteDirectoryData(absoluteReClusteringClassificationsDir)

# Get all generated models
absoluteReclusteringDir = absoluteResultsDir + "/ReClustering/"
reclusteringDirectories = sorted(getDirectoriesInDir(absoluteReclusteringDir))
reclusteringModelsDirectories = filter(lambda x: "Model" in x, reclusteringDirectories)


for modelDir in reclusteringModelsDirectories:
	models = sorted(getFilesInDir(absoluteReclusteringDir + "/" + modelDir))
	models = filter(lambda x: "gmm" in x and "0001" not in x, models)
	
	for model in models:		

		if verbose:
			print getCurrentDateTimeString() + " - Evaluating model " + modelDir + "/" + model

		# Get current time to monitorize model evaluation time
		executionModelStartTime = time.time()

		aux = modelDir.split("-")
		str_significancy = aux[1]
		str_n_comp = aux[2]
		mt = aux[3]
		
		# Initialize MLE Class
		gmm = machine_learning.GMM()

		# Load generated model from text file
		gmm.load_from_text(absoluteReclusteringDir + "/" + modelDir + "/" + model)
		
		# Get sample components
		n_components = gmm.n_components
		
		dataset = open(absoluteReclusteringFullDataDir + "/" + str_significancy + "-" + str_n_comp, "r")
		result = open(absoluteReClusteringClassificationsDir + "/" + str_significancy + "-" + str_n_comp + "-" + mt + "-" + str(n_components).zfill(4) + ".csv", "w")

		# For each sample classify on model		
		for line in dataset:
			sample = line.split(csvDelimiter)
			# Get Customer Id
			customer = sample[0]
			# Create numpy array with sample data
			sample = numpy.array([float(x) for x in sample[1:]])
			
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
