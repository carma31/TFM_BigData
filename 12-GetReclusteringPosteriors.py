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
# Evaluate GMM models to get the posterior priority of GMM model
# for each sample
# ....................................................................

# Load numpy
import numpy
# Load custom maching_learning
import machine_learning

from math import log

# Load project utils script
from tfm_utils import *

# Get current time to monitorize execution time
executionStartTime = time.time()

# Initialize GMM model
gmm = machine_learning.GMM()


# Results Directory Creation
createDirectoryIfNotExists(absoluteReClusteringPosteriorsDir)

# Delete previus executions data
deleteDirectoryData(absoluteReClusteringPosteriorsDir)


reClusteringDir = absoluteResultsDir + "/ReClustering"

# Get all generated models

directories = sorted(getDirectoriesInDir(reClusteringDir))
modelsDir = filter(lambda d: "ReClusteringModels" in d, directories)

modelsDir = filter(lambda d: not ("239" in d or "296" in d), modelsDir)

models = []
for d in modelsDir:
	modelsAux = sorted(getFilesInDir(reClusteringDir + "/" + d))
	for m in modelsAux:
		if "0001" not in m:
			models.append(d + "/" + m)

entropyFile = open(absoluteReClusteringEntropyFileName, "w")

for model in models:
	if ".txt" in model:
		if verbose:
			print getCurrentDateTimeString() + " - Evaluating model " + model

		aux = model.split("/")
		firstClustModel = aux[0][aux[0].index("-") + 1:]
		modelName = aux[1][aux[1].index("-"): -4]

		# Get current time to monitorize model evaluation time
		executionModelStartTime = time.time()
		
		# Load generated model from text file
		try:
			gmm.load_from_text(reClusteringDir + "/" + model)
		except Exception:
			print "Error loading " + model
			continue
		
		# Get sample components
		n_components = gmm.n_components
		
		dataset = open(absoluteReclusteringFullDataDir + "/" + firstClustModel[:-2], "r")
		result = open(absoluteReClusteringPosteriorsDir + "/" + firstClustModel + modelName + ".csv", "w")
		
		entropy = 0.0
		samples = 0
		components = 0

		# For each sample classify on model		
		for line in dataset:
			samples += 1
			sample = line.split(csvDelimiter)
			# Get Customer Id
			customer = sample[0]
			# Create numpy array with sample data
			sample = numpy.array([float(x) for x in sample[1:]])
			# Classify sample
			res, logl = gmm.posteriors(sample)

			for i in res:
				if i > 0.000001:
					entropy += -i * log(i, 10)

			# Write result in result file
			result.write(toCSVLine([customer] + [("%.6f"% x) for x in res]) + "\n")
		
		components = len(res)
		normEntropy = entropy/(samples*components)

		entropyFile.write(toCSVLine([firstClustModel[0:3], firstClustModel[4:-2], firstClustModel[-1:], modelName[1:], str(normEntropy)]) + "\n")

		dataset.close()
		result.close()

		# Get current time to monitorize model evaluation time
		executionModelEndTime = time.time()
		if verbose:
			print getExecutionTimeMsg(executionModelStartTime, executionModelEndTime)


entropyFile.close()


# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
		print getExecutionTimeMsg(executionStartTime, executionEndTime)
