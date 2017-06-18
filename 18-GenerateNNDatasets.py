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

# Create results directory
createDirectoryIfNotExists(absoluteNnDatasets)


models = [\
	(122, 70, 'd', 2),\
	(122, 70, 'd', 4),\
	(122, 80, 'd', 2),\
	(122, 80, 'd', 4),\
	(154, 75, 'd', 4),\
	(154, 90, 'd', 2),\
	(192, 75, 'd', 2),\
	(192, 75, 'd', 4),\
	(192, 90, 'd', 4),\
	(122, 80, 'f', 2),\
	(154, 80, 'f', 2),\
	(192, 75, 'f', 2)
]


for model in models:
	# Get current time to parsing file
	parsingStartTime = time.time()

	n_comp = model[0]
	str_n_comp = str(n_comp).zfill(4)
	sign = model[1]
	str_sign = str(sign).zfill(3)
	mt = model[2]
	n_comp_2n = model[3]
	str_n_comp_2n = str(n_comp_2n).zfill(4)
	className = str_sign + "-" + str_n_comp + "-" + mt + "-" + str_n_comp_2n
	
	if verbose:
		print getCurrentDateTimeString() + " - Generating dataset " + className

	createDirectoryIfNotExists(absoluteNnDatasets + "/" + str_n_comp)
	createDirectoryIfNotExists(absoluteNnDatasets + "/" + str_n_comp + "/" + mt)
	createDirectoryIfNotExists(absoluteNnDatasets + "/" + str_n_comp + "/" + mt + "/" + str_sign)
	createDirectoryIfNotExists(absoluteNnDatasets + "/" + str_n_comp + "/" + mt + "/" + str_sign + "/" + str_n_comp_2n)

		
	for n in xrange(n_comp_2n):
		dirname = absoluteNnDatasets + "/" + str_n_comp + "/" + mt + "/" + str_sign + "/" + str_n_comp_2n + "/" + str(n + 1)
		createDirectoryIfNotExists(dirname)

		f = open(dirname + "/dataset-aux.csv", "w")
		inputF = open(absoluteReClusteringClassificationsDir + "/" + className + ".csv", "r")

		for line in inputF:
			customer = line.split(csvDelimiter)[0]
			comp = int(line.split(csvDelimiter)[1])
			if comp != n:
				continue

			customerFile = open(absoluteCustomer24CDir + "/" + customer + ".csv", "r")
			for l in customerFile:
				f.write(l)
			customerFile.close()
		
		
		inputF.close()
		f.close()

		# Spark Context Inizalization
		sc = SparkContext(appName = TFM_appName)

		rawData = sc.textFile(dirname + "/dataset-aux.csv")
		parsedData = rawData.map(parseDayReadingData)
		withKeyData = parsedData.map(lambda r: ((int(r.READING_YEAR), int(r.READING_MONTH), int(r.READING_DAY), int(r.READING_DAY_OF_WEEK)), (\
			float(r.READING_00), float(r.READING_01), float(r.READING_02), float(r.READING_03), float(r.READING_04), float(r.READING_05),\
			float(r.READING_06), float(r.READING_07), float(r.READING_08), float(r.READING_09), float(r.READING_10), float(r.READING_11),\
			float(r.READING_12), float(r.READING_13), float(r.READING_14), float(r.READING_15), float(r.READING_16), float(r.READING_17),\
			float(r.READING_18), float(r.READING_19), float(r.READING_20), float(r.READING_21), float(r.READING_22), float(r.READING_23),\
			1)))

		sumData = withKeyData.reduceByKey(lambda a, b: (\
			a[0] + b[0],\
			a[1] + b[1],\
			a[2] + b[2],\
			a[3] + b[3],\
			a[4] + b[4],\
			a[5] + b[5],\
			a[6] + b[6],\
			a[7] + b[7],\
			a[8] + b[8],\
			a[9] + b[9],\
			a[10] + b[10],\
			a[11] + b[11],\
			a[12] + b[12],\
			a[13] + b[13],\
			a[14] + b[14],\
			a[15] + b[15],\
			a[16] + b[16],\
			a[17] + b[17],\
			a[18] + b[18],\
			a[19] + b[19],\
			a[20] + b[20],\
			a[21] + b[21],\
			a[22] + b[22],\
			a[23] + b[23],\
			a[24] + b[24]
		))

		sortedData = sumData.sortByKey()

		meanData = sortedData.map(lambda r: [r[0][0], r[0][1], r[0][2], r[0][3],\
			r[1][0]/r[1][24],\
			r[1][1]/r[1][24],\
			r[1][2]/r[1][24],\
			r[1][3]/r[1][24],\
			r[1][4]/r[1][24],\
			r[1][5]/r[1][24],\
			r[1][6]/r[1][24],\
			r[1][7]/r[1][24],\
			r[1][8]/r[1][24],\
			r[1][9]/r[1][24],\
			r[1][10]/r[1][24],\
			r[1][11]/r[1][24],\
			r[1][12]/r[1][24],\
			r[1][13]/r[1][24],\
			r[1][14]/r[1][24],\
			r[1][15]/r[1][24],\
			r[1][16]/r[1][24],\
			r[1][17]/r[1][24],\
			r[1][18]/r[1][24],\
			r[1][19]/r[1][24],\
			r[1][20]/r[1][24],\
			r[1][21]/r[1][24],\
			r[1][22]/r[1][24],\
			r[1][23]/r[1][24],\
		])

		# Get all file data
		res = meanData.collect()

		# Save data 
		res_file = open(dirname + "/dataset.csv", "w")
		for l in res:
		
			res_file.write(toCSVLine(l) + "\n")
		
		res_file.close()

		os.remove(dirname + "/dataset-aux.csv)

		sc.stop()
	
	# Get current time to parsing file
	parsingEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(parsingStartTime, parsingEndTime)

# Get current time to monitorize execution time
executionEndTime = time.time()
if verbose:
	print getExecutionTimeMsg(executionStartTime, executionEndTime)
