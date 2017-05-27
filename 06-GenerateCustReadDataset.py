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
# Get all customers files and merge in one file with Customer Id  
# and readings
# ....................................................................


# Load project utils script
from tfm_utils import *

# Import Python Spark Context
from pyspark import SparkContext

# Get current time to monitorize execution time
startTime = time.time()

# Spark Context Inizalization
sc = SparkContext()

# Delete previous execution data
if deleteDataPrevExec:
	if os.path.isfile(absoluteCustomerClusterDataset):
		os.remove(absoluteCustomerClusterDataset)
		if verbose:
			print getCurrentDateTimeString() + " - " + absoluteCustomerClusterDataset + " deleted successfully!!!"

# Get customers files
customerFiles = sorted(getFilesInDir(absoluteCustomer24CDir))

# Initialize lines counter
totalLinesCount = 0

if verbose:
	print getCurrentDateTimeString() + " - Parsing files in " + absoluteClusterDataset

# For each customer file get readings and merge in one result file
for custFile in customerFiles:

	# Get current time to monitorize parse file execution time
	custFileStartTime = time.time()
	
	if verbose:
		print getCurrentDateTimeString() + " - Parsing file " + custFile

	# Read customer file
	customerData = sc.textFile(absoluteCustomer24CDir + "/" + custFile)

	# Parse rows to get DayReadingData
	customerDayReading = customerData.map(parseDayReadingData)

	"""
	# Get Day readings
	dayReading = customerDayReading.map(lambda c: [c.CUSTOMER_ID, c.READING_00, c.READING_01, c.READING_02,\
		c.READING_03, c.READING_04, c.READING_05, c.READING_06, c.READING_07, c.READING_08, c.READING_09, c.READING_10,\
		c.READING_11, c.READING_12, c.READING_13, c.READING_14, c.READING_15, c.READING_16, c.READING_17, c.READING_18,\
		c.READING_19, c.READING_20, c.READING_21, c.READING_22, c.READING_23\
	])

	# Transform tuples in CSV lines
	CSVCustomerDayReading = dayReading.map(toCSVLine)
	"""

	# Transform tuples in CSV lines
	CSVCustomerDayReading = customerDayReading.map(toCSVLine)

	# Get all file data
	data = CSVCustomerDayReading.collect()

	# Write result file data and get lines number
	linesCount = writeCSV(absoluteFullClusterDataset, data, "a")

	# Upadete lines counter
	totalLinesCount += linesCount

	if verbose:
		print getCurrentDateTimeString() + " - Writed " + str(linesCount) + " in " + fullClusterDataset
		print getCurrentDateTimeString() + " - " + custFile + " parsed successfully"

	# Get current time to monitorize parse file execution time
	custFileEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(custFileStartTime, custFileEndTime)


# Get current time to monitorize execution time
endTime = time.time()

if verbose:
	print getCurrentDateTimeString() + " - Writed " + str(linesCount) + " in total"
	print getExecutionTimeMsg(startTime, endTime)
