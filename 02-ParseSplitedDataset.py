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
# Parse splited dataset file to drop unused info and write data
# in files per Customer and files per day
# ....................................................................


# Load project utils script
from tfm_utils import *


# Import Python Spark Context
from pyspark import SparkContext

# Get current time to monitorize execution time
startTime = time.time()

# Spark Context Inizalization
sc = SparkContext()


# Select split parts. 
# -1 indicate all
splitSelectionIni = -1
splitSelectionEnd = -1

# Get splited files 
splitedFiles = sorted(getFilesInDir(absoluteSplitDir))

# Filter to get selected splited files
selectedSplits = [f for f in splitedFiles if (splitSelectionIni < 0 or (splitSelectionIni >= 0 and int(f) >= splitSelectionIni)) and\
	(splitSelectionEnd < 0 or (splitSelectionEnd >= 0 and int(f) <= splitSelectionEnd))]

# Result Directory Creation
createDirectoryIfNotExists(absoluteCustomerFilesDir)

# Delete previus executions data
if deleteDataPrevExec:
	deleteDirectoryData(absoluteCustomerFilesDir)

if verbose:
	print getCurrentDateTimeString() + " - Parsing file in " + absoluteSplitDir

# For each split file parse data to get relevant data and save readings per Customer
for splitFile in selectedSplits:
	
	# Get current time to monitorize parse file execution time
	splitStartTime = time.time()
	
	if verbose:
		print getCurrentDateTimeString() + " - Parsing file " + splitFile

	# Read splited file
	splitData = sc.textFile(absoluteSplitDir + "/" + splitFile)

	# Parse raw data to get RawDatasetRow named tuples
	rawRow = splitData.map(lambda row: parseRawDatasetRow(row))
	
	# Filter data to get data with valid reading date-time data
	filteredRawData = rawRow.filter(lambda r: len(r.READING_DATETIME.split(" ")) >= 2)

	# Parse readings to get reading date and reading hour separate to get RelevantData named tuple
	relevantData = filteredRawData.map(lambda r: RelevantData(r.CUSTOMER_ID, getReadingDate(r.READING_DATETIME), getReadingTime(r.READING_DATETIME), r.GENERAL_SUPPLY_KWH))

	# Parse RelevantData tuples to get reaging hour and reading date with reading minutes
	relevantDataHour = relevantData.map(lambda r: RelevantData(r.CUSTOMER_ID, parseReadingDate(r.READING_DATE, r.READING_TIME),\
		parseReadingTime(r.READING_TIME), parseReading(r.GENERAL_SUPPLY_KWH)))
	
	# Parse RelevantData tuplees to get reading date in year, month and day separated fields
	parsedRelevantData = relevantDataHour.map(lambda r: ParsedRelevantData(r.CUSTOMER_ID, getYear(r.READING_DATE), getMonth(r.READING_DATE),\
		getDay(r.READING_DATE), r.READING_TIME, r.GENERAL_SUPPLY_KWH))

	# Transform tuples in CSV lines
	CSVData = parsedRelevantData.map(toCSVLine)

	# Get all file data
	res = CSVData.collect()

	# Write data in a CSV file per customer
	for l in res:
		aux = l.split(",")
		res_file = open(absoluteCustomerFilesDir + "/" + aux[0] + ".csv", "a")
		res_file.write(l + "\n")
		res_file.close()

	# Get current time to monitorize parse file execution time
	splitEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(splitStartTime, splitEndTime)

# Get current time to monitorize execution time
endTime = time.time()

if verbose:
	print getExecutionTimeMsg(startTime, endTime)
