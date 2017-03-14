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
# Parse customer readings file to get all readings per day in the 
# same row and save data in files per custmer and per day
# ....................................................................


# Load project utils script
from tfm_utils import *


# Import Python Spark Context
from pyspark import SparkContext

# Get current time to monitorize execution time
startTime = time.time()

# Spark Context Inizalization
sc = SparkContext()


# Get customer files
customerFiles = sorted(getFilesInDir(absoluteCustomerFilesDir))


# Result Directory Creation
createDirectoryIfNotExists(absoluteCustomer24CDir)
createDirectoryIfNotExists(absoluteDay24CDir)

# Delete previus executions data
if deleteDataPrevExec:
	deleteDirectoryData(absoluteCustomer24CDir)
if deleteDataPrevExec:
	deleteDirectoryData(absoluteDay24CDir)


if verbose:
	print getCurrentDateTimeString() + " - Parsing files in " + absoluteCustomerFilesDir

# For each customer file group data per reading hours and then group data per day 
# with all reading in one row
for customerFile in customerFiles:
	
	# Get current time to monitorize parse file execution time
	custFileStartTime = time.time()
	
	if verbose:
		print getCurrentDateTimeString() + " - Parsing file " + customerFile

	# Read customer readings file
	customerData = sc.textFile(absoluteCustomerFilesDir + "/" + customerFile)

	# Parse rows to get ParsedRelevantData named tuples
	parsedRelevantData = customerData.map(parseParsedRelevantData)

	# Prepare data to reduce by key
	toReducedParsedRelevantData = parsedRelevantData.map(lambda r: ((r.CUSTOMER_ID, r.READING_YEAR, r.READING_MONTH, r.READING_DAY, r.READING_HOUR), r.GENERAL_SUPPLY_KWH))

	# Reduce data by key to get total reading per hour
	reducedData = toReducedParsedRelevantData.reduceByKey(lambda a, b: a + b)

	# Parse data to get ParsedRelevantData named tuples
	reducedParsedRelevantData = reducedData.map(lambda t: ParsedRelevantData(t[0][0], t[0][1], t[0][2], t[0][3], t[0][4], t[1]))

	# Parse data to assign reading to its hour position
	to24C = reducedParsedRelevantData.map(lambda r: ((r.CUSTOMER_ID, r.READING_YEAR, r.READING_MONTH, r.READING_DAY), (\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 0 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 1 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 2 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 3 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 4 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 5 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 6 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 7 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 8 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 9 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 10 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 11 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 12 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 13 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 14 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 15 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 16 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 17 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 18 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 19 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 20 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 21 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 22 else 0,\
		 r.GENERAL_SUPPLY_KWH if r.READING_HOUR == 23 else 0\
	)))

	# Reduce Rows by key to get one row per day
	reducedTo24C = to24C.reduceByKey(lambda a, b: (\
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
		a[23] + b[23]\
	))

	# Sort data per date using key (CUSTOMER_ID, READING_YEAR, READING_MONTH, READING_DAY)
	sortedReadingDayData = reducedTo24C.sortByKey()

	# Get day of week
	readingDayData = sortedReadingDayData.map(lambda r: [r[0][0], r[0][1], r[0][2], r[0][3], getDayOfWeek(int(r[0][1]), int(r[0][2]), int(r[0][3])),\
		r[1][0], r[1][1], r[1][2], r[1][3], r[1][4], r[1][5],\
		r[1][6], r[1][7], r[1][8], r[1][9], r[1][10], r[1][11],\
		r[1][12], r[1][13], r[1][14], r[1][15], r[1][16], r[1][17],\
		r[1][18], r[1][19], r[1][20], r[1][21], r[1][22], r[1][23]\
	])
	
	# Transform tuples in CSV lines
	readingDayDataCSV = readingDayData.map(toCSVLine)

	# Get all file data
	res = readingDayDataCSV.collect()

	# Save data in Customer and Day files
	cust_res_file = open(absoluteCustomer24CDir + "/" + customerFile, "a")
	for l in res:
		aux = l.split(",")
		
		cust_res_file.write(l + "\n")
		
		date_res_file = open(absoluteDay24CDir + "/" + aux[1] + (str(aux[2].strip()).zfill(2)) + (str(aux[3].strip()).zfill(2)) + ".csv", "a")
		date_res_file.write(l + "\n")		
		date_res_file.close()

	cust_res_file.close()

	# Get current time to monitorize parse file execution time
	custFileEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(custFileStartTime, custFileEndTime)

# Get current time to monitorize execution time
endTime = time.time()

if verbose:
	print getExecutionTimeMsg(startTime, endTime)
