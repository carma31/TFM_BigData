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
# Porject Utils scripts with aux variables and functions
# ....................................................................

#####################################################################
# Required Modules
#####################################################################
import time
import datetime
import os
from collections import namedtuple
import shutil
import sys

from tfm_config import *


#####################################################################
# RDD definitios configuration
#####################################################################

# Raw Dataset Fields
rawDatasetFields = ["CUSTOMER_ID", "READING_DATETIME", "CALENDAR_KEY", "EVENT_KEY", "GENERAL_SUPPLY_KWH",\
	"CONTROLLED_LOAD_KWH", "GROSS_GENERATION_KWH", "NET_GENERATION_KWH", "OTHER_KWH"]
# Named Tuple for Raw Dataset Fields
RawDatasetRow = namedtuple('RawDatasetRow', rawDatasetFields)

# Relevant Dataset Fields
relevantDataFields = ["CUSTOMER_ID", "READING_DATE", "READING_TIME", "GENERAL_SUPPLY_KWH"]
# Named Tuple for Relevant Dataset Fields
RelevantData = namedtuple('RelevantData', relevantDataFields)

# Parsed Relevant Dataset Fields
parsedRelevantDataFields = ["CUSTOMER_ID", "READING_YEAR", "READING_MONTH", "READING_DAY", "READING_HOUR", "GENERAL_SUPPLY_KWH"]
# Named Tuple for Relevant Dataset Fields
ParsedRelevantData = namedtuple('ParsedRelevantData', parsedRelevantDataFields)

# Day Reading Fields
dayReadingDataFields = ['CUSTOMER_ID', 'READING_YEAR', 'READING_MONTH', 'READING_DAY', 'READING_DAY_OF_WEEK',\
	'READING_00', 'READING_01', 'READING_02', 'READING_03', 'READING_04', 'READING_05',\
	'READING_06', 'READING_07', 'READING_08', 'READING_09', 'READING_10', 'READING_11',\
	'READING_12', 'READING_13', 'READING_14', 'READING_15', 'READING_16', 'READING_17',\
	'READING_18', 'READING_19', 'READING_20', 'READING_21', 'READING_22', 'READING_23'\
	]
# Named Tuple for Customer Day Reading Fields
DayReadingData = namedtuple('DayReading', dayReadingDataFields)


# Full Occurrences Data With Total Fields
fullOccurrencesDataTotalFields = ['CUSTOMER_ID', 'OCCURRENCES_LIST', 'TOTAL_SAMPLES']
# Named Tuple for Full Occurrences Data With Total Fields
FullOccurrencesDataTotal = namedtuple('FullOccurrencesDataTotal', fullOccurrencesDataTotalFields)

# Aux Normalization Data Fields
auxNormalizationDataFields = ['CUSTOMER_ID', 'COMPONENT_OCCURRENCIES_TUPLE_LIST', 'DESC_ORDERED_OCCURRENCES_LIST', 'TOTAL_SAMPLES']
# Named Tuple for Aux Normalization Data Fields
AuxNormalizationData = namedtuple('AuxNormalizationData', auxNormalizationDataFields)

#####################################################################
# Aux functions definition
#####################################################################

# Get current time string function
# Params:
# Return:
#	* string	- Current Datetime string
def getCurrentDateTimeString():
	return str(datetime.datetime.now())


# Get Execution Time function
# Params:
#	* startTime	- integer	- Start execution time
#	* endTime	- integer	- End execution time
# Return:
#	* integer	- Execution time
def getExecutionTime(startTime, endTime):
	executionTime = endTime - startTime
	return executionTime


# Get Executio time log message function
# Params:
#	* startTime	- integer	- Start execution time
#	* endTime	- integer	- End execution time
# Return:
#	* string	- Execution time log message
def getExecutionTimeMsg(startTime, endTime):
	executionTime = getExecutionTime(startTime, endTime)
	executionHours = executionTime/3600
	executionMin = (executionTime%3600)/60
	executionSeconds = executionTime%60
	aux = getCurrentDateTimeString() + " - Executed in %.0f hours %.0f minutes %f seconds"
	return (aux % (executionHours, executionMin, executionSeconds))


# Parse day reading data function
# Params:
#	* rawRow	- string - Raw data with fields separateds with csvDelimiter
# Return:
#	* namedtuple	- DayReadingData named tuple
def parseDayReadingData(rawRow):
	dayReadingData = rawRow.split(csvDelimiter)
	dayReadingData += [None] * (len(dayReadingDataFields) - len(dayReadingData))
	return DayReadingData(*dayReadingData)


# Parse raw dataset data function
# Params:
#	* rawRow	- string - Raw data with fields separateds with csvDelimiter
# Return:
#	* namedtuple	- RawDatasetRow named tuple
def parseRawDatasetRow(rawRow):
	rawRowFields = rawRow.split(csvDelimiter)
	rawRowFields += [None] * (len(rawDatasetFields) - len(rawRowFields))
	return RawDatasetRow(*rawRowFields)


# Parse parsed relevant data function
# Params:
#	* rawRow	- string - Raw data with fields separateds with csvDelimiter
# Return:
#	* namedtuple	- ParsedRelevantData named tuple
def parseParsedRelevantData(rawRow):
	rawRowFields = rawRow.split(csvDelimiter)
	rawRowFields += [None] * (len(parsedRelevantDataFields) - len(rawRowFields))
	rawRowFields[1] = int(rawRowFields[1])
	rawRowFields[2] = int(rawRowFields[2])
	rawRowFields[3] = int(rawRowFields[3])
	rawRowFields[4] = int(rawRowFields[4])
	rawRowFields[5] = float(rawRowFields[5])
	return ParsedRelevantData(*rawRowFields)


# Get files in directory function
# Params:
#	* dirname	- string - Directory name
# Return:
#	* string list	- Files names in directory
def getFilesInDir(dirname):
	return [f for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]

# Transform row in CSV line function
# Params:
#	* row	- string list - List with fields to transform in CSV line
# Return:
#	* string	- String with fields seprated with csvDelimiter
def toCSVLine(row):
	return csvDelimiter.join(str(field).replace("(", "").replace(")", "").strip() for field in row)


# Write data in a CSV File function
# Params
#	* filename		- string		- CSV file name
#	* file_content	- string list	- List with lines to write in CSV file
#	* write_method	- string		- Wirte method (Write 'w', Append 'a'). Default value 'w'
# Return:
#	* integer	- Number of lines writed in file
def writeCSV(filename, file_content, write_method = "w"):
	lines_count = 0
	f = open(filename, write_method)
	for line in file_content:
		f.write(line + "\n")
		lines_count += 1
	f.close()

	return lines_count


# Create directory function
# Check if directory exists else create it
# Params:
#	* directory	- string	- Directory name
# Return:
def createDirectoryIfNotExists (directory):
	if not os.path.isdir(directory):
		os.makedirs(directory)
		if verbose:
			print getCurrentDateTimeString() + " - " + directory + " created successfully!!!"


# Delete directory files function
# Params:
#	* directory	- string	- Directory name
# Return:
def deleteDirectoryData(directory):
	if os.path.isdir(directory):
		for root, dirs, files in os.walk(directory, topdown = False):
			for f in files:
				os.remove(os.path.join(root, f))
				if verbose:
					print getCurrentDateTimeString() + " - " + f + " deleted successfully!!!"
			for d in dirs:
				shutil.rmtree(os.path.join(root, d))
				if verbose:
					print getCurrentDateTimeString() + " - " + d + " deleted successfully!!!"
	else:
		if verbose:
			print getCurrentDateTimeString() + " - " + directory + " not exists!!!"


def getReadingDate(datetimeString):
	date = ""
	if datetimeString:
		date = datetimeString.split(" ")[0]

	return date

def getReadingTime(datetimeString):
	time = ""
	if datetimeString:
		if " " in datetimeString:
			time = datetimeString.split(" ")[1]

	return time

def getYear(dateString):
	year = 0
	if dateString:
		year = int(dateString.split("-")[0])
	
	return year

def getMonth(dateString):
	month = 0
	if dateString:
		if "-" in dateString:
			month = int(dateString.split("-")[1])

	return month

def getDay(dateString):
	day = 0
	if dateString:
		if dateString.count("-") > 1:
			day = int(dateString.split("-")[2])
	return day

def getHour(timeString):
	hour = 0
	if timeString:
		hour = int(timeString.split(":")[0])
	
	return hour

def getMinute(timeString):
	minute = 0
	if timeString:
		if ":" in timeString:
			minute = int(timeString.split(":")[1])

	return minute

def parseReadingDate(readingDate, readingTime):
	year = getYear(readingDate)
	month = getMonth(readingDate)
	day = getDay(readingDate)

	hour = getHour(readingTime)
	minute = getMinute(readingTime)

	if hour == 23 and minute == 30:
		day += 1

		if month == 2:
			if year % 400 == 0 or (year % 100 != 0 and year % 4 == 0):
				if day == 30:
					day = 1
					month = 3
			else:
				if day == 29:
					day = 1
					month = 3
		elif month in [1, 3, 5, 7, 8, 10]:
			if day == 32:
				day = 1
				month += 1
		elif month in [4, 6, 9, 11]:
			if day == 31:
				day = 1
				month += 1
		elif month == 12:
			if day == 32:
				day = 1
				month = 1
				year += 1

	new_reading_time = str(year) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2)

	return new_reading_time

def parseReadingTime(readingTime):
	hour = getHour(readingTime)
	minute = getMinute(readingTime)

	if minute == 30:
		if hour == 23:
			hour = 0
		else:
			hour += 1

	return hour

def parseReading(reading):
	r = 0
	if reading:
		r = float("0" + reading  if reading[0] == "." else reading)

	return r

def getDayOfWeek(year, month, day):
	return (datetime.datetime(year = year, month = month, day = day).weekday() + 1)



# Function to get index of significant component index on desc ordered components list7
# Params:
#	* descOrdAccumComponentsList	- Int List	- Accumulated Components Occurrences Desc Ordered List
#   * significantSample				- Int	- Num of significant samples to be considered
# Return:
#	* Integer						- Index of last significant component  
def getLastSignificantComponentIdx(descOrdAccumComponentsList, significantSamples):
	idx = 0
	aux = [1 if componentOccurrences < significantSamples else 0 for componentOccurrences in descOrdAccumComponentsList]
	
	if len(descOrdAccumComponentsList) > 0:
		desaccumList = [descOrdAccumComponentsList[0]] + [descOrdAccumComponentsList[i] - descOrdAccumComponentsList[i - 1] for i in xrange(1, len(descOrdAccumComponentsList))]
	else:
		desaccumList = []
	
	if 0 in aux:
		idx = aux.index(0) + 1
	else:
		idx = len(descOrdAccumComponentsList)	

	
	for i in xrange(idx, len(aux)):
		minBound = desaccumList[idx - 1] - (desaccumList[idx - 1] * pctSignificanceAcceptance)
		maxBound = desaccumList[idx - 1] + (desaccumList[idx - 1] * pctSignificanceAcceptance)
		
		if desaccumList[i] >= minBound and desaccumList[i] <= maxBound:
			idx = i + 1
		
	return idx


# Parse Full Occurrences Data With Total
# Params:
#	* fieldList	- List - List of CUSTOMER_ID, OCCURRENCES_LIST and TOTAL_SAMPLES
# Return:
#	* namedtuple	- FullOccurrencesDataTotal named tuple
def parseFullOccurrencesDataTotal(fieldsList):
	res = fieldsList
	res += [None] * (len(fullOccurrencesDataTotalFields) - len(res))
	return FullOccurrencesDataTotal(*res)


# Parse Aux Normalization Data
# Params:
#	* fieldList	- List - List of CUSTOMER_ID, COMPONENT_OCCURRENCIES_TUPLE_LIST, DESC_ORDERED_OCCURRENCES_LIST and TOTAL_SAMPLES
# Return:
#	* namedtuple	- AuxNormalizationData named tuple
def parseAuxNormalizationData(fieldsList):
	res = fieldsList
	res += [None] * (len(auxNormalizationDataFields) - len(res))
	return AuxNormalizationData(*res)

