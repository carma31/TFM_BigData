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
# Config file with shared configuratin to launch python scripts
# ....................................................................


#####################################################################
# Directories configurations
#####################################################################

# Main Project Directory
baseDir = "/home/kike/Escritorio/TFM"

# Data Directory
dataDir = "/Data"
# Data Directory With Absolute Path
absoluteDataDir = baseDir + dataDir

# Data Directory with Splited Raw Dataset
splitDir="/Splited"
# Data Directory With Splited Raw Dataset With Absolute Path
absoluteSplitDir = absoluteDataDir + splitDir

# Dataset In Files Per Customer
customerFilesDir = "/PerCustomer"
# Dataset In Files per Customer With Absolute Path
absoluteCustomerFilesDir = absoluteDataDir + customerFilesDir

# Dataset In Files Per Customer With 24c
customer24CDir = "/Customer24C"
# Dataset In Files per Customer Withn 24C and Absolute Path
absoluteCustomer24CDir = absoluteDataDir + customer24CDir

# Dataset In Files Per Day
day24CDir = "/24CDay"
# Dataset In Files per Customer With Absolute Path
absoluteDay24CDir = absoluteDataDir + day24CDir

# ClusterDatasetName
clusterDataset = "DayReadings.csv"
# Cluster Dataset Name Absolute Path
absoluteClusterDataset = absoluteDataDir + "/" + clusterDataset

# CustomerClusterDatasetName
customerClusterDataset = "CustomerDayReadings.csv"
# Customer Cluster Dataset Name Absolute Path
absoluteCustomerClusterDataset = absoluteDataDir + "/" + customerClusterDataset

# FullClusterDatasetName
fullClusterDataset = "FullDayReadings.csv"
# Full Cluster Dataset Name Absolute Path
absoluteFullClusterDataset = absoluteDataDir + "/" + fullClusterDataset

# Generic Results Directory
resultsDir = "Results"
absoluteResultsDir = baseDir + "/" + resultsDir

# Clustering Logs Directory
clusteringLogDirName = "ClusteringLog"
# Clustering Logs Directory Absolute Path
absoluteclusteringLogDir = absoluteResultsDir + "/" + clusteringLogDirName

# Clustering Models Directory
clusteringModelsDirName = "ClusteringModels"
# Clustering Models Directory Absolute Path
absoluteClusteringModelsDirName = absoluteResultsDir + "/" + clusteringModelsDirName

# Clustering Full Cover Matrix Type Logs Directory
clusteringFullLogDirName = "ClusteringFullLog"
# Clustering Logs Directory Absolute Path
absoluteclusteringFullLogDir = absoluteResultsDir + "/" + clusteringFullLogDirName

# Clustering Models Directory
clusteringFullModelsDirName = "ClusteringFullModels"
# Clustering Models Directory Absolute Path
absoluteClusteringFullModelsDirName = absoluteResultsDir + "/" + clusteringFullModelsDirName

# Clustering Classification Results Directory
clusteringClassResDir = "ClusteringClassifications"
# Clustering Classification Results Directory Absolute Path
absoluteClusteringClassResDir = absoluteResultsDir + "/" + clusteringClassResDir

# Clustering Full Classification Results Directory
clusteringFullClassResDir = "ClusteringFullClassifications"
# Clustering Full Classification Results Directory Absolute Path
absoluteClusteringFullClassResDir = absoluteResultsDir + "/" + clusteringFullClassResDir


# Clustering Statistics Dir
clusteringFullStatisticsDir = 'ClusteringFullStatistics'
# Clustering Statistics Dir Absolute Path
absoluteClusteringFullStatisticsDir = absoluteResultsDir + "/" + clusteringFullStatisticsDir

# Customer Clusters Ocurrences File Name
perCustomerClustFileName = 'perCustomer'

# Customer Clusters File Name
clustersPerCustomerClustFileName = 'clustersPerCustomer'

# Statistic Per Clusters File Name
clustersStatisticsFileName = 'clustersStatistics'


# ReClustering Directory Name
reclusteringDir = 'ReClusteringData'
# ReClustering Directory Name Absolute Path
absoluteReclusteringDir = absoluteDataDir + "/" + reclusteringDir


# ReClustering Directory Name
reclusteringFullDataDir = 'FullReClusteringData'
# ReClustering Directory Name Absolute Path
absoluteReclusteringFullDataDir = absoluteDataDir + "/" + reclusteringFullDataDir


# ReClustering Logs Directory
reclusteringLogDirName = "ReClustering/ReClusteringLog"
# Clustering Logs Directory Absolute Path
absoluteReclusteringLogDirName = absoluteResultsDir + "/" + reclusteringLogDirName

# Clustering Models Directory
reclusteringModelsDirName = "ReClustering/ReClusteringModels"
# Clustering Models Directory Absolute Path
absoluteReclusteringModelsDirName = absoluteResultsDir + "/" + reclusteringModelsDirName


# ReClustering Posteriors
reClusteringPosteriorsDir = "ReClustering/Posteriors"
# ReClustering Posteriors Absolute Path
absoluteReClusteringPosteriorsDir = absoluteResultsDir + "/" + reClusteringPosteriorsDir

# ReClustering Entropy File Name
reClusteringEntropyFileName = "ReClustering/Entropy.csv"
# ReClustering Entropy File Name Absolute Path
absoluteReClusteringEntropyFileName = absoluteResultsDir + "/" + reClusteringEntropyFileName

#####################################################################
# Aux variables configuration
#####################################################################

# Determine if print log messages
verbose = True

# CSV file delimiters
csvDelimiter = ","

# Delete data from previous execution
deleteDataPrevExec = True

#Spark App Name
TFM_appName = "TFM"

# Normalization Decimal Precision
normalizationPrecision = 4

# Significance Aceptance Percentage
pctSignificanceAcceptance = 0.05 

#####################################################################
# Clustering configuration
#####################################################################

# Matrix Covariance type ['diagonal', 'full', 'tied', 'tied_diagonal', 'spherical']
clust_covar_type = 'diagonal'

# Maximum number of components of cluster
clust_max_components = 300

# Slices
clust_slices = 8

# Batch size
clust_batch_size = 100

#####################################################################
# Clustering Full configuration
#####################################################################

# Matrix Covariance type ['diagonal', 'full', 'tied', 'tied_diagonal', 'spherical']
clustFullCovarType = 'full'

# Maximum number of components of cluster
clustFullMaxComponents = 100

# Slices
clustFullSlices = 8

# Batch size
clustFullBatchSize = 100


#####################################################################
# ReClustering configuration
#####################################################################


reclust_covar_types = [('diagonal', 'd'), ('full', 'f')]

# Maximum number of components of cluster
reclust_max_components = 150

# Slices
reclust_slices = 8

# Batch size
reclust_batch_size = 100

