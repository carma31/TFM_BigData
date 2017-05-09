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
# Cluster Dataset Name Absolute Path
absoluteCustomerClusterDataset = absoluteDataDir + "/" + customerClusterDataset

# Clustering Logs Directory
clusteringLogDirName = "ClusteringLog"
# Clustering Logs Directory Absolute Path
absoluteclusteringLogDir = baseDir + "/" + clusteringLogDirName

# Clustering Models Directory
clusteringModelsDirName = "ClusteringModels"
# Clustering Models Directory Absolute Path
absoluteClusteringModelsDirName = baseDir + "/" + clusteringModelsDirName

# Clustering Classification Results Directory
clusteringClassResDir = "ClusteringClassifications"
# Clustering Classification Results Directory Absolute Path
absoluteClusteringClassResDir = baseDir + "/" + clusteringClassResDir

# Customer Clusters Ocurrences File Name
perCustomerClustFileName = 'perCustomer'

# Customer Clusters File Name
clustersPerCustomerClustFileName = 'clustersPerCustomer'

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
