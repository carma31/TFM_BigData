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

# Load common project configuration
source ./tfm_config.sh

#####################################################################
# Aux functions definition
#####################################################################
function createDirIfNotExists() {
	if [ ! -d $1 ]; then

		if [ $verbose -eq 1 ]; then
			verboseOp=" -v "
			printf "$(date) - $1 Not exists\n"
		else
			verboseOp=""
		fi

		mkdir -p $verboseOp $1

		if [ $verbose -eq 1 ]; then
			printf "$(date) - $1 created successfully!!!\n"
		fi
	fi
}
