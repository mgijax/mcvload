#!/bin/sh 
#
#  addColumns.sh
###########################################################################
#
#  Purpose: Create a new file with null 9th and/or 10th columns when
#	either are missing from the input file
#
#
#  Usage:
#
#      addColumns.sh  filename 
#
#      where
#          filename = path to the input file
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#
#      - tab delimited file
#
#  Outputs:
#
#      - Log file (${ADD_COLUMNS_LOGFILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#
#  Assumes:  Nothing
#
#  Implementation:
#
#  Notes:  None
#
###########################################################################

CURRENTDIR=`pwd`
BINDIR=`dirname $0`

CONFIG=`cd ${BINDIR}/..; pwd`/mcvload.config

USAGE='Usage: addColumns.sh  filename '

#
# Make sure an input file was passed to the script. 
#
if [ $# -eq 1 ]
then
    INPUT_FILE=$1
else
    echo ${USAGE}; exit 1
fi

#
# Make sure the configuration file exists and source it.
#
if [ -f ${CONFIG} ]
then
    . ${CONFIG}
else
    echo "Missing configuration file: ${CONFIG}"
    exit 1
fi

#
# Make sure the input file exists (regular file or symbolic link).
#
if [ "`ls -L ${INPUT_FILE} 2>/dev/null`" = "" ]
then
    echo "Missing input file: ${INPUT_FILE}"
    exit 1
fi

ADD_COLUMNS_LOGFILE=${CURRENTDIR}/`basename ${ADD_COLUMNS_LOGFILE}`

echo "CURRENTDIR:         ${CURRENTDIR}"
echo "ADD_COLUMNS_LOGFILE:   ${ADD_COLUMNS_LOGFILE}"
#
# Initialize the log file.
#
LOG=${ADD_COLUMNS_LOGFILE}
rm -rf ${LOG}
touch ${LOG}

${MCVLOAD}/bin/addColumns.py ${INPUT_FILE} | tee -a ${ADD_COLUMNS_LOGFILE}
if [ $? -ne 0 ]
then
    echo 'addColumns.sh failed' | tee -a ${ADD_COLUMNS_LOGFILE}
    exit 1
fi

exit 0
