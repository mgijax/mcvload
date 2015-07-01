#!/bin/sh
#
#  mcvload.sh
###########################################################################
#
#  Purpose:
# 	This script runs the Marker Category Vocab Annotation Load
#
  Usage=mcvload.sh
#
#  Env Vars:
#
#      See the configuration file mcvload.config
#
#  Inputs:
#
#      - Common configuration file -
#               /usr/local/mgi/live/mgiconfig/master.config.sh
#      - configuration file - mcvload.config
#      - input file - see mcvload.config and vocload/MCV.config
#
#  Outputs:
#
#      - An archive file
#      - Log files defined by the environment variables ${LOG_PROC},
#        ${LOG_DIAG}, ${LOG_CUR} and ${LOG_VAL}
#      - annotload logs and bcp file to ${OUTPUTDIR}
#      - vocload logs and bcp files  - see vocload/MCV.config
#      - Records written to the database tables
#      - Exceptions written to standard error
#      - Configuration and initialization errors are written to a log file
#        for the shell script
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#
#  Assumes:  Nothing
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Source the configuration file to establish the environment.
#      3) Verify that the input files exist.
#      4) Initialize the log file.
#      5) Determine if the input file has changed since the last time that
#         the load was run. Do not continue if the input file is not new.
#      6) Call mcvQC.sh to generate the sanity/QC reports and 
#         annotation file.
#      7) Load annotations
#      8) Archive the input file.
#      9) Touch the "lastrun" file to timestamp the last run of the load.

# History:
#
# sc	04/20/2010 - TR6839
#	-new
#

cd `dirname $0`
LOG=`pwd`/mcvload.log
rm -rf ${LOG}

RUNTYPE=live

CONFIG_LOAD=../mcvload.config

#
# Verify and source the configuration file
#
if [ ! -r ${CONFIG_LOAD} ]
then
   echo "Cannot read configuration file: ${CONFIG_LOAD}"
    exit 1   
fi

. ${CONFIG_LOAD}

#
# Verify annotation configuration file
#
if [ ! -r ${CONFIG_ANNOTLOAD} ]
then
   echo "Cannot read configuration file: ${CONFIG_ANNOTLOAD}"
    exit 1
fi

#
# Make sure the input file exists (regular file or symbolic link).
#
if [ "`ls -L ${INPUT_FILE_DEFAULT} 2>/dev/null`" = "" ]
then
    echo "Missing input file: ${INPUT_FILE_DEFAULT}"
    exit 1
fi

#
# Create a temporary file and make sure that it is removed when this script
# terminates.
#
TMP_FILE=/tmp/`basename $0`.$$
touch ${TMP_FILE}
trap "rm -f ${TMP_FILE}" 0 1 2 15

#
#  Source the DLA library functions.
#

if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#####################################
#
# Main
#
#####################################

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
preload ${OUTPUTDIR}

#
# There should be a "lastrun" file in the input directory that was created
# the last time the load was run for this input file. If this file exists
# and is more recent than the input file, the load does not need to be run.
#
LASTRUN_FILE=${INPUTDIR}/lastrun

if [ -f ${LASTRUN_FILE} ]
then
    if env test ${LASTRUN_FILE} -nt ${INPUT_FILE_DEFAULT}
    then
        echo "Input file has not been updated - skipping load" | tee -a ${LOG_PROC}
        STAT=0
	checkStatus ${STAT} 'Checking input file'
	shutDown
	exit 0
    fi
fi

#
# Generate the sanity/QC reports
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Generate the sanity/QC reports" | tee -a ${LOG_DIAG}
${MCVLOAD_QC_SH} ${INPUT_FILE_DEFAULT} ${RUNTYPE} 2>&1 >> ${LOG_DIAG} 
STAT=$?
checkStatus ${STAT} "QC reports"
if [ ${STAT} -eq 1 ]
then
    shutDown
    exit 1
fi

#
# run annotation load
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Running MCV/Marker annotation load" >> ${LOG_DIAG}
cd ${OUTPUTDIR}
${ANNOTLOAD_CSH} ${CONFIG_ANNOTLOAD} mcv >> ${LOG_DIAG} 
STAT=$?
checkStatus ${STAT} "${ANNOTLOAD_CSH} ${CONFIG_ANNOT}"

#
# Archive a copy of the input file, adding a timestamp suffix.
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Archive input file" | tee -a ${LOG_DIAG}
TIMESTAMP=`date '+%Y%m%d.%H%M'`
ARC_FILE=`basename ${INPUT_FILE_DEFAULT}`.${TIMESTAMP}
cp -p ${INPUT_FILE_DEFAULT} ${ARCHIVEDIR}/${ARC_FILE}

#
# Touch the "lastrun" file to note when the load was run.
#
touch ${LASTRUN_FILE}

#
# run postload cleanup and email logs
#
shutDown

