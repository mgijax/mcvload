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
#      See the configuration file
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
#      2:  Non-fatal error occurred
#
#  Assumes:  Nothing
#
# History:
#
# sc	04/20/2010 - TR6839
#	-new
#

cd `dirname $0`
LOG=`pwd`/mcvload.log
rm -rf ${LOG}

CONFIG_LOAD=../mcvload.config
echo $CONFIG_LOAD

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
# run annotation load
#

echo "Running MCV/Marker annotation load" >> ${LOG_DIAG}
cd ${OUTPUTDIR}
${ANNOTLOAD_CSH} ${CONFIG_ANNOTLOAD} >> ${LOG_DIAG} 
STAT=$?
checkStatus ${STAT} "${ANNOTLOAD_CSH} ${CONFIG_ANNOT}"

#
# run postload cleanup and email logs
#
shutDown

