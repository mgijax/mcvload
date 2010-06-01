#!/bin/sh -x
#
#  run_mcv_vocabload.sh
###########################################################################
#
#  Purpose:
# 	This script optionally runs the Marker Category Vocab (MCV) Load
#
  Usage=run_mcv_vocabload.sh
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
# sc	04/30/2010 - TR6839
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
# run vocabulary load
#
echo "Running MCV Vocabulary load"  >> ${LOG_DIAG}
CONFIG_LOAD=${VOCLOAD}/MCV.config

${VOCLOAD}/runOBOIncLoad.sh ${CONFIG_LOAD} >> ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${VOCLOAD}/runOBOFullLoad.sh ${CONFIG_LOAD}"

echo "Moving SO ID association to MCV term to SO ldb"

cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGI_PUBLICUSER} -P`ca ${MGI_PUBPASSWORDFILE}` -e  >> ${LOG}

select _Accession_key
into #so
from ACC_Accession
where _MGIType_key = 13
and preferred = 0
and _LogicalDB_key = 146
and prefixPart = "SO:"
go

update ACC_Accession
set a._LogicalDB_key = 145
from ACC_Accession a, #so s
where a._Accession_key = s._Accession_key
go

quit
EOSQL

exit 0
