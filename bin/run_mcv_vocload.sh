#!/bin/sh
#
#  run_mcv_vocload.sh
###########################################################################
#
#  Purpose:
# 	This script runs the Marker Category Vocab (MCV) Load
#	and moves secondary SO IDs to the SO Logical DB as preferred,
#	private
#
  Usage=run_mcv_vocload.sh
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
#      - Log file for the script defined by ${LOG}, note update output goes 
#	 to this log
#      - Log file for this wrapper ${LOG_RUNVOCLOAD}
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
LOG=`pwd`/run_mcv_vocload.log
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

#
# Verify and source the vocload configuration file
#
CONFIG_VOCLOAD=${VOCLOAD}/MCV.config

if [ ! -r ${CONFIG_VOCLOAD} ]
then
   echo "Cannot read configuration file: ${CONFIG_VOCLOAD}"
    exit 1
fi

. ${CONFIG_VOCLOAD}

LOG_RUNVOCLOAD=${RUNTIME_DIR}/runvocload.log
rm -rf LOG_RUNVOCLOAD

#####################################
#
# Main
#
#####################################

#
# run vocabulary load
#
echo "Running MCV Vocabulary load"  | tee -a ${LOG_RUNVOCLOAD}
CONFIG_VOCLOAD=${VOCLOAD}/MCV.config

${VOCLOAD}/runOBOIncLoad.sh ${CONFIG_VOCLOAD} >> ${LOG_RUNVOCLOAD}
STAT=$?
checkStatus ${STAT} "${VOCLOAD}/runOBOFullLoad.sh ${CONFIG_VOCLOAD}"

echo "Moving SO ID association to MCV term to SO ldb" | tee -a ${LOG_RUNVOCLOAD}

cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGI_DBUSER} -P`cat ${MGI_DBPASSWORDFILE}` -e  >> ${LOG_RUNVOCLOAD}

select _Accession_key
into #so
from ACC_Accession
where _MGIType_key = 13
and preferred = 0
and _LogicalDB_key = 146
and prefixPart = "SO:"
go

update ACC_Accession
set a._LogicalDB_key = 145, a.preferred = 1, private = 1
from ACC_Accession a, #so s
where a._Accession_key = s._Accession_key
go

quit
EOSQL

echo 'Done moving SO ID to SO ldb' | tee -a ${LOG_RUNVOCLOAD}

exit 0
