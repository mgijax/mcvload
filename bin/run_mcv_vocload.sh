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
touch ${LOG}

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

LOG_RUNVOCLOAD=${LOGDIR}/runvocload.log
rm -rf ${LOG_RUNVOCLOAD}

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

cat - <<EOSQL | psql -h${MGD_DBSERVER} -d${MGD_DBNAME} -U mgd_dbo -e  >> ${LOG_RUNVOCLOAD}

create temp table somcvTemp as
select a1.accid as mcvID, t._term_key as mcvTermKey, t.term as mcvTerm, t.note as mcvNote, a2._accession_key, a2.accid as soID
    from voc_term t, acc_accession a1, acc_accession a2
    where a1._logicaldb_key = 146
    and a1._mgitype_key = 13
    and a1._object_key = t._term_key
    and a2._logicaldb_key = 145
    and a2._mgitype_key = 13
    and a2._object_key = t._term_key
;

update ACC_Accession a
set _LogicalDB_key = 146, preferred = 0, private = 0
from somcvTemp s
where a._Accession_key = s._accession_key
;

EOSQL

${VOCLOAD}/runOBOIncLoad.sh ${CONFIG_VOCLOAD} >> ${LOG_RUNVOCLOAD}
STAT=$?
checkStatus ${STAT} "${VOCLOAD}/runOBOIncLoad.sh ${CONFIG_VOCLOAD}"

echo "Moving SO ID association to MCV term to SO ldb" | tee -a ${LOG_RUNVOCLOAD}

cat - <<EOSQL | psql -h${MGD_DBSERVER} -d${MGD_DBNAME} -U mgd_dbo -e  >> ${LOG_RUNVOCLOAD}

create temp table soTemp as 
select _Accession_key
from ACC_Accession
where _MGIType_key = 13
and preferred = 0
and _LogicalDB_key = 146
and prefixPart = 'SO:'
;

update ACC_Accession a
set _LogicalDB_key = 145, preferred = 1, private = 1
from soTemp s
where a._Accession_key = s._Accession_key
;

EOSQL

echo 'Done moving SO ID to SO ldb' | tee -a ${LOG_RUNVOCLOAD}

exit 0
