#!/bin/sh 
#
#  mcvQC.sh
###########################################################################
#
#  Purpose:
#
#      This script is a wrapper around the process that creates sanity/QC
#      reports for a given mcv annotation file.
#
#  Usage:
#
#      mcvQC.sh  filename  [ "live" ]
#
#      where
#          filename = path to the input file
#          live = option to let the script know that this is a "live" run
#                 so the output files are created under the /data/loads
#                 directory instead of the current directory
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#
#      - annotation input file with the following tab-delimited fields:
#
#	1. SO ID (MCV ID may be used instead)
# 	2. Marker MGI ID
# 	3. J: (J:#####)
# 	4. Evidence Code Abbreviation (max length 5)
# 	5. Inferred From 
# 	6. Qualifier - may be blank
# 	7. Editor user login
# 	8. Date (MM/DD/YYYY) - may be blank, if so date of load is used.
# 	9. Notes - may be blank (max length 255)
# 	10. Blank (this is used for ldb if different than default 'MGI')
#
#       There can be additional fields, but they are not used.
#
#  Outputs:
#
#      - Sanity report for the input file.
#
#      - Log file (${MCVLOADQC_LOGFILE})
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
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Source the common configuration file to establish the environment.
#      3) Verify that the input files exist.
#      4) If this is not a "live" run, override the path to the output, log
#         and report files, so they reside in the current directory.
#      5) Initialize the log and report files.
#      6) Clean up the input files by removing blank lines, Ctrl-M, etc.
#      7) Generate the sanity report.
#      8) Create a temp table for the input data.
#      9) Call mcvQC.py to generate the QC reports and the
#          annotation file.
#      10) Drop the temp table.
#
#  Notes:  None
#
###########################################################################

CURRENTDIR=`pwd`
BINDIR=`dirname $0`

CONFIG=`cd ${BINDIR}/..; pwd`/mcvload.config

USAGE='Usage: mcvQC.sh  filename  [ "live" ]'

LIVE_RUN=0; export LIVE_RUN

#
# Make sure an input file was passed to the script. If the optional "live"
# argument is given, that means that the output files are located in the
# /data/loads/... directory, not in the current directory.
#
if [ $# -eq 1 ]
then
    INPUT_FILE=$1
elif [ $# -eq 2 -a "$2" = "live" ]
then
    INPUT_FILE=$1
    LIVE_RUN=1
else
    echo ${USAGE}; exit 1
fi

#
# Create a temporary file and make sure that it is removed when this script
# terminates.
#
TMP_FILE=/tmp/`basename $0`.$$
touch ${TMP_FILE}
trap "rm -f ${TMP_FILE}" 0 1 2 15

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

#
# If this is not a "live" run, the output, log and report files should reside
# in the current directory, so override the default settings.
#
if [ ${LIVE_RUN} -eq 0 ]
then
    INPUT_FILE_QC=${CURRENTDIR}/`basename ${INPUT_FILE_QC}`
    INPUT_FILE_BCP=${CURRENTDIR}/`basename ${INPUT_FILE_BCP}`
    ANNOT_FILE=${CURRENTDIR}/`basename ${ANNOT_FILE}`
    MCVLOADQC_LOGFILE=${CURRENTDIR}/`basename ${MCVLOADQC_LOGFILE}`
    SANITY_RPT=${CURRENTDIR}/`basename ${SANITY_RPT}`
    INVALID_MARKER_RPT=${CURRENTDIR}/`basename ${INVALID_MARKER_RPT}`
    SEC_MARKER_RPT=${CURRENTDIR}/`basename ${SEC_MARKER_RPT}`
    INVALID_TERMID_RPT=${CURRENTDIR}/`basename ${INVALID_TERMID_RPT}`
    INVALID_JNUM_RPT=${CURRENTDIR}/`basename ${INVALID_JNUM_RPT}`
    INVALID_EVID_RPT=${CURRENTDIR}/`basename ${INVALID_EVID_RPT}`
    INVALID_EDITOR_RPT=${CURRENTDIR}/`basename ${INVALID_EDITOR_RPT}`
    MULTIPLE_MCV_RPT=${CURRENTDIR}/`basename ${MULTIPLE_MCV_RPT}`
    BEFORE_AFTER_RPT=${CURRENTDIR}/`basename ${BEFORE_AFTER_RPT}`
    RPT_NAMES_RPT=${CURRENTDIR}/`basename ${RPT_NAMES_RPT}`
fi

#echo "CURRENTDIR:         ${CURRENTDIR}"
#echo "INPUT_FILE_QC:      ${INPUT_FILE_QC}"
#echo "INPUT_FILE_BCP:     ${INPUT_FILE_BCP}"
#echo "ANNOT_FILE:         ${ANNOT_FILE}"
#echo "MCVLOADQC_LOGFILE:   ${MCVLOADQC_LOGFILE}"
#echo "SANITY_RPT:         ${SANITY_RPT}"
#echo "INVALID_MARKER_RPT: ${INVALID_MARKER_RPT}"
#echo "SEC_MARKER_RPT:     ${SEC_MARKER_RPT}"
#echo "INVALID_TERMID_RPT:    ${INVALID_TERMID_RPT}"
#echo "INVALID_JNUM_RPT:	${INVALID_JNUM_RPT}"
#echo "INVALID_EVID_RPT:	${INVALID_EVID_RPT}"
#echo "INVALID_EDITOR_RPT:	${INVALID_EDITOR_RPT}"
#echo "MULTIPLE_MCV_RPT:	${MULTIPLE_MCV_RPT}"
#echo "BEFORE_AFTER_RPT: ${BEFORE_AFTER_RPT}"

#
# Initialize the log file.
#
LOG=${MCVLOADQC_LOGFILE}
rm -rf ${LOG}
touch ${LOG}

#
# Initialize the report files to make sure the current user can write to them.
#
RPT_LIST="${SANITY_RPT} ${INVALID_MARKER_RPT} ${SEC_MARKER_RPT} ${INVALID_TERMID_RPT} ${INVALID_JNUM_RPT} ${INVALID_EVID_RPT} ${INVALID_EDITOR_RPT}  ${MULTIPLE_MCV_RPT} ${BEFORE_AFTER_RPT} ${RPT_NAMES_RPT}"

for i in ${RPT_LIST}
do
    rm -f $i; >$i
done

#
# Convert the input file into a QC-ready version that can be used to run
# the sanity/QC reports against. This involves doing the following:
# 1) Extract columns 1 thru 10
# 2) Remove any spaces
# 3) Extract only lines that have alphanumerics (excludes blank lines)
# 4) Remove any Ctrl-M characters (dos2unix)
#
cat ${INPUT_FILE} | tail +2 | cut -d'	' -f1-10 | sed 's/ //g' | grep '[0-9A-Za-z]' > ${INPUT_FILE_QC}
dos2unix ${INPUT_FILE_QC} ${INPUT_FILE_QC} 2>/dev/null
#
# FUNCTION: Check for duplicate lines in an input file and write the lines
#           to the sanity report.
#
checkDupLines ()
{
    FILE=$1    # The input file to check
    REPORT=$2  # The sanity report to write to

    echo "Duplicate Lines" >> ${REPORT}
    echo "---------------" >> ${REPORT}
    sort ${FILE} | uniq -d > ${TMP_FILE}
    cat ${TMP_FILE} >> ${REPORT}
    if [ `cat ${TMP_FILE} | wc -l` -eq 0 ]
    then
        return 0
    else
        return 1
    fi
}

#
# FUNCTION: Check for lines with missing columns in an input file and write
#           the line numbers to the sanity report.
#
checkColumns ()
{
    FILE=$1         # The input file to check
    REPORT=$2       # The sanity report to write to
    NUM_COLUMNS=$3  # The number of columns expected in each input record
    ${MCVLOAD}/bin/checkColumns.py ${FILE} ${NUM_COLUMNS} >> ${REPORT}
}


#
# Run sanity checks on the input file.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Run sanity checks on the input file" >> ${LOG}
FILE_ERROR=0

checkDupLines ${INPUT_FILE_QC} ${SANITY_RPT}
if [ $? -ne 0 ]
then
    FILE_ERROR=1
fi

checkColumns ${INPUT_FILE_QC} ${SANITY_RPT} ${MCVLOAD_FILE_COLUMNS}
if [ $? -ne 0 ]
then
    FILE_ERROR=1
fi

#
# If the input file had sanity error, remove the QC-ready input file and
# skip the QC reports.
#
if [ ${FILE_ERROR} -ne 0 ]
then
    echo "Sanity errors detected in input file" | tee -a ${LOG}
    rm -f ${INPUT_FILE_QC}
    exit 1
fi

#
# Append the current user ID to the name of the temp table that needs to be
# created. This allows multiple people to run the QC checks at the same time
# without sharing the same table.
#
MCVLOAD_TEMP_TABLE=${MCVLOAD_TEMP_TABLE}_${USER}

#
# Create a temp table for the input data.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Create a temp table for the input data" >> ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGI_PUBLICUSER} -P`cat ${MGI_PUBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

create table ${MCVLOAD_TEMP_TABLE} (
    termID varchar(30) null,
    mgiID varchar(30) not null,
    jNum varchar(30) null,
    evidCode varchar(30) null,
    editor varchar(30) null
)
go

create nonclustered index idx_termID on ${MCVLOAD_TEMP_TABLE} (termID)
go

create nonclustered index idx_mgiID on ${MCVLOAD_TEMP_TABLE} (mgiID)
go

create nonclustered index idx_jNum on ${MCVLOAD_TEMP_TABLE} (jNum)
go

create nonclustered index idx_evidCode on ${MCVLOAD_TEMP_TABLE} (evidCode)
go

create nonclustered index idx_editor on ${MCVLOAD_TEMP_TABLE} (editor)
go

grant all on ${MCVLOAD_TEMP_TABLE} to public
go

quit
EOSQL

#
# Generate the QC reports.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "\nGenerate the QC reports\n" | tee -a ${LOG}
{ ${MCVLOAD_QC} ${INPUT_FILE_QC} 2>&1; echo $? > ${TMP_FILE}; } >> ${LOG}
if [ `cat ${TMP_FILE}` -eq 1 ]
then
    echo "A fatal error occurred while generating the QC reports"
    echo "See log file (${LOG})"
    RC=1
elif [ `cat ${TMP_FILE}` -eq 2 ]
then
    cat ${RPT_NAMES_RPT} | tee -a ${LOG}
    RC=0
elif [ `cat ${TMP_FILE}` -eq 3 ]
then
    cat ${RPT_NAMES_RPT} | tee -a ${LOG}
    RC=1
else
    echo "QC reports successful, no errors" | tee -a ${LOG}
    RC=0
fi

#
# Drop the temp table.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Drop the temp table" >> ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGI_PUBLICUSER} -P`cat ${MGI_PUBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

drop table ${MCVLOAD_TEMP_TABLE}
go

quit
EOSQL

date >> ${LOG}

#
# Remove the QC-ready input file and the bcp file.
#
rm -f ${INPUT_FILE_QC}
rm -f ${INPUT_FILE_BCP}

exit ${RC}
