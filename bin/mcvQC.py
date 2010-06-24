#!/usr/local/bin/python
#
#  mcvQC.py
###########################################################################
#
#  Purpose:
#
#	This script will generate a set of QC reports for a marker category
#	vocab annotation file. It also creates an annotation file for the 
#	records that pass the QC checks from the input file.
#
#  Usage:
#
#      mcvQC.py  filename
#
#      where:
#          filename = path to the input file
#
#  Env Vars:
#
#      The following environment variables are set by the configuration
#      files that are sourced by the wrapper script:
#
#          MGI_PUBLICUSER
#          MGI_PUBPASSWORDFILE
#          MCVLOAD_TEMP_TABLE
#          INPUT_FILE_BCP
#          INVALID_MARKER_RPT
#          SEC_MARKER_RPT
#          INVALID_TERMID_RPT
#	   INVALID_JNUM_RPT
#	   INVALID_EVID_RPT
#	   INVALID_EDITOR_RPT
#	   MULTIPLE_MCV_RPT
#	   BEFORE_AFTER_RPT
#          ANNOT_FILE
#
#      The following environment variable is set by the wrapper script:
#
#          LIVE_RUN
#
#  Inputs:
# 	See: mcvQC.sh	
#
#  Outputs:
#
#      - BCP file (${INPUT_FILE_BCP}) for loading the input data
#        into a temp table
#
#      - QC report (${INVALID_MARKER_RPT})
#
#      - QC report (${SEC_MARKER_RPT})
#
#      - QC report (${INVALID_TERMID_RPT})
#
#      - QC report (${INVALID_JNUM_RPT})
#
#      - QC report (${INVALID_EVID_RPT})
#
#      - QC report (${INVALID_EDITOR_RPT})
#
#      - QC report (${MULTIPLE_MCV_RPT})
#
#      - QC report (${BEFORE_AFTER_RPT})
#
#      - Annotation file (${ANNOT_FILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  Discrepancy errors detected in the input files
#
#  Assumes:
#
#      This script assumes that the wrapper script has already created the
#      table in tempdb for loading the input records into. The table
#      name is defined by the environment variable ${MCVLOAD_TEMP_TABLE}.
#      The wrapper script will also take care of dropping the table after
#      this script terminates.
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Load the records from the input file into the temp table.
#      5) Generate the QC reports.
#      7) Create the annotation file from the input records that do not
#         have discrepancies (for a "live" run only).
#
#  Notes:  None
#
###########################################################################

import sys
import os
import string
import re
import mgi_utils
import db

#
#  CONSTANTS
#
TAB = '\t'
NL = '\n'

USAGE = 'Usage: mcvQC.py  inputFile'

#
#  GLOBALS
#
user = os.environ['MGI_PUBLICUSER']
passwordFile = os.environ['MGI_PUBPASSWORDFILE']

liveRun = os.environ['LIVE_RUN']

tempTable = os.environ['MCVLOAD_TEMP_TABLE']

bcpFile = os.environ['INPUT_FILE_BCP']
annotFile = os.environ['ANNOT_FILE']

invMrkRptFile = os.environ['INVALID_MARKER_RPT']
secMrkRptFile = os.environ['SEC_MARKER_RPT']
invTermIdRptFile = os.environ['INVALID_TERMID_RPT']
invJNumRptFile = os.environ['INVALID_JNUM_RPT']
invEvidRptFile = os.environ['INVALID_EVID_RPT']
invEditorRptFile = os.environ['INVALID_EDITOR_RPT']
multiMcvRptFile = os.environ['MULTIPLE_MCV_RPT']
beforeAfterRptFile =  os.environ['BEFORE_AFTER_RPT']
rptNamesFile = os.environ['RPT_NAMES_RPT']

timestamp = mgi_utils.date()

errorCount = 0
multiCt = 0
fatalReportNames = []
nonfatalReportNames = []

# Looks like {mgiID:[ [annotAttributes1], ...], ...}
# value is a list of lists, each list being the set
# of attributes needed to create an annotation load file
annot = {}

# Looks like {mgiID:symbol, ...}
# All official markers in the database mapped to their symbols
mgiIDToSymbolDict = {}

# Looks like {mcvOrSoID:term, ...}
# All mcv or so ids mapped to their terms
termIDToTermDict = {}

# Looks like {mgiID:[termID1, ...], ...}
# markers mapped to their SO/MCV IDs
mgdMgiIdToTermIdDict = {}

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def checkArgs ():
    global inputFile

    if len(sys.argv) != 2:
        print USAGE
        sys.exit(1)

    inputFile = sys.argv[1]

    return


#
# Purpose: Perform initialization steps.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def init ():
    global mgiIDToSymbolDict, termIDToTermDict
    global mgdMgiIdToTermIdDict, inputTermIdLookupByMgiId

    print 'DB Server:' + db.get_sqlServer()
    print 'DB Name:  ' + db.get_sqlDatabase()
    sys.stdout.flush()

    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFile)

    openFiles()
    loadTempTable()

    # Load global lookup dictionaries
    results = db.sql('''select a.accid, m.symbol
	from ACC_Accession a, MRK_Marker m
	where a._MGIType_key = 2
	and a._LogicalDB_key = 1
	and a.prefixPart = "MGI:"
	and a._Object_key = m._Marker_key''', 'auto')

    for r in results:
	mgiIDToSymbolDict[r['accid']] = r['symbol']

    # select both SO and MCV IDs
    results = db.sql('''select a.accID, t.term
	from ACC_Accession a, VOC_Term t
	where a._LogicalDB_key in (145,146)
	and a._MGIType_key = 13
	and a._Object_key = t._Term_key''', 'auto')

    for r in results:
	termIDToTermDict[r['accID']] = r['term']

    results = db.sql('''select a1.accID as termID, a2.accID as mgiID
            from  VOC_Annot v, ACC_Accession a1, ACC_Accession a2
            where v._AnnotType_key =  1011
            and v._Term_key = a1._Object_key
            and a1._MGIType_key = 13
            and a1._LogicalDB_key in (145, 146)
            and a1.preferred = 1
	    and a1.prefixPart = "MCV:"
            and v._Object_key = a2._Object_key
            and a2._MGIType_key = 2
            and a2._LogicalDB_key = 1
            and a2.prefixPart = "MGI:"''', 'auto')
    for r in results:
        mgiID = r['mgiID']
        termID = r['termID']
        if not mgdMgiIdToTermIdDict.has_key(mgiID):
            mgdMgiIdToTermIdDict[mgiID] = []
        mgdMgiIdToTermIdDict[mgiID].append(termID)

    #
    # get all SO/MCV annotations to markers from Input
    #
    results = db.sql('select tmp.termID, tmp.mgiID ' + \
                'from tempdb..' + tempTable + ' tmp ', 'auto')
                #'where tmp.mgiID is not null ' + \
                #'and tmp.termID is not null', 'auto')
    # load lookup
    inputTermIdLookupByMgiId = {}
    for r in results:
        mgiID = r['mgiID']
        termID = r['termID']
	if not inputTermIdLookupByMgiId.has_key(mgiID):
	    inputTermIdLookupByMgiId[mgiID] = [] # default
	if termID != None: # this case when only mgiID in file for delete
	    inputTermIdLookupByMgiId[mgiID].append(termID)
	
    return


#
# Purpose: Open the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    global fpInput, fpBCP
    global fpInvMrkRpt, fpSecMrkRpt, fpInvTermIdRpt
    global fpInvJNumRpt, fpInvEvidRpt, fpInvEditorRpt
    global fpMultiMCVRpt, fpBeforeAfterRpt, fpRptNamesRpt
    #
    # Open the input file.
    #
    try:
        fpInput = open(inputFile, 'r')
    except:
        print 'Cannot open input file: ' + inputFile
        sys.exit(1)

    #
    # Open the output file.
    #
    try:
        fpBCP = open(bcpFile, 'w')
    except:
        print 'Cannot open output file: ' + bcpFile
        sys.exit(1)

    #
    # Open the report files.
    #
    try:
        fpInvMrkRpt = open(invMrkRptFile, 'a')
    except:
        print 'Cannot open report file: ' + invMrkRptFile
        sys.exit(1)
    try:
        fpSecMrkRpt = open(secMrkRptFile, 'a')
    except:
        print 'Cannot open report file: ' + secMrkRptFile
        sys.exit(1)
    try:
        fpInvTermIdRpt = open(invTermIdRptFile, 'a')
    except:
        print 'Cannot open report file: ' + invTermIdRptFile
        sys.exit(1)
    try:
        fpInvJNumRpt = open(invJNumRptFile, 'a')
    except:
        print 'Cannot open report file: ' + invJNumRptFile
        sys.exit(1)
    try:
        fpInvEvidRpt = open(invEvidRptFile, 'a')
    except:
        print 'Cannot open report file: ' + invEvidRptFile
        sys.exit(1)
    try:
        fpInvEditorRpt = open(invEditorRptFile, 'a')
    except:
        print 'Cannot open report file: ' + invEditorRptFile
        sys.exit(1)
    try:
        fpMultiMCVRpt = open(multiMcvRptFile, 'a')
    except:
        print 'Cannot open report file: ' + multiMcvRptFile
        sys.exit(1)
    try:
        fpBeforeAfterRpt = open(beforeAfterRptFile, 'a')
    except:
        print 'Cannot open report file: ' + beforeAfterRptFile
        sys.exit(1)
    try:
        fpRptNamesRpt = open(rptNamesFile, 'a')
    except:
        print 'Cannot open report file: ' + rptNamesFile
        sys.exit(1)

    return


#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFiles ():
    fpInput.close()
    fpInvMrkRpt.close()
    fpSecMrkRpt.close()
    fpInvTermIdRpt.close()
    fpInvJNumRpt.close()
    fpInvEvidRpt.close()
    fpInvEditorRpt.close()
    fpMultiMCVRpt.close()
    fpBeforeAfterRpt.close()
    return


#
# Purpose: Load the data from the input file into the temp table.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def loadTempTable ():
    global annot

    print 'Create a bcp file from the input file'
    sys.stdout.flush()

    #
    # Read each record from the input file, perform validation checks and
    # write them to a bcp file.
    #
    line = fpInput.readline()
    count = 1
    while line:
        tokens = re.split(TAB, line[:-1])
        termID  = tokens[0]
        mgiID = tokens[1]
	jNum = tokens[2]
	evidCode = tokens[3]
	inferFrom = tokens[4]
	qual = tokens[5]
	if qual == None:
	    qual = ''
	editor = tokens[6]
	date = tokens[7]
	if date == None:
	    date = ''
	notes = tokens[8]
	if notes == None:
	    notes = ''
	ldb = '' # not used for mcvload
	
	termIDExists = len(re.findall('[a-zA-Z0-9]',termID))
	mgiIDExists = len(re.findall('[a-zA-Z0-9]',mgiID))
	jNumExists = len(re.findall('[a-zA-Z0-9]',jNum))
	evidCodeExists = len(re.findall('[a-zA-Z0-9]',evidCode))
	inferFromExists = len(re.findall('[a-zA-Z0-9]',inferFrom))
	qualExists = len(re.findall('[a-zA-Z0-9]',qual))
	editorExists = len(re.findall('[a-zA-Z0-9]',editor))
	dateExists = len(re.findall('[a-zA-Z0-9]',date))
	notesExists = len(re.findall('[a-zA-Z0-9]',notes))
	
	#
	# Special Case
	# If there is only an mgiID this is ok, it means curator intends to delete all 
        # annotations
	#
	if termIDExists == 0 and jNumExists == 0 and evidCodeExists == 0 and inferFromExists == 0 \
	    and editorExists == 0 and mgiIDExists > 0:
	    # check the MGI ID for format and continue
	    if re.match('MGI:[0-9]+',mgiID) == None:
                print 'Invalid MGI ID (line ' + str(count) + ')'
                fpBCP.close()
                closeFiles()
                sys.exit(1)
	    # write out to the bcp file:
	    fpBCP.write(termID + TAB + mgiID + TAB + jNum +  TAB + evidCode + \
            TAB + editor + NL)

	    # add to the annotation dictionary so it gets written to the annotation file
	    annotList = [termID, mgiID, jNum, evidCode, inferFrom, qual, editor, date, notes, ldb]
            if not annot.has_key(mgiID):
                annot[mgiID] = []
            annot[mgiID].append(annotList)
		
	    # get next input line and continue
	    line = fpInput.readline()
	    continue

        # There must be a term ID in proper format
        # SO:nnnnnnn or MCV:nnnnnnn
        if termIDExists == 0:
            print 'Missing Term ID (line ' + str(count) + ')'
            fpBCP.close()
            closeFiles()
            sys.exit(1)
	else:
	    if re.match('MCV:[0-9]+',termID) == None and \
		re.match('SO:[0-9]+',termID) == None:
		print 'Invalid Term ID (line ' + str(count) + ')' + ' ' + termID
		fpBCP.close()
		closeFiles()
		sys.exit(1)

        #
        # There must be an MGI ID in proper format (MGI:nnnnnnn).
        #
        if mgiIDExists == 0:
            print 'Missing MGI ID (line ' + str(count) + ')'
            fpBCP.close()
            closeFiles()
            sys.exit(1)
        else:
            if re.match('MGI:[0-9]+',mgiID) == None:
                print 'Invalid MGI ID (line ' + str(count) + ')'
                fpBCP.close()
                closeFiles()
                sys.exit(1)

        #
        # There must be an J Number in proper format (J:nnnnnnn).
        #
        if jNumExists == 0:
            print 'Missing J Number (line ' + str(count) + ')'
            fpBCP.close()
            closeFiles()
            sys.exit(1)
        else:
            if re.match('J:[0-9]+',jNum) == None:
                print 'Invalid J Number (line ' + str(count) + ')'
                fpBCP.close()
                closeFiles()
                sys.exit(1)

        #
        # There must be an evidence code
        #
        if evidCodeExists == 0:
            print 'Missing Evidence Code(line ' + str(count) + ')'
            fpBCP.close()
            closeFiles()
            sys.exit(1)

        #
        # There must be an editor login
        #
        if editorExists == 0:
            print 'Missing Editor login (line ' + str(count) + ')'
            fpBCP.close()
            closeFiles()
            sys.exit(1)

        fpBCP.write(termID + TAB + mgiID + TAB + jNum +  TAB + evidCode + \
	    TAB + editor + NL)

        #
        # Maintain a dictionary of the MGI IDs that are in the input file.
        # The key for each entry is the MGI ID and the value is a list of
        # the annotation attributes for that the MGI ID.
        #
        if mgiID != '':
	    annotList = [termID, mgiID, jNum, evidCode,	inferFrom, qual, editor, date, notes, ldb]
            if not annot.has_key(mgiID):
		annot[mgiID] = []
	    annot[mgiID].append(annotList)

        line = fpInput.readline()
        count += 1

    #
    # Close the bcp file.
    #
    fpBCP.close()

    #
    # Load the input data into the temp table.
    #
    print 'Load the input data into the temp table: ' + tempTable
    sys.stdout.flush()

    bcpCmd = 'cat %s | bcp tempdb..%s in %s -c -t"%s" -S%s -U%s' % (passwordFile, tempTable, bcpFile, TAB, db.get_sqlServer(), db.get_sqlUser())
    rc = os.system(bcpCmd)
    if rc <> 0:
        closeFiles()
        sys.exit(1)

    return


#
# Purpose: Create the invalid marker report.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvMarkerReport ():
    global annot, errorCount, fatalReportNames

    print 'Create the invalid marker report'
    sys.stdout.flush()
    fpInvMrkRpt.write(string.center('Invalid Marker Report',136) + NL)
    fpInvMrkRpt.write(string.center('(' + timestamp + ')',136) + 2*NL)
    fpInvMrkRpt.write('%-20s  %-16s  %-20s  %-20s  %-30s%s' %
                     ('Term ID','MGI ID','Associated Object',
                      'Marker Status','Reason',NL))
    fpInvMrkRpt.write(20*'-' + '  ' + 16*'-' + '  ' + \
                      20*'-' + '  ' + 20*'-' + '  ' + 30*'-' + NL)

    cmds = []

    #
    # Find any MGI IDs from the input data that:
    # 1) Do not exist in the database.
    # 2) Exist for a non-marker object.
    # 3) Exist for a marker, but the status is not "official" or "interim".
    #
    cmds.append('select tmp.termID, ' + \
                       'tmp.mgiID, ' + \
                       'null "name", ' + \
                       'null "status" ' + \
                'from tempdb..' + tempTable + ' tmp ' + \
                'where tmp.mgiID is not null and ' + \
                      'not exists (select 1 ' + \
                                  'from ACC_Accession a ' + \
                                  'where a.accID = tmp.mgiID) ' + \
                'union ' + \
                'select tmp.termID, ' + \
                       'tmp.mgiID, ' + \
                       't.name, ' + \
                       'null "status" ' + \
                'from tempdb..' + tempTable + ' tmp, ' + \
                     'ACC_Accession a1, ' + \
                     'ACC_MGIType t ' + \
                'where tmp.mgiID is not null and ' + \
                      'a1.accID = tmp.mgiID and ' + \
                      'a1._LogicalDB_key = 1 and ' + \
                      'a1._MGIType_key != 2 and ' + \
                      'not exists (select 1 ' + \
                                  'from ACC_Accession a2 ' + \
                                  'where a2.accID = tmp.mgiID and ' + \
                                        'a2._LogicalDB_key = 1 and ' + \
                                        'a2._MGIType_key = 2) and ' + \
                      'a1._MGIType_key = t._MGIType_key ' + \
                'union ' + \
                'select tmp.termID, ' + \
                       'tmp.mgiID, ' + \
                       't.name, ' + \
                       'ms.status ' + \
                'from tempdb..' + tempTable + ' tmp, ' + \
                     'ACC_Accession a, ' + \
                     'ACC_MGIType t, ' + \
                     'MRK_Marker m, ' + \
                     'MRK_Status ms ' + \
                'where tmp.mgiID is not null and ' + \
                      'a.accID = tmp.mgiID and ' + \
                      'a._LogicalDB_key = 1 and ' + \
                      'a._MGIType_key = 2 and ' + \
                      'a._MGIType_key = t._MGIType_key and ' + \
                      'a._Object_key = m._Marker_key and ' + \
                      'm._Marker_Status_key not in (1,3) and ' + \
                      'm._Marker_Status_key = ms._Marker_Status_key ' + \
                'order by tmp.mgiID, tmp.termID')

    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
        termID = r['termID']
        mgiID = r['mgiID']
        objectType = r['name']
        markerStatus = r['status']

        if objectType == None:
            objectType = ''
        if markerStatus == None:
            markerStatus = ''

        if objectType == '':
            reason = 'MGI ID does not exist'
        elif markerStatus == '':
            reason = 'MGI ID exists for non-marker'
        else:
            reason = 'Marker status is invalid'

        fpInvMrkRpt.write('%-20s  %-16s  %-20s  %-20s  %-30s%s' %
            (termID, mgiID, objectType, markerStatus, reason, NL))

        #
        # If the MGI ID and term ID are found in the annotation
        # dictionary, remove the term ID from the list so the
        # annotation doesn't get written to the annotation file.
        #
        #if liveRun == "1":
        #    if annot.has_key(mgiID):
        #        list = annot[mgiID]
        #        if list.count(termID) > 0:
        #            list.remove(termID)
        #        annot[mgiID] = list
    numErrors = len(results[0])
    fpInvMrkRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    errorCount += numErrors
    if numErrors > 0:
        if not invMrkRptFile in fatalReportNames:
            fatalReportNames.append(invMrkRptFile + NL)
    return


#
# Purpose: Create the secondary marker report.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createSecMarkerReport ():
    global annot, errorCount, fatalReportNames

    print 'Create the secondary marker report'
    sys.stdout.flush()
    fpSecMrkRpt.write(string.center('Secondary Marker Report',130) + NL)
    fpSecMrkRpt.write(string.center('(' + timestamp + ')',130) + 2*NL)
    fpSecMrkRpt.write('%-20s  %-16s  %-50s  %-16s%s' %
                     ('Term ID', 'Secondary MGI ID',
                      'Marker Symbol','Primary MGI ID',NL))
    fpSecMrkRpt.write(20*'-' + '  ' + 16*'-' + '  ' + \
                      50*'-' + '  ' + 16*'-' + NL)

    cmds = []

    #
    # Find any MGI IDs from the input data that are secondary IDs
    # for a marker.
    #
    cmds.append('select tmp.termID, ' + \
                       'tmp.mgiID, ' + \
                       'm.symbol, ' + \
                       'a2.accID ' + \
                'from tempdb..' + tempTable + ' tmp, ' + \
                     'ACC_Accession a1, ' + \
                     'ACC_Accession a2, ' + \
                     'MRK_Marker m ' + \
                'where tmp.mgiID is not null and ' + \
                      'tmp.mgiID = a1.accID and ' + \
                      'a1._MGIType_key = 2 and ' + \
                      'a1._LogicalDB_key = 1 and ' + \
                      'a1.preferred = 0 and ' + \
                      'a1._Object_key = a2._Object_key and ' + \
                      'a2._MGIType_key = 2 and ' + \
                      'a2._LogicalDB_key = 1 and ' + \
                      'a2.preferred = 1 and ' + \
                      'a2._Object_key = m._Marker_key ' + \
                'order by tmp.mgiID, tmp.termID')

    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
        termID = r['termID']
        mgiID = r['mgiID']

        fpSecMrkRpt.write('%-20s  %-16s  %-50s  %-16s%s' %
            (termID, mgiID, r['symbol'], r['accID'], NL))

        #
        # If the MGI ID and term ID are found in the annotation
        # dictionary, remove the term ID from the list so the
        # annotation doesn't get written to the annotation file.
        #
        #if liveRun == "1":
        #    if annot.has_key(mgiID):
        #        list = annot[mgiID]
        #        if list.count(termID) > 0:
        #            list.remove(termID)
        #        annot[mgiID] = list
    numErrors = len(results[0])
    fpSecMrkRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    errorCount += numErrors
    if numErrors > 0:
        if not secMrkRptFile in fatalReportNames:
            fatalReportNames.append(secMrkRptFile + NL)
    return

#
# Purpose: Create the invalid MCV/SO term ID report.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvTermIdReport ():
    global errorCount, fatalReportNames

    print 'Create the invalid Term ID report'
    sys.stdout.flush()
    fpInvTermIdRpt.write(string.center('Invalid Term ID Report',80) + NL)
    fpInvTermIdRpt.write(string.center('(' + timestamp + ')',80) + 2*NL)
    fpInvTermIdRpt.write('%-20s%s' % ('Term ID',NL))
    fpInvTermIdRpt.write(20*'-' + NL)
    cmds = []

    #
    # Find any sequence IDs from the input data that are not in the database.
    #
    cmds.append('select tmp.termID ' + \
                'from tempdb..' + tempTable + ' tmp ' + \
                'where tmp.termID is not null and ' + \
                      'not exists (select 1 ' + \
                                  'from ACC_Accession a ' + \
                                  'where a.accID = tmp.termID and ' + \
                                        'a._MGIType_key = 13 and ' + \
					'a._LogicalDB_key in (145,146)) ' + \
                'order by tmp.termID')

    results = db.sql(cmds,'auto')

    #
    # Write a record to the report for each sequence ID that is not in the
    # database..
    #
    for r in results[0]:
        termID = r['termID']
        fpInvTermIdRpt.write('%-20s%s' % (termID, NL))

    numErrors = len(results[0])
    fpInvTermIdRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    errorCount += numErrors
    if numErrors > 0:
        if not invTermIdRptFile in fatalReportNames:
            fatalReportNames.append(invTermIdRptFile + NL)

    return

#
# Purpose: Create the invalid J Number report
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvJNumReport ():
    global errorCount, fatalReportNames

    print 'Create the invalid J Number report'
    sys.stdout.flush()
    fpInvJNumRpt.write(string.center('Invalid J Number Report',80) + NL)
    fpInvJNumRpt.write(string.center('(' + timestamp + ')',80) + 2*NL)
    fpInvJNumRpt.write('%-20s%s' % ('J Number',NL))
    fpInvJNumRpt.write(20*'-' + NL)
    cmds = []

    #
    # Find any J Numbers from the input data that are not in the database.
    #
    cmds.append('select tmp.jNum ' + \
		'from tempdb..' + tempTable + ' tmp ' + \
		'where tmp.jNum is not null and ' + \
		    'not exists (select 1 ' + \
		    'from ACC_Accession a ' + \
			  'where a.accID = tmp.jNum and ' + \
				'a._MGIType_key = 1 and ' + \
				'a._LogicalDB_key = 1 and ' + \
				'a.prefixPart = "J:" and ' + \
				'a.preferred = 1) ' + \
				'order by tmp.jNum')
    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
	jNum = r['jNum']
	fpInvJNumRpt.write('%-20s%s' % (jNum, NL))
   
    numErrors = len(results[0])
    fpInvJNumRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    errorCount += numErrors
    if numErrors > 0:
        if not invJNumRptFile in fatalReportNames:
            fatalReportNames.append(invJNumRptFile + NL)
 
    return

#
# Purpose: Create the invalid Evidence Code report
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvEvidReport ():
    global errorCount, fatalReportNames

    print 'Create the invalid evidence code report'
    sys.stdout.flush()
    fpInvEvidRpt.write(string.center('Invalid Evidence Code Report',80) + NL)
    fpInvEvidRpt.write(string.center('(' + timestamp + ')',80) + 2*NL)
    fpInvEvidRpt.write('%-20s%s' % ('Evidence Code',NL))
    fpInvEvidRpt.write(20*'-' + NL)
    cmds = []

    #
    # Find any Evidence Codes from the input data that are not in the database.
    #
    cmds.append('select tmp.evidCode ' + \
                'from tempdb..' + tempTable + ' tmp ' + \
                'where tmp.evidCode is not null and ' + \
                    'not exists (select 1 ' + \
                    'from VOC_Term t ' + \
                          'where t._Vocab_key = 80 and ' + \
                                'tmp.evidCode = t.term)')
    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
        evidCode = r['evidCode']
        fpInvEvidRpt.write('%-20s%s' % (evidCode, NL))

    numErrors = len(results[0])
    fpInvEvidRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    errorCount += numErrors
    if numErrors > 0:
        if not invEvidRptFile in fatalReportNames:
            fatalReportNames.append(invEvidRptFile + NL)

    return
#
# Purpose: Create the invalid Editor login report
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvEditorReport ():
    global errorCount, fatalReportNames

    print 'Create the invalid editor login report'
    sys.stdout.flush()
    fpInvEditorRpt.write(string.center('Invalid Editor Login Report',80) + NL)
    fpInvEditorRpt.write(string.center('(' + timestamp + ')',80) + 2*NL)
    fpInvEditorRpt.write('%-20s%s' % ('Editor Login',NL))
    fpInvEditorRpt.write(20*'-' + NL)
    cmds = []

    #
    # Find any Editor logins from the input data that are not in the database.
    #
    cmds.append('select tmp.editor ' + \
                'from tempdb..' + tempTable + ' tmp ' + \
                'where tmp.editor is not null and ' + \
                    'not exists (select 1 ' + \
                    'from MGI_User u ' + \
                          'where u.login = tmp.editor)')
    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
        editor = r['editor']
        fpInvEditorRpt.write('%-20s%s' % (editor, NL))

    numErrors = len(results[0])
    fpInvEditorRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    errorCount += numErrors
    if numErrors > 0:
        if not invEditorRptFile in fatalReportNames:
            fatalReportNames.append(invEditorRptFile + NL)

    return

#
# Purpose: Create report for markers annotatd to mor than one
#	MCV term
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#

def createMultipleMCVReport():
    global multiCt, annot, nonfatalReportNames

    print 'Create the multiple MCV annotation report'
    sys.stdout.flush()
    fpMultiMCVRpt.write(string.center(\
	'Multiple MCV Annotation Report',80) + NL)
    fpMultiMCVRpt.write(string.center('(' + timestamp + ')',80) + 2*NL)

    # 
    # Report markers in the input annotated to multiple terms
    # in the input
    #
    fpMultiMCVRpt.write(string.center(\
        'Multiple MCV Annotation In the Input File Report',80) + 2*NL)
    fpMultiMCVRpt.write('%-20s  %-16s  %-20s  %-30s%s' %
                     ('MGI ID','Symbol',
                      'Term ID','Term',NL))
    fpMultiMCVRpt.write(20*'-' + ' ' + 16*'-' + ' ' + 20*'-' + ' ' + 30*'-' + ' ' + NL)

    mgiIDList = annot.keys()
    mgiIDList.sort()
    multiCt = 0
    for mgiID in mgiIDList:
        attrs = annot[mgiID]
	if len(attrs) > 1:
	    multiCt += 1
	    for attrList in attrs:
		mgiID = attrList[1]
		symbol = mgiIDToSymbolDict[mgiID]
		termID = attrList[0]
		term = termIDToTermDict[termID]
		fpMultiMCVRpt.write('%-20s  %-16s  %-20s  %-30s%s' %
		(mgiID, symbol, termID, term, NL))    
    fpMultiMCVRpt.write(NL + 'Number of Markers with Multiple MCV Annotations: ' + str(multiCt) + NL)
    if multiCt > 0:
	if not multiMcvRptFile in nonfatalReportNames:
	    print 'writing multiMcvRptFile to nonfatalReportNames'
	    nonfatalReportNames.append(multiMcvRptFile + NL)

    #
    # report markers in input annotated to different terms in the database
    #
    #fpMultiMCVRpt.write(string.center(NL + \
    #    'Multiple MCV Annotation Btwn Input File and Database Report',80) + \
#	    2*NL)
#    fpMultiMCVRpt.write('%-20s  %-16s  %-20s  %-30s  %-30s%s' %
#                     ('MGI ID','Symbol',
#                      'Input Term ID','Input Term','Database Term(s)', NL))
#    fpMultiMCVRpt.write(20*'-' + ' ' + 16*'-' + ' ' + 20*'-' + ' ' + 30*'-' + \
#	' ' + NL)
#
    #
    # Write the records to the report.
    #
#    multiCt = 0
#    for mgiID in inputTermIdLookupByMgiId.keys():
#	inputTermIDList = inputTermIdLookupByMgiId[mgiID]
#	print 'inputTermIDList: %s' % inputTermIDList
#	mgdTermIdList = []
#	if mgdMgiIdToTermIdDict.has_key(mgiID):
#	    mgdTermIdList = mgdMgiIdToTermIdDict[mgiID]
#	    print 'found input mgiID: %s in db termList: %s' % (mgiID, mgdTermIdList)
#	else:
#	    print 'did not find mgiID in db %s' % (mgiID)
#	    continue
#        for termID in inputTermIDList:
#	    if termID not in mgdTermIdList:
#		multiCt += 1
#	 	if termIDToTermDict.has_key(termID):
#		    term = termIDToTermDict[termID]
#		else: 
#		    print 'termID %s not in db' % termID
#		    continue
#		if mgiIDToSymbolDict.has_key(mgiID):
#		    symbol = mgiIDToSymbolDict[mgiID]
#		else: 
#		    print 'mgiID %s not in db' % mgiID
#		    continue
#
#		mgdTermList = []
#	        for mgdTermId in mgdTermIdList:
#		    mgdTerm = termIDToTermDict[mgdTermId]
#		    if mgdTerm not in mgdTermList:
#			mgdTermList.append(mgdTerm)
#		fpMultiMCVRpt.write('%-20s  %-16s  %-20s  %-30s  %-30s%s' %
#		    (mgiID, symbol, termID, term, ','.join(mgdTermList), NL))
#    fpMultiMCVRpt.write(NL + 'Number of Rows: ' + str(multiCt) + NL)
#
    # This is not an error
    #errorCount += multiCt

    return

#
# Purpose: For markers represented in the annotation file, list 
#	preload MCV annotations and post-load MCV annotations
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createBeforeAfterReport():

    print 'Create the Before/After annotation report'
    fpBeforeAfterRpt.write(string.center('Before/After Report',110) + 2*NL)
    fpBeforeAfterRpt.write('%-12s  %-20s  %-30s  %-30s  %-30s  %-30s%s' %
                     ('MGI ID','Symbol',
                      'Before Term ID(s)','Before Term(s)','After Term ID(s)', 'After Term(s)', NL))
    fpBeforeAfterRpt.write(20*'-' + ' ' + 20*'-' + ' ' + 30*'-' + ' ' + \
	30*'-' +  ' ' + 30*'-' +  ' ' + 30*'-' +  ' ' + NL)

    for mgiID in inputTermIdLookupByMgiId.keys():
	print 'beforeAfter input mgiID: %s' % mgiID
	symbol = mgiIDToSymbolDict[mgiID]
	inputTermIDList = []
	if inputTermIdLookupByMgiId.has_key(mgiID):
	    inputTermIDList = inputTermIdLookupByMgiId[mgiID]
	    print 'inputTermIDList: %s' % inputTermIDList
	inputTermList = []
	for id in inputTermIDList:
	    if id != None:
		term = termIDToTermDict[id]
		inputTermList.append(term)
	mgdTermIDList = []
	if mgdMgiIdToTermIdDict.has_key(mgiID):
	    mgdTermIDList = mgdMgiIdToTermIdDict[mgiID]
	    print 'mgdTermIDList: %s' % mgdTermIDList
	mgdTermList = []
	for id in mgdTermIDList:
	    term = termIDToTermDict[id]
	    mgdTermList.append(term)
	print  '%s %s %s %s %s %s' % (mgiID, symbol, mgdTermIDList, mgdTermList, inputTermIDList, inputTermList)
	fpBeforeAfterRpt.write('%-20s  %-20s  %-30s  %-30s  %-30s  %-30s%s' %
	    (mgiID, symbol, ','.join(mgdTermIDList), ','.join(mgdTermList), \
	    ','.join(inputTermIDList), ','.join(inputTermList), NL))

    return
#
# Purpose: Create the annotation file from the dictionary termID/marker
#          annotations that did not have any discrepancies.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createAnnotFile ():
    print 'Create the annotation file'
    sys.stdout.flush()

    try:
        fpAnnot = open(annotFile, 'w')
    except:
        print 'Cannot open output file: ' + annotFile
        sys.exit(1)

    mgiIDList = annot.keys()
    mgiIDList.sort()

    for mgiID in mgiIDList:
	# get the list of attribute lists for this mgiID
        attrs = annot[mgiID]
	# for each attribute list write out attributes to the
	# annotation file
	for attrList in attrs:
	    line = TAB.join(attrList)
	    line += NL
	    fpAnnot.write(line)
    fpAnnot.close()


#
# Main
#
checkArgs()
init()

createInvMarkerReport()
createSecMarkerReport()
createInvTermIdReport()
createInvJNumReport()
createInvEvidReport()
createInvEditorReport()
createMultipleMCVReport()
if errorCount == 0:
    createBeforeAfterReport()
    nonfatalReportNames.append('\nBefore/After file generated. See: %s\n' % beforeAfterRptFile)
else:
    fatalReportNames.append('\nDid not generate before/after file because of errors\n')
closeFiles()

if liveRun == "1":
    createAnnotFile()

# write  non fatal report names to stdout
names = string.join(nonfatalReportNames,'' )
fpRptNamesRpt.write('\nNon-Fatal QC errors detected in the following files:\n')
fpRptNamesRpt.write(names)

# write fatal report names to stdout
fpRptNamesRpt.write('\nFatalQC errors detected in the following files:\n')
names = string.join(fatalReportNames,'' )
fpRptNamesRpt.write(names)

fpRptNamesRpt.close()

# multiple annotations in the input are Ok
# will not prevent loading
if multiCt > 0:
    sys.exit(2)
# any report errors and load shouldn't run
elif errorCount > 0:
    sys.exit(3)
else:
    sys.exit(0)
