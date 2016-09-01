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
#          MCVLOAD_TEMP_TABLE
#          INPUT_FILE_BCP
#          INVALID_MARKER_RPT
#          SEC_MARKER_RPT
#          INVALID_TERMID_RPT
#	   INVALID_JNUM_RPT
#	   INVALID_EVID_RPT
#	   INVALID_EDITOR_RPT
#	   MULTIPLE_MCV_RPT
#	   MKR_TYPE_CONFLICT_RPT
#	   GRPNG_TERM_RPT
#	   BEFORE_AFTER_RPT
#	   RPT_NAMES_RPT
#          ANNOT_FILE
#	   GROUPING_TERMIDS
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
#      - QC report (${MKR_TYPE_CONFLICT_RPT})
#
#      - QC report (${BEFORE_AFTER_RPT})
#
#      - Annotation file (${ANNOT_FILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  Non-fatal discrepancy errors detected in the input files
#      3:  Fatal discrepancy errors detected in the input files
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
#      7) Create the annotation file if no fatal discrepancies
#         (for a "live" run only).
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

# for updating marker type
UPDATE = '''update MRK_Marker
	    set _Marker_Type_key = %s,
	    _ModifiedBy_key = %s,
	    modification_date = now()
	    where _Marker_key = %s'''
MARKER_KEY = '''select _Object_key as _Marker_key
		from ACC_Accession
		where _MGIType_key = 2
		and _LogicalDB_key = 1
		and prefixPart = 'MGI:'
		and preferred = 1
		and accID = '%s' '''
#
#  GLOBALS
#

# for updating marker types
updatedBy = None
updatedByKey = None

liveRun = os.environ['LIVE_RUN']

tempTable = os.environ['MCVLOAD_TEMP_TABLE']

# temp table bcp file name
bcpFile = os.environ['INPUT_FILE_BCP']

# annotation file name used in 'live' mode only
annotFile = os.environ['ANNOT_FILE']

# grouping terms
groupingTermIds = os.environ['GROUPING_TERMIDS']

# Report file names
invMrkRptFile = os.environ['INVALID_MARKER_RPT']
secMrkRptFile = os.environ['SEC_MARKER_RPT']
invTermIdRptFile = os.environ['INVALID_TERMID_RPT']
invJNumRptFile = os.environ['INVALID_JNUM_RPT']
invEvidRptFile = os.environ['INVALID_EVID_RPT']
invEditorRptFile = os.environ['INVALID_EDITOR_RPT']
multiMcvRptFile = os.environ['MULTIPLE_MCV_RPT']
conflictRptFile = os.environ['MKR_TYPE_CONFLICT_RPT']
groupingTermRptFile = os.environ['GRPNG_TERM_RPT']
beforeAfterRptFile =  os.environ['BEFORE_AFTER_RPT']
rptNamesFile = os.environ['RPT_NAMES_RPT']

BCP_COMMAND = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh'

timestamp = mgi_utils.date()

# current number of fatal errors
fatalCount = 0

# list of reports which contain fatal errors
fatalReportNames = []

# current number of non fatal multiple MCV/gene errors
#multiCt = 0

# current number on non fatal mkr type conflicts
#conflictCt = 0

# current number of nonfatal errors
nonfatalCount = 0

# list of reports which contain non-fatal errors
nonfatalReportNames = []

# Looks like {mgiID:[ [annotAttributes1], ...], ...}
# value is a list of lists, each list being the set
# of attributes needed to create an annotation load file
annot = {}

# Looks like {mgiID:symbol, ...}
# All official markers in the database mapped to their symbols
mgiIDToSymbolDict = {}

# Looks like {mcvOrSoID:term, ...}
# All mcv and so ids mapped to their terms
termIDToTermDict = {}

# Looks like {mgiID:[termID1, ...], ...}
# markers mapped to their SO/MCV IDs
mgdMgiIdToTermIdDict = {}

# map marker type key to marker type and the reverse
mkrTypeKeyToMkrTypeDict = {}
mkrTypeToKeyDict = {}

#
# map marker type to the MCV Term associated with the marker type
#  looks like {mType:mcvTerm, ...}
mkrTypeToAssocMCVTermDict = {}

# The inverse of above looks like {mcvTerm:mType}
# marker type mcv term and actual marker type should always be the same
# but just in case we create this lookup
mcvTermToMkrTypeDict = {}

# map MCV Term to its parent term representing a marker type (could be itself)
# looks like {mcvTerm:mkrTypeTerm, ...}
mcvTermToParentMkrTypeTermDict = {}

inputTermIdLookupByMgiId = {}

mkrKeyToMkrTypeKeyDict = {}
#
# map marker mgiID to its marker type
#
mgiIdToMkrTypeDict = {}

# markers whose type need updating based on the MCV marker type
# {mgiID: mcv marker type term
markersToUpdateDict = {}

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variable
# Throws: Nothing
#
def checkArgs ():
    global inputFile

    if len(sys.argv) != 2:
        print USAGE
        sys.exit(1)

    inputFile = sys.argv[1]


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

    global mcvTermToParentMkrTypeTermDict
    global mcvKeyToTermDict, mkrTypeToAssocMCVTermDict
    global mcvTermToMkrTypeDict, mkrKeyToMkrTypeKeyDict, mkrTypeToKeyDict

    global updatedBy, updatedByKey

    print 'DB Server:' + db.get_sqlServer()
    print 'DB Name:  ' + db.get_sqlDatabase()
    sys.stdout.flush()

    db.useOneConnection(1)
    #db.set_sqlLogFunction(db.sqlLogAll)
    openFiles()
    loadTempTable()

    # get user key for updates
    results = db.sql('''select _User_key
	from MGI_User
	where login = '%s' ''' % updatedBy)

    updatedByKey = results[0]['_User_key']

    #
    # Load global lookup dictionaries
    #

    # create lookup of all official markers in the database 
    # mapped to their symbols
    results = db.sql('''select a.accid, m.symbol
	from ACC_Accession a, MRK_Marker m
	where a._MGIType_key = 2
	and a._LogicalDB_key = 1
	and a.prefixPart = 'MGI:'
	and a._Object_key = m._Marker_key''', 'auto')

    for r in results:
	mgiIDToSymbolDict[r['accid']] = r['symbol']

    # create lookup of all mcv and so ids mapped to their terms
    results = db.sql('''select a.accID, t.term
	from ACC_Accession a, VOC_Term t
	where a._LogicalDB_key in (145,146)
	and a._MGIType_key = 13
	and a._Object_key = t._Term_key''', 'auto')

    for r in results:
	termIDToTermDict[r['accID']] = r['term']

    # create lookup of markers mapped to their SO/MCV IDs
    results = db.sql('''select a1.accID as termID, a2.accID as mgiID
            from  VOC_Annot v, ACC_Accession a1, ACC_Accession a2
            where v._AnnotType_key =  1011
            and v._Term_key = a1._Object_key
            and a1._MGIType_key = 13
            and a1._LogicalDB_key in (145, 146)
            and a1.preferred = 1
	    and a1.prefixPart = 'MCV:'
            and v._Object_key = a2._Object_key
            and a2._MGIType_key = 2
            and a2._LogicalDB_key = 1
            and a2.prefixPart = 'MGI:' ''', 'auto')
    for r in results:
        mgiID = r['mgiID']
        termID = r['termID']
        if not mgdMgiIdToTermIdDict.has_key(mgiID):
            mgdMgiIdToTermIdDict[mgiID] = []
        mgdMgiIdToTermIdDict[mgiID].append(termID)

    #
    # get all SO/MCV annotations to markers from temp table
    # loaded from the input file
    #
    results = db.sql('select tmp.termID, tmp.mgiID ' + \
                'from ' + tempTable + ' tmp ', 'auto')
                #'where tmp.mgiID is not null ' + \
                #'and tmp.termID is not null', 'auto')

    # load lookup
    for r in results:
        mgiID = r['mgiID']
        termID = r['termID']
	if not inputTermIdLookupByMgiId.has_key(mgiID):
	   inputTermIdLookupByMgiId[mgiID] = [] # default
	if termID != None: # this case when only mgiID in file for delete
	    inputTermIdLookupByMgiId[mgiID].append(termID)
    #
    # get marker types from the database
    #
    results = db.sql(''' select a.accId as mgiID, t.name
                from MRK_Marker m, ACC_Accession a, MRK_Types t
                where m._Marker_Status_key = 1
                and m._Organism_key = 1
		and m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and a._LogicalDB_key = 1
		and a.preferred = 1
		and a.prefixPart = 'MGI:'
		and m._Marker_Type_key = t._Marker_Type_key''', 'auto')
    for r in results:
	mgiIdToMkrTypeDict[r['mgiID']] = r['name']

    results = db.sql(''' select name, _Marker_Type_key
		from MRK_Types''', 'auto')
    for r in results:
	mkrTypeKeyToMkrTypeDict[ r['_Marker_Type_key'] ] =  r['name']
	mkrTypeToKeyDict[r['name']] = r['_Marker_Type_key']

    # parse the MCV Note and load lookups
    # we store the association of a marker type to a MCV
    # term in the term Note. Only MCV terms which correspond to
    # marker types have these notes
    # note looks like:
    # Marker_Type=N
    #
    # _Vocab_key = 79 = Marker Category Vocab
    # _NoteType_key = 1001 = Private Vocab Term Comment'

    # Get the MCV vocab terms and their notes from the database
    # Notes tell us the term's MGI marker type if term maps directly to a
    # marker type
    cmds = []
    cmds.append('select n._Object_key, rtrim(nc.note) as chunk, ' + \
	'nc.sequenceNum ' + \
        'into temp notes ' + \
        'from MGI_Note n, MGI_NoteChunk nc ' + \
        'where n._MGIType_key = 13 ' + \
            'and n._NoteType_key = 1001 ' + \
            'and n._Note_key = nc._Note_key')
    cmds.append('create index notes_idx1 on notes(_Object_key)')
    cmds.append('select t._Term_key, t.term, n.chunk ' + \
            'from VOC_Term t left outer join notes n on ' + \
		'n._object_key = t._term_key ' + \
            'where t._Vocab_key = 79 ' + \
            'and t._Term_key = n._Object_key ' + \
            'order by t._Term_key, n.sequenceNum')
    results = db.sql(cmds, 'auto')
    notes = {} # map the terms to their note chunks
    for r in results[2]:
        term = r['term']
        chunk = r['chunk']
        # if there is a note chunk add it to the notes dictionary
        # we'll pull all the chunks together later
        if chunk != None:
            if not notes.has_key(term):
                notes[term] = []
            notes[term].append(chunk)
    # parse the marker type from the note, if there is one
    for term in notes.keys():
        note = string.join(notes[term], '')
        if not note[0:11] == 'Marker_Type':
            continue

        # parse the note
	tokens = string.split(note, ';')
	mType = tokens[0]
	tokens = string.split(mType, '=')
	
        # 2nd token is the marker type key
        mkrTypeKey = int(string.strip(tokens[1]))
	mkrType = mkrTypeKeyToMkrTypeDict[mkrTypeKey]
        # There is only 1  MCV term per MGI Mkr type
        mkrTypeToAssocMCVTermDict[mkrType]= term
        mcvTermToMkrTypeDict[term] = mkrType

    #
    # now map all mcv terms to their parent term representing a marker type
    # for all children in the closure table - find the parent
    # which is a marker type parent
    #
    cmds = []
    cmds.append('select _AncestorObject_key, _DescendentObject_key ' + \
	    'into temp clos ' + \
            'from DAG_Closure ' + \
            'where _DAG_key = 9 ' + \
            'and _MGIType_key = 13')
    cmds.append('create index clos_idx1 on clos(_AncestorObject_key)')
    cmds.append('create index clos_idx2 on clos(_DescendentObject_key)')
    cmds.append('select t1.term as ancestorTerm, ' + \
		't2.term as descendentTerm ' + \
		'from clos c, VOC_Term t1, VOC_Term t2 ' + \
		'where c._AncestorObject_key = t1._Term_key ' + \
		'and c._DescendentObject_key = t2._Term_key ' + \
                'order by t2.term')
    results = db.sql(cmds, 'auto')
    # the mcv terms that represent marker types
    mcvMarkerTypeValues  = mkrTypeToAssocMCVTermDict.values()
    for r in results[3]:
        aTerm = r['ancestorTerm']
        dTerm = r['descendentTerm']
        if mcvTermToParentMkrTypeTermDict.has_key(dTerm):
            # we've already mapped this descendent to its marker type parent
            continue
        # dTerm may be a marker type term
        elif dTerm in mcvMarkerTypeValues:
            mcvTermToParentMkrTypeTermDict[dTerm] = dTerm
        # if the ancestor of this descendent term is a
        # marker type term load it into the dict
        elif aTerm in mcvMarkerTypeValues:
            mcvTermToParentMkrTypeTermDict[dTerm] = aTerm

    # map marker keys to their marker type
    cmd = ''' select _Marker_Type_key, _Marker_key
                from MRK_Marker
                where _Marker_Status_key = 1'''

    results = db.sql(cmd, 'auto')
    for r in results:
        mkrTypeKey = r['_Marker_Type_key']
        mkrKey = r['_Marker_key']
        mkrKeyToMkrTypeKeyDict[mkrKey] = mkrTypeKey


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
    global fpMultiMCVRpt, fpConflictRpt, fpGroupingTermRpt
    global fpBeforeAfterRpt, fpRptNamesRpt

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
        fpConflictRpt = open(conflictRptFile, 'a')
    except:
        print 'Cannot open report file: ' + conflictRptFile
        sys.exit(1)
    try:
        fpGroupingTermRpt = open(groupingTermRptFile, 'a')
    except:
        print 'Cannot open report file: ' + groupingTermRptFile
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
    fpConflictRpt.close()
    fpGroupingTermRpt.close()
    fpBeforeAfterRpt.close()


#
# Purpose: Load the data from the input file into the temp table.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def loadTempTable ():
    global annot, updatedBy

    print 'Create a bcp file from the input file'
    sys.stdout.flush()

    #
    # Read each record from the input file, perform validation checks and
    # write them to a bcp file.
    #
    line = fpInput.readline()
    updatedBy = re.split(TAB, line[:-1])[6]
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
	
	#
	# Special Case
	# If there is only an mgiID this is ok, it means 
        # curator intends to delete all annotations
	#
	if termIDExists == 0 and jNumExists == 0 and \
		evidCodeExists == 0 and inferFromExists == 0 \
		and editorExists == 0 and mgiIDExists > 0:
	    # check the MGI ID for format 
	    if re.match('MGI:[0-9]+',mgiID) == None:
                print 'Invalid MGI ID (line ' + str(count) + ')'
                fpBCP.close()
                closeFiles()
                sys.exit(1)
	    # write out to the bcp file:
	    fpBCP.write(termID + TAB + mgiID + TAB + jNum +  TAB + evidCode + \
            TAB + editor + NL)

	    # add to the annotation dictionary so it gets written to the 
	    # annotation file
	    annotList = [termID, mgiID, jNum, evidCode, inferFrom, qual, \
		editor, date, notes, ldb]
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
		print 'Invalid Term ID (line ' + str(count) + ')' + ' ' + \
		    termID
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
	    annotList = [termID, mgiID, jNum, evidCode,	inferFrom, \
		qual, editor, date, notes, ldb]
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

    bcpCmd = '%s %s %s %s "/" %s "\\t" "\\n" mgd' % \
        (BCP_COMMAND, db.get_sqlServer(), db.get_sqlDatabase(),tempTable,
        bcpFile)
    rc = os.system(bcpCmd)
    if rc <> 0:
        closeFiles()
        sys.exit(1)


# Purpose: Create report for marker type/MCV feature type conflict
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createMarkerTypeConflictReport():
    global nonfatalCount, nonfatalReportNames
    print 'Create the Markers with conflict between Marker Type and MCV Marker Type Report'
    sys.stdout.flush()
    fpConflictRpt.write(string.center('Markers whose Marker Type has been updated to match MCV Marker Type',136) + NL)
    fpConflictRpt.write(string.center('(' + timestamp + ')',136) + 2*NL)
    fpConflictRpt.write('%-16s  %-20s  %-30s  %-30s  %-30s%s' %
                     ('MGI ID','Old Marker Type','MCV Term',
                      'MCV Marker Type Term','Web Display MCV Term',NL))
    fpConflictRpt.write(16*'-' + '  ' + 20*'-' + '  ' + \
                      30*'-' + '  ' + 30*'-' + '  ' + 30*'-' + NL)

    cmds = []
    #
    # Get the MGI ID and Term IDs from the temp table
    #
    cmds.append('select tmp.termID, ' + \
                       'tmp.mgiID ' + \
                 'from ' + tempTable + ' tmp ' + \
                 'where tmp.mgiID is not null ' + \
                 'order by lower(tmp.mgiID)')

    results = db.sql(cmds,'auto')
    conflictCt = 0
    for r in results[0]:
	# get marker type
        mgiID = r['mgiID']
	if not mgiIdToMkrTypeDict.has_key(mgiID):
	    print 'MGI ID: %s not primary or not valid' % mgiID
	    continue
	mkrType = mgiIdToMkrTypeDict[mgiID]
	# get term
	termID = r['termID']
	if not termIDToTermDict.has_key(termID):
            continue
	mcvTerm = termIDToTermDict[termID]
	# get mcv marker type term and the corresponding marker type
	if not mcvTermToParentMkrTypeTermDict.has_key(mcvTerm):
	    # could be a grouping term
	    continue
	if not mcvTermToParentMkrTypeTermDict.has_key(mcvTerm):
	    continue
	mcvMkrTypeTerm = mcvTermToParentMkrTypeTermDict[mcvTerm]
	if not mcvTermToMkrTypeDict.has_key(mcvMkrTypeTerm):
	    continue
	mcvMkrType = mcvTermToMkrTypeDict[mcvMkrTypeTerm]
	if mkrType != mcvMkrType:
	    # save for later marker type update
	    markersToUpdateDict[mgiID] = mcvMkrType
	    conflictCt += 1

	    loadAssignedTerm = mkrTypeToAssocMCVTermDict[mkrType]

	    #print 'mgiID: %s mkrType: %s mcvTerm: %s mcvMkrTypeTerm: %s loadAssignedTerm: %s' % (mgiID, mkrType, mcvTerm, mcvMkrTypeTerm, loadAssignedTerm)
	    fpConflictRpt.write('%-16s  %-20s  %-30s  %-30s  %-30s%s' %
            (mgiID, mkrType, mcvTerm, mcvMkrTypeTerm, loadAssignedTerm, NL))
    fpConflictRpt.write(NL + 'Number of Conflicts between Marker Type ' +
	'and MCV Marker Type: ' + str(conflictCt) + NL)
    if conflictCt > 0 and not conflictRptFile in nonfatalReportNames:
	nonfatalReportNames.append(conflictRptFile + NL)
    nonfatalCount += conflictCt

# Purpose: Create the invalid marker report.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvMarkerReport ():
    global annot, nonfatalCount, nonfatalReportNames

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
                       'null as name, ' + \
                       'null as status ' + \
                'from ' + tempTable + ' tmp ' + \
                'where tmp.mgiID is not null and ' + \
                      'not exists (select 1 ' + \
                                  'from ACC_Accession a ' + \
                                  'where lower(a.accID) = lower(tmp.mgiID)) ' + \
                'union ' + \
                'select tmp.termID, ' + \
                       'tmp.mgiID, ' + \
                       't.name, ' + \
                       'null as status ' + \
                'from ' + tempTable + ' tmp, ' + \
                     'ACC_Accession a1, ' + \
                     'ACC_MGIType t ' + \
                'where tmp.mgiID is not null and ' + \
                      'lower(a1.accID) = lower(tmp.mgiID) and ' + \
                      'a1._LogicalDB_key = 1 and ' + \
                      'a1._MGIType_key != 2 and ' + \
                      'not exists (select 1 ' + \
                                  'from ACC_Accession a2 ' + \
                                  'where lower(a2.accID) = lower(tmp.mgiID) and ' + \
                                        'a2._LogicalDB_key = 1 and ' + \
                                        'a2._MGIType_key = 2) and ' + \
                      'a1._MGIType_key = t._MGIType_key ' + \
                'union ' + \
                'select tmp.termID, ' + \
                       'tmp.mgiID, ' + \
                       't.name, ' + \
                       'ms.status ' + \
                'from ' + tempTable + ' tmp, ' + \
                     'ACC_Accession a, ' + \
                     'ACC_MGIType t, ' + \
                     'MRK_Marker m, ' + \
                     'MRK_Status ms ' + \
                'where tmp.mgiID is not null and ' + \
                      'lower(a.accID) = lower(tmp.mgiID) and ' + \
                      'a._LogicalDB_key = 1 and ' + \
                      'a._MGIType_key = 2 and ' + \
                      'a._MGIType_key = t._MGIType_key and ' + \
                      'a._Object_key = m._Marker_key and ' + \
                      'm._Marker_Status_key = 2 and ' + \
                      'm._Marker_Status_key = ms._Marker_Status_key ' + \
                'order by mgiID, termID')

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

    numErrors = len(results[0])
    fpInvMrkRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    nonfatalCount += numErrors
    if numErrors > 0:
        if not invMrkRptFile in nonfatalReportNames:
            nonfatalReportNames.append(invMrkRptFile + NL)


#
# Purpose: Create the secondary marker report.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createSecMarkerReport ():
    global annot, nonfatalCount, nonfatalReportNames

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
                'from ' + tempTable + ' tmp, ' + \
                     'ACC_Accession a1, ' + \
                     'ACC_Accession a2, ' + \
                     'MRK_Marker m ' + \
                'where tmp.mgiID is not null and ' + \
                      'lower(tmp.mgiID) = lower(a1.accID) and ' + \
                      'a1._MGIType_key = 2 and ' + \
                      'a1._LogicalDB_key = 1 and ' + \
                      'a1.preferred = 0 and ' + \
                      'a1._Object_key = a2._Object_key and ' + \
                      'a2._MGIType_key = 2 and ' + \
                      'a2._LogicalDB_key = 1 and ' + \
                      'a2.preferred = 1 and ' + \
                      'a2._Object_key = m._Marker_key ' + \
                'order by lower(tmp.mgiID), lower(tmp.termID)')

    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
        termID = r['termID']
        mgiID = r['mgiID']

        fpSecMrkRpt.write('%-20s  %-16s  %-50s  %-16s%s' %
            (termID, mgiID, r['symbol'], r['accID'], NL))

    numErrors = len(results[0])
    fpSecMrkRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    if numErrors > 0:
        if not secMrkRptFile in nonfatalReportNames:
            nonfatalReportNames.append(secMrkRptFile + NL)
    nonfatalCount += numErrors

#
# Purpose: Create the invalid MCV/SO term ID report.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvTermIdReport ():
    global fatalCount, fatalReportNames

    print 'Create the invalid Term ID report'
    sys.stdout.flush()
    fpInvTermIdRpt.write(string.center('Invalid Term ID Report',80) + NL)
    fpInvTermIdRpt.write(string.center('(' + timestamp + ')',80) + 2*NL)
    fpInvTermIdRpt.write('%-20s%s' % ('Term ID',NL))
    fpInvTermIdRpt.write(20*'-' + NL)
    cmds = []

    #
    # Find any term IDs from the input data that are not in the database.
    #
    cmds.append('select tmp.termID ' + \
                'from ' + tempTable + ' tmp ' + \
                'where tmp.termID is not null and ' + \
                      'not exists (select 1 ' + \
                                  'from ACC_Accession a ' + \
                                  'where lower(a.accID) = lower(tmp.termID) and ' + \
                                        'a._MGIType_key = 13 and ' + \
					'a._LogicalDB_key in (145,146)) ' + \
                'order by lower(tmp.termID)')

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
    fatalCount += numErrors
    if numErrors > 0:
        if not invTermIdRptFile in fatalReportNames:
            fatalReportNames.append(invTermIdRptFile + NL)


#
# Purpose: Create the annotation to grouping terms rpt
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createGroupingTermIdReport ():
    global fatalCount, fatalReportNames

    print 'Create the Grouping Term report'
    sys.stdout.flush()
    fpGroupingTermRpt.write(string.center('Annotations to Grouping Terms Report',80) + NL)
    fpGroupingTermRpt.write(string.center('(' + timestamp + ')',80) + 2*NL)
    fpGroupingTermRpt.write('%-20s %-20s%s' % ('MGI ID', 'Term ID',NL))
    fpGroupingTermRpt.write(20*'-' + ' ' + 20*'-' + NL)
    quotedTerms=''
    for t in string.split(groupingTermIds, ','):
	quotedTerms = '%s"%s",' % (quotedTerms, t)
    quotedTerms = quotedTerms[:-1]
    cmds = []
    #
    # Find any annotations to grouping IDs
    #
    cmds.append('select tmp.mgiID, tmp.termID ' + \
                'from ' + tempTable + ' tmp ' + \
                'where tmp.termID is not null ' + \
	        'and lower(tmp.termID) in (%s) ' % quotedTerms.lower() + \
                'order by lower(tmp.termID)')
    results = db.sql(cmds,'auto')

    #
    # Write a record to the report for each grouping term annotation
    #
    for r in results[0]:
	mgiID = r['mgiID']
        termID = r['termID']
        fpGroupingTermRpt.write('%-20s%s%-20s%s' % (mgiID, TAB, termID, NL))

    numErrors = len(results[0])
    fpGroupingTermRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    fatalCount += numErrors
    if numErrors > 0:
        if not groupingTermRptFile in fatalReportNames:
            fatalReportNames.append(groupingTermRptFile + NL)

#
# Purpose: Create the invalid J Number report
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvJNumReport ():
    global fatalCount, fatalReportNames

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
		'from ' + tempTable + ' tmp ' + \
		'where tmp.jNum is not null and ' + \
		    'not exists (select 1 ' + \
		    'from ACC_Accession a ' + \
			  'where lower(a.accID) = lower(tmp.jNum) and ' + \
				'a._MGIType_key = 1 and ' + \
				'a._LogicalDB_key = 1 and ' + \
				'a.prefixPart = \'J:\' and ' + \
				'a.preferred = 1) ' + \
				'order by lower(tmp.jNum)')
    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
	jNum = r['jNum']
	fpInvJNumRpt.write('%-20s%s' % (jNum, NL))
   
    numErrors = len(results[0])
    fpInvJNumRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    fatalCount += numErrors
    if numErrors > 0:
        if not invJNumRptFile in fatalReportNames:
            fatalReportNames.append(invJNumRptFile + NL)

#
# Purpose: Create the invalid Evidence Code report
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvEvidReport ():
    global fatalCount, fatalReportNames

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
                'from ' + tempTable + ' tmp ' + \
                'where tmp.evidCode is not null and ' + \
                    'not exists (select 1 ' + \
                    'from VOC_Term t ' + \
                          'where t._Vocab_key = 80 and ' + \
                                'lower(tmp.evidCode) = lower(t.term))')
    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
        evidCode = r['evidCode']
        fpInvEvidRpt.write('%-20s%s' % (evidCode, NL))

    numErrors = len(results[0])
    fpInvEvidRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    fatalCount += numErrors
    if numErrors > 0:
        if not invEvidRptFile in fatalReportNames:
            fatalReportNames.append(invEvidRptFile + NL)

#
# Purpose: Create the invalid Editor login report
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createInvEditorReport ():
    global fatalCount, fatalReportNames

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
                'from ' + tempTable + ' tmp ' + \
                'where tmp.editor is not null and ' + \
                    'not exists (select 1 ' + \
                    'from MGI_User u ' + \
                          'where lower(u.login) = lower(tmp.editor))')
    results = db.sql(cmds,'auto')

    #
    # Write the records to the report.
    #
    for r in results[0]:
        editor = r['editor']
        fpInvEditorRpt.write('%-20s%s' % (editor, NL))

    numErrors = len(results[0])
    fpInvEditorRpt.write(NL + 'Number of Rows: ' + str(numErrors) + NL)
    fatalCount += numErrors
    if numErrors > 0:
        if not invEditorRptFile in fatalReportNames:
            fatalReportNames.append(invEditorRptFile + NL)


#
# Purpose: Create report for markers annotatd to mor than one
#	MCV term
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#

def createMultipleMCVReport():
    global nonfatalCount, annot, nonfatalReportNames

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
    if multiCt > 0 and not multiMcvRptFile in nonfatalReportNames:
	nonfatalReportNames.append(multiMcvRptFile + NL)
    nonfatalCount += multiCt

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
	symbol = mgiIDToSymbolDict[mgiID]
	inputTermIDList = []
	if inputTermIdLookupByMgiId.has_key(mgiID):
	    inputTermIDList = inputTermIdLookupByMgiId[mgiID]
	inputTermList = []
	for id in inputTermIDList:
	    if id != None:
		term = termIDToTermDict[id]
		inputTermList.append(term)
	mgdTermIDList = []
	if mgdMgiIdToTermIdDict.has_key(mgiID):
	    mgdTermIDList = mgdMgiIdToTermIdDict[mgiID]
	mgdTermList = []
	for id in mgdTermIDList:
	    term = termIDToTermDict[id]
	    mgdTermList.append(term)
	fpBeforeAfterRpt.write('%-20s  %-20s  %-30s  %-30s  %-30s  %-30s%s' %
	    (mgiID, symbol, ','.join(mgdTermIDList), ','.join(mgdTermList), \
		','.join(inputTermIDList), ','.join(inputTermList), NL))

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
# Purpose: Update markers the the MCV marker type
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def updateMarkerType ():
    for mgiID in markersToUpdateDict:
	typeTerm = markersToUpdateDict[mgiID]
	mrkTypeKey = mkrTypeToKeyDict[typeTerm]
	results = db.sql(MARKER_KEY % mgiID, 'auto')
	mrkKey = results[0]['_Marker_key']
	db.sql(UPDATE % (mrkTypeKey, updatedByKey, mrkKey), None)
    db.commit()

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
createMarkerTypeConflictReport()
createGroupingTermIdReport()

if fatalCount == 0:
    createBeforeAfterReport()
    nonfatalReportNames.append('\nBefore/After file generated. See: %s\n' % beforeAfterRptFile)
else:
    fatalReportNames.append('\nDid not generate before/after file because of errors\n')
closeFiles()

if liveRun == "1":
    createAnnotFile()
    updateMarkerType()

# write  non fatal report names to stdout
names = string.join(nonfatalReportNames,'' )
fpRptNamesRpt.write('\nNon-Fatal QC errors detected in the following files:\n')
fpRptNamesRpt.write(names)

# write fatal report names to stdout
fpRptNamesRpt.write('\nFatalQC errors detected in the following files:\n')
names = string.join(fatalReportNames,'' )
fpRptNamesRpt.write(names)

fpRptNamesRpt.close()
db.useOneConnection(0)

if fatalCount > 0: # fatal errors
    sys.exit(3)
#elif multiCt > 0 or conflictCt > 0:
elif nonfatalCount > 0:
    sys.exit(2)
else:
    sys.exit(0)
