#
#  checkColumns.py
###########################################################################
#
#  Purpose:
#
#       This script checks that there are the correct number of columns
#	in a file
#
#  Usage:
#
#      checkColumns.py  filename numColumns	
#
#      where:
#          filename = path to the input file
#
#  Env Vars:
#
#      The following environment variables are set by the configuration
#      files that are sourced by the wrapper script:
#
#  Inputs:
#
#  Outputs:
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  Discrepancy errors detected in the input files
#  Implementation:
#
#  Notes:  None
#
###########################################################################


import string
import sys

USAGE = 'Usage: mcvloadQC.py  inputFile numColumns'
TAB = '\t'

inputFile = None
fpInput = None
numColumns = None
errors = 0

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def checkArgs ():
    global inputFile, numColumns

    if len(sys.argv) != 3:
        print(USAGE)
        sys.exit(1)

    inputFile = sys.argv[1]
    numColumns = int(sys.argv[2])
    #print 'Incoming numColumns: %s' % sys.argv[2]
    return

#
# Purpose: Open the file for reading
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFile ():
    global fpInput

    try:
        fpInput = open(inputFile, 'r')
    except:
        print('Cannot open input file: ' + inputFile)
        sys.exit(1)
    return

#
# Purpose: check the file for proper number of columns
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def checkColumns ():
    global errors
    lineNum = 0
    print("\n\nLines With Missing Columns")
    print("--------------------------")
    for line in fpInput.readlines():
        lineNum = lineNum + 1
        columns = str.split(line, TAB)
        nc = len(columns) 
        #print 'lineNum %s' % lineNum
        #print 'colNum: %s' % nc
        #print 'columns: %s' % columns
        #print 'typeNC: %s configColumns: %s' % (type(nc), type(numColumns))
        #print 'nc  %s < numColumns %s = %s ' % (nc, numColumns, nc < numColumns)
        if nc < numColumns:
            print('lineNum: %s, columns: %s numColumns: %s' % (lineNum, columns, nc))
            errors = errors + 1
    return

#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFile():
    global fpInput

    fpInput.close()
    return

checkArgs()
openFile()
checkColumns()
closeFile()
if errors > 0:
    sys.exit(1)
