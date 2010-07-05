#!/usr/local/bin/python
#
#  addColumns.py
###########################################################################
#
#  Purpose:
#
#       This script adds columns 9 and 10 to a file
#
#  Usage:
#
#      addColumns.py  filename
#
#      where:
#          filename = path to the input file
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#
#  Implementation:
#
#  Notes:  None
#
###########################################################################

import string
import sys
import os

USAGE = 'Usage: addColumns.py  inputFile'
TAB = '\t'
CRT = '\n'

inputFile = None
fpInput = None
fpOutput = None
outputFile = None
header = None
lineList = []
hasMissingColumns = 0

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
        print 'Cannot open input file: ' + inputFile
        sys.exit(1)
    return

#
# Purpose: Add columns 9 and/or 10 when missing
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def addColumns ():
    global header, lineList, hasMissingColumns

    header = fpInput.readline()
    lineList.append(header)
    for line in fpInput.readlines():
        tokens = string.split(line[:-1], '\t')
        numCols = len(tokens)
        # if < 8 curator will have to fix, if > 10 ok
	# if missing 9 or 10 just add it
        if numCols == 9:
            line  = '%s%s%s' % (line[:-1], TAB, CRT)
	    hasMissingColumns = 1
        elif numCols == 8:
            line = '%s%s%s%s' % (line[:-1], TAB, TAB, CRT)
	    hasMissingColumns = 1
	lineList.append(line)

#
# Purpose: write new file with added columns
# Returns: Nothing
# Assumes: Nothing
# Effects: creates file in the filesystem
# Throws: Nothing
#
def writeFile():
    global fpOutput, outputFile
    outputFile = '%s.%s' % (inputFile, os.environ['ADD_COLUMNS_EXT'])
    fpOutput = open(outputFile, 'w')
    for line in lineList:
	fpOutput.write(line)

#
# Main
#
checkArgs()
openFile()
addColumns()
if hasMissingColumns == 1:
    writeFile()
    fpOutput.close()
    print '\nInput file has missing 9th and/or 10th columns. New file: %s' % outputFile
else:
    print '\nInput file does not have missing 9th or 10th columns'
fpInput.close()
