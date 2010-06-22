#!/usr/local/bin/python

'''
#
# template.py 06/09/00
#
# Report:
#       Markers of type gene with no MCV annotations
#
# Usage:
#       geneNoMcvAnnot.py
#
# History:
#
# lec	06/07/10
#	- created
#
'''
 
import sys 
import db
import reportlib

CRT = reportlib.CRT
SPACE = reportlib.SPACE
TAB = reportlib.TAB
PAGE = reportlib.PAGE

#
# Main
#

fp = reportlib.init(sys.argv[0], '')

#
# cmd = sys.argv[1]
#
# or
#
# cmd = 'select * from MRK_Marker where _Species_key = 1 and chromosome = "1"'
#

cmds = []
cmds.append('''select _Marker_key, _Marker_Type_key, symbol
	into #noAnnot
	from MRK_Marker m
	where m._Marker_Status_key = 1
	and _Organism_key = 1
	and _Marker_Type_key = 1
	and not exists(select 1
	from  VOC_Annot v
	where m._Marker_key = v._Object_key
	and v._AnnotType_key = 1011)''')

cmds.append('''select a.accid, n.symbol
	    from #noAnnot n, ACC_Accession a
	    where n._Marker_key = a._Object_key
	    and a._MGIType_key = 2
	    and a.preferred = 1
	    and a._LogicalDB_key = 1
	    and a.prefixPart = "MGI:"
	    order by n.symbol''')

results = db.sql(cmds, 'auto')

fp.write('%s Genes with no MCV Annotation%s%s' %(len(results[1]),CRT, CRT))
for r in results[1]:
    fp.write(r['accid'] + TAB + r['symbol'] + CRT)

#reportlib.trailer(fp)
#reportlib.finish_nonps(fp)	# non-postscript file
#reportlib.finish_ps(fp)	# convert to postscript file

