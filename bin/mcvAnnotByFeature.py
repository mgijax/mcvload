#!/usr/local/bin/python

'''
#
#
# Report:
#      counts for each Feature Type with an MCV annotation
#
# Usage:
#	
#       mcvAnnotByFeature.py
#
# History:
#
# sc	06/14/10
#	- created
#
'''
 
import sys 
import db
import reportlib

db.setAutoTranslate(False)
db.setAutoTranslateBE(False)

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

cmds.append('''select t.term as feature, 0 as featureCount
	into temp notUsed
	from VOC_Term t
	where t._Vocab_key = 79
	and not exists (select 1
	from VOC_annot va 
	where va._AnnotType_key = 1011
		and va._Term_key = t._Term_key)''')

cmds.append('''select va.*, t.term as feature
        into temp mcv
        from VOC_Annot va left outer join
	VOC_Term t on t._term_key = va._term_key
        where va._AnnotType_key = 1011''')

cmds.append('''select feature, count(feature) as featureCount
	from mcv
	group by feature
	union
	select feature, featureCount
	from notUsed
	order by feature''')

results = db.sql(cmds, 'auto')

fp.write('MCV Annotations by Feature%s%s' %(CRT, CRT))
for r in results[2]:
    fp.write('%s%s%s%s' % (r['feature'], TAB, r['featureCount'], CRT))

