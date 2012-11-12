"""
Get a sample of articles from the news DB, show language distribution according to
 - existing 'lang' column in the DB
 - google's CLD (executed on the fly for each article)
"""

import os, sys
sys.path.extend(('.','..'))
from cleanDb import openConnection
import cld

conn, cur = openConnection()
cur = conn.cursor('x')
cur.execute("SELECT m.id, p.content, m.lang_altcode FROM processed_article p JOIN feed_article_meta m ON (p.feed_articleid = m.id) WHERE p.mode='cleartext' ORDER BY m.id DESC LIMIT 100000")

cnt = {}
cnt2 = {}
while True:
	row = cur.fetchone()
	if not row: break
	aid, txt, lang = row; lang = str(lang[:2])
	lang2 = cld.detect(txt.encode('utf8','ignore'))[1]
	cnt[lang] = cnt.get(lang,0)+1
	cnt2[lang2] = cnt2.get(lang2,0)+1
	print 'done',sum(cnt.itervalues())


print 'done'

def top(d,n=60):
	for pair in sorted(d.iteritems(), key=lambda pair: -pair[1])[:n]:
		print '%s %5d' % pair

print 'DATABASE SAYS:'
top(cnt)
print '\nCLD SAYS:'
top(cnt2)
