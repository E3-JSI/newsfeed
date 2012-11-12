#!/usr/bin/env python

import cgi
import cgitb
cgitb.enable()

import os, sys
sys.path.extend(('../../','../../dispatch','../../langdet'))
from db2zmq_cleartext import DB_get_full_article
import pprint

import psycopg2, psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

def htmlStr(x):
	if type(x)==unicode:
		x = x.encode('utf8','replace')
	else:
		x = str(x)

	assert type(x)==str
	if x.startswith('http://') and len(x)<300:
		return '<a href="%s">%s</a>' % (x.replace('&','&amp;').replace('<','&lt;').replace('"','&quot;'), x)
	else:
		return x.replace('&','&amp;').replace('<','&lt;').replace('\n','<br>')

	

def resultTableToHtml(rows, columns=None, ignore_columns=[]):
	"""
	HTML display of DB results. If `columns` is omitted, it's autodetected
	`rows` should be a list of dicts.
	"""
	if not rows:
		return '(No rows)'
	
	columns = columns or sorted(rows[0].keys())
	columns = [c for c in columns if c not in ignore_columns]
	
	ret = '<table border="1"><tr>' + ''.join('<th>%s</th>' % c for c in columns) + '</tr>'
	for row in rows:
		ret += '<tr>' + ''.join('<td>%s</td>' % htmlStr(row[c]) for c in columns) + '</tr>'
	ret += '</table><br>\n'
	return ret


def articleDetail(articleHandle):
	# connect to DB
	conn = psycopg2.connect(database='news', host='maximus', user='mitjat', password='XXX_GITHUB_XXX')
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute("SET bytea_output TO 'escape'")
	res = ''

	# identify the article
	if articleHandle.isdigit():
		articleId = int(articleHandle)
	else:
		cur.execute("SELECT fa_id FROM feed_article_urls WHERE url_hash(%s)=url_hash(url) and %s=url LIMIT 1", (articleHandle,articleHandle))
		if cur.rowcount:
			articleId = cur.fetchone()[0]
		else:
			return 'URL not in the DB'
	res += '<h1>Article %s</h1>' % articleId

	# fetch what db2zmq sees
	zmq_data = DB_get_full_article(cur, articleId)
	res += '\n\n<h3>zmq</h3>' + '<pre>'+pprint.pformat(zmq_data)+'</pre>'

	# fetch raw results
	cur.execute("SELECT * FROM feed_article_meta WHERE id=%s", (articleId,))
	res += '\n\n<h3>feed_article_meta</h3>' + resultTableToHtml(cur.fetchall())

	cur.execute("SELECT * FROM feed_article WHERE id=%s", (articleId,))
	rows = cur.fetchall()
	res += '\n\n<h3>feed_article</h3>' + resultTableToHtml(rows)
	feedId = rows[0]['feedid']
	siteId = rows[0]['feedsiteid']

	cur.execute("SELECT * FROM feed WHERE id=%s", (feedId,))
	res += '\n\n<h3>feed</h3>' + resultTableToHtml(cur.fetchall())

	cur.execute("SELECT * FROM site WHERE id=%s", (siteId,))
	res += '\n\n<h3>site</h3>' + resultTableToHtml(cur.fetchall())

	cur.execute("SELECT * FROM processed_article WHERE feed_articleid=%s", (articleId,))
	rows = cur.fetchall()
	res += '\n\n<h3>processed_article</h3>' + resultTableToHtml(rows, ignore_columns=['content'])
	for (i,row) in enumerate(rows):
		res += '<b>content (row %d)</b><pre style="border: solid black 1px">%s</pre>' % (i, row['content'].encode('utf8','replace').replace('&','&amp;').replace('<','&lt;'))

	cur.execute("""SELECT story_id, ARRAY_AGG('<li><a href="?id='||m.id||'">'||m.id||'</a> - '||COALESCE(m.title,'(no title)')) AS story_articles FROM feed_article_googles g JOIN feed_article_meta m ON (g.feed_articleid=m.id) WHERE story_id=(SELECT story_id FROM feed_article_googles WHERE feed_articleid=%s) GROUP BY story_id;""", (articleId,))
	res += '\n\n<h3>Google clusters/stories</h3>'
	for row in cur:
		res += '<b>Story '+str(row['story_id'])+'</b><ul>'+'\n'.join(row['story_articles'])+'</ul>'

	cur.execute("SELECT * FROM article WHERE feed_articleid=%s", (articleId,))
	rows = cur.fetchall()
	res += '\n\n<h3>article</h3>' + resultTableToHtml(rows, ignore_columns=['content'])
	for (i,row) in enumerate(rows):
		res += '<b>content (row %d)</b><div style="font-family: monospaced; border: solid black 1px">%r</div>' % (i, str(row['content']).replace('&','&amp;').replace('<','&lt;'))

	return res

header = """
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8"/>
</head>
<body>
"""

footer = """
</body>
</html>
"""

if __name__=='__main__':
	# parse request
	form = cgi.FieldStorage()
	articleHandle = form.getvalue('id')

	# print response
	print 'Content-type: text/html\n\n'
	
	print 'New query: ID or URL'
	print '<form target="" method="GET"><input name="id"><input type="submit"></form>'
	
	if articleHandle:
		print header + articleDetail(articleHandle) + footer
	else:
		print 'Gimme an id'
		