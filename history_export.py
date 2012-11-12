"""
Export articles from the DB into a sqlite table with XML-encoded articles.
Enrych them on the fly.

This file is intended to be used (and modified as need be; this is nothing stable)
for articles that are not available through the real-time pipeline
or where custom export nuances are needed. Written for exporting rych google
clusters for RENDER.

(Sqlite is only used to avoid having directories with 1e6 files or huge
aggregate files with no easy way to tell which articles are in there.)
"""

import os, sys; sys.path.extend(('.','..','../langdet'))
import sqlite3
import time
import threading
import lxml
from Queue import Queue

import cld

from cleanDb import openConnection
import zmq2zmq_enrych; reload(zmq2zmq_enrych)
from zmq2zmq_enrych import is_enrychable, enrycher_worker
import db2zmq_cleartext2; reload(db2zmq_cleartext2)
from db2zmq_cleartext2 import DB_get_full_article
import serialize2; reload(serialize2)
from iso_map import iso2to3

DB_OUT = '/tmp/deleteme.sqlite'  # path to the sqlite DB
MAX_ENRYCHER_REQUESTS = 2     # max number of simultaneous requests

conn_in, cur_in = openConnection('history_export')
cur_in_ids = conn_in.cursor('foo')  # named cursor for incremental fetching
conn_out = sqlite3.connect(DB_OUT); conn_out.isolation_level = None; cur_out = conn_out.cursor();

# set up the output DB
cur_out.execute("CREATE TABLE IF NOT EXISTS news (article_id integer primary key, story_id text, xml text);")
cur_out.execute("CREATE INDEX IF NOT EXISTS article_id_idx ON news(article_id);")

# fetch IDs to export
#cur_in_ids.execute("SELECT feed_articleid, story_id FROM feed_article_googles g JOIN feed_article fa ON (fa.id=g.feed_articleid) WHERE fa.id>45909130 AND fa.found BETWEEN '2012-04-01' AND '2012-05-01' ORDER BY id")  # 537466 articles; not taking lang into account
#cur_in_ids.execute("SELECT feed_articleid, story_id FROM feed_article_googles")
#cur_in_ids.execute("SELECT feed_articleid, story_id FROM feed_article_googles g JOIN feed_article fa ON (fa.id=g.feed_articleid) LIMIT 10 OFFSET 10")
#cur_in_ids.execute("SELECT 20932789 AS feed_articleid, 'FAKE_STORY' as story_id")
cur_in_ids.execute("SELECT feed_articleid, story_id FROM feed_article_googles g JOIN feed_article fa ON (fa.id=g.feed_articleid) ORDER BY fa.id DESC LIMIT 1000 OFFSET 1000")
id_rows = cur_in_ids.fetchall()
conn_in.commit()
assert  id_rows

# input and output queues for worker threads that call enrycher. (zmq is only used in the main thread)
txt_queue = Queue(maxsize=MAX_ENRYCHER_REQUESTS)
rych_queue = Queue(maxsize=MAX_ENRYCHER_REQUESTS)

print 'Starting history export of %d articles' % cur_in_ids.rowcount
for row in id_rows:
	article_id, story_id = row
	cur_out.execute("SELECT 1 FROM news WHERE article_id=?", (article_id,))
	if cur_out.fetchone():
		print 'Already done', article_id
		continue

	article = DB_get_full_article(cur_in, article_id)
	article['story_id'] = story_id
	article['rych'] = '<item> We know that <math> 3 &lt; 5 </math> </item>'
	article['title'] += ' hoću'.decode('utf8')

	print 'done', article['id']
	xml = serialize2.xml_encode(article)
	#print xml
	assert(max(map(ord,xml)) < 128)
	lxml.etree.fromstring(xml)
	
