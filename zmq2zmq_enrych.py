#!/usr/bin/env python

"""
Subscribes to the zmq feed of cleartexted articles.
Pushes them (multithreaded) to enrycher, publishes
enryched documents to a new zmq socket.
Also extracts and dumps some selected info from enrycher output
back into the feed_article_meta table in DB.

ZMQ subscribes to: db2zmq_cleartext.py
ZMQ subscribers:   zmq2zmq_xenrych.py
"""

import os, sys, traceback
import urllib2
import threading
import time
from Queue import Queue
import zmq
import re
import random

sys.path.extend(('.', '..'))
from cleanDb import openConnection
from db2zmq_cleartext import lat_lon_dict

# max number of concurrent requests
MAX_ENRYCHER_REQUESTS = 10

def is_enrychable(article):
	"A pipeline filter; articles for which this returns True get enryched."
	return (article['lang'] or 'xx').split('-')[0] in ('en','eng','enz','slv','sl') and article.get('cleartext') and len(article['cleartext'])<50000

def enrych(txt, url):
	"""
	Process plaintext `txt` (unicode or utf8) with enrycher;
	return resulting xml (unicode).
	`url` is the URL at which Enrycher lives.
	"""
	if isinstance(txt, unicode):
		txt = txt.encode('utf8', 'ignore')
	http_data = txt.lstrip().replace('\n','\n\n')

	req = urllib2.Request(url=url, data=http_data)
	f = urllib2.urlopen(req, timeout=3)
	return f.read().decode('utf8','replace')


def DB_write_rych_info(cur, article):
	"""
	Parse enrycher-mentioned geographical entities, add their coords to the DB.
	Also, extend the 'geo' attribute of `article`.
	"""
	geo_ids = map(int, re.findall(r'resource="http://sws.geonames.org/(\d+)/"', article['rych']))
	if geo_ids:
		cur.execute("SELECT geo FROM feed_article_meta WHERE id=%s AND geo IS NOT NULL UNION SELECT latitude::text||' '||longitude::text AS geo FROM geonames WHERE id IN (%s)" % (article['id'], ','.join(map(str, geo_ids)),) )
		geo_coords = [row['geo'] for row in cur]
		# update the DB
		cur.execute("UPDATE feed_article_meta SET geo=%s WHERE id=%s", (';'.join(geo_coords), article['id'],))
		cur.connection.commit()
		# update the zmq object; `geo_coords` includes the old entries, so we just override
		article['geo'] = map(lat_lon_dict, geo_coords)
		

def enrycher_worker(in_queue, out_queue, url=None):
	"""
	Worker thread. Takes an article dict from in_queue, adds the enrycher xml,
	puts the enryched article in out_queue.
	If `url` is given, queries Enrycher at that URL, otherwise the URL is constructed
	based on the language of each artcile in in_queue.
	"""
	conn, cur = openConnection('rych info writer')
	while True:
		try:
			article = in_queue.get()
			lang = article.get('lang','').split('-')[0]

			# auto-detect URL
			if not url:
				if lang in ('en','eng','enz'):
					if 0 and article.get('google_story_id'):
						url = 'http://aidemo.ijs.si:8080/EnrycherWeb-render/run-render'  # all + stanford parses + sentiment 
					else:
						url = 'http://aidemo.ijs.si:8080/EnrycherWeb-render/run-demo'
				elif lang in ('sl','slv'):
					url = 'http://aidemo.ijs.si:8080/EnrycherWeb-render/sl-run'
				else:
					raise ValueError('Unsupported language: %r' % lang)

			#print '[%s] pre-enrych %s' % (threading.currentThread().name, article['id'])
			#print article['id'], lang, `article.get('google_story_id')`, url
			article['rych'] = enrych(article['cleartext'], url)
			#print '[%s] pre-db %s' % (threading.currentThread().name, article['id'])
			DB_write_rych_info(cur, article)
			#print '[%s] pre-out-enqueue %s' % (threading.currentThread().name, article['id'])
			out_queue.put(article)

		except Exception as exc:
			# pass through the unenryched article
			out_queue.put(article)

			# report error
			print '!! error while processing article %s (lang %s) at %r' % (article.get('id'), article.get('lang'), url)
			txt = article.get('cleartext', '').replace('\n',' ')
			print 'Some stats about the input data: %d bytes, %d sentences, max sentence length %d bytes. File saved to /tmp/bad_enrycher_input' % (
				len(txt), len(txt.split('. ')), max(map(len,txt.split('. '))+[-1]) )
			print exc, exc.args
			try:
				with open('/tmp/bad_enrycher_input','w') as badf:
					badf.write(txt.encode('utf8'))
			except:
				print '(file not saved, IOError)'

if __name__=='__main__':
	zmqctx = zmq.Context()

	sock_txt = zmqctx.socket(zmq.SUB)
	sock_txt.connect ("tcp://localhost:13371")
	sock_txt.setsockopt(zmq.SUBSCRIBE, "")

	sock_rych = zmqctx.socket(zmq.PUB)
	sock_rych.setsockopt(zmq.HWM, 100)
	sock_rych.bind('tcp://*:13372')

	# input and output queues for worker threads that call enrycher. (zmq is only used in the main thread)
	in_queue = Queue(maxsize=MAX_ENRYCHER_REQUESTS)
	out_queue = Queue(maxsize=100*MAX_ENRYCHER_REQUESTS)
	
	# prepare worker threads
	for i in range(MAX_ENRYCHER_REQUESTS):
		worker = threading.Thread(target=enrycher_worker, args=(in_queue,out_queue))
		worker.start()

	try:
		while True:
			if in_queue.full():
				print 'sleep ... %d:%d ...' % (in_queue.qsize(), out_queue.qsize(),),
				time.sleep(1)
				print '!'
				
			if not in_queue.full() and zmq.select([sock_txt], [], [], 3)[0]:
				article = sock_txt.recv_pyobj()				
				if is_enrychable(article):
					print 'enqueued %s (lang=%r)' % (article['id'], article['lang'])
					print '%d:%d' % (in_queue.qsize(), out_queue.qsize(),),
					in_queue.put(article)
				else:
					print 'ignored %s (lang=%r)' % (article['id'], article['lang'])
					print '%d:%d' % (in_queue.qsize(), out_queue.qsize(),),
					out_queue.put(article)

			while not out_queue.empty():
				article = out_queue.get()
				print '%d:%d' % (in_queue.qsize(), out_queue.qsize(),),
				if 'rych' in article:
					print 'done %s, %d bytes of xml' % (article['id'], len(article['rych']))
				sock_rych.send_pyobj(article)
	except:
		traceback.print_exc()
	finally:
		sock_txt.close()
		sock_rych.close()
		zmqctx.term()
