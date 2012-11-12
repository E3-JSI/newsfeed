#!/usr/bin/env python

"""
Subscribes to the zmq feed of cleartexted articles.
Pushes them (multithreaded) to enrycher, publishes
enryched documents to a new zmq socket.
Also extracts and dumps some selected info from enrycher output
back into the feed_article_meta table in DB.

ZMQ subscribes to: zmq2zmq_enrych.py
ZMQ subscribers:   zmq2http_all.py
"""

import os, sys, traceback
import urllib2
import time
import zmq
import re

sys.path.extend(('.', '..'))
import serialize
#from cleanDb import openConnection

def is_bloomberg_scorable(article):
	return article.get('lang')=='deu' or 'bloomberg' in article.get('source_hostname')

def add_bloomberg_score(article):
	"""
	Adds a new attribute, 'bloomberg_score', to `article`. Returns None.
	Uses Andrej Muhic's MATLAB service.
	On failure, returns the unchanged article.
	"""
	try:
		http_data = '<article-set>\n'+serialize.xml_encode(article)+'\n</article-set>'
		try:
			req = urllib2.Request(url='http://xling.ijs.si:9000/bloombergostxml', data=http_data)
			print 'XX:', `urllib2.urlopen(req, timeout=1).read()`
		except Exception as e:
			print 'XX:', `e`
		req = urllib2.Request(url='http://xling.ijs.si:9000/bloombergostxml', data=http_data)
		f = urllib2.urlopen(req, timeout=1)
		retval = f.read().decode('utf8','replace')
		print retval
		article['bloomberg_score'] = retval
	except:
		traceback.print_exc()

if __name__=='__main__':
	zmqctx = zmq.Context()

	sock_txt = zmqctx.socket(zmq.SUB)
	sock_txt.connect ("tcp://localhost:13373")
	sock_txt.setsockopt(zmq.SUBSCRIBE, "")

	sock_rych = zmqctx.socket(zmq.PUB)
	sock_rych.setsockopt(zmq.HWM, 100)
	sock_rych.bind('tcp://*:13374')

	try:
		while True:
			while not zmq.select([sock_txt], [], [], 3)[0]:
				time.sleep(.1)
			article = sock_txt.recv_pyobj()

			if is_bloomberg_scorable(article):
				print 'processing %r' % (article['id'], )
				add_bloomberg_score(article)
			else:
				print '(%s %s)' % (article['id'], article['lang'])

			sock_rych.send_pyobj(article)
	except:
		traceback.print_exc()
	finally:
		sock_txt.close()
		sock_rych.close()
		zmqctx.term()
