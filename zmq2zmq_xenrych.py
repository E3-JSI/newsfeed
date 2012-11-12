#!/usr/bin/env python

"""
Subscribes to the zmq feed of cleartexted articles.
Pushes them (multithreaded) to enrycher, publishes
enryched documents to a new zmq socket.
Also extracts and dumps some selected info from enrycher output
back into the feed_article_meta table in DB.

ZMQ subscribes to: zmq2zmq_enrych.py
ZMQ subscribers:   zmq2zmq_bloombergness.py
"""

import os, sys, traceback
import urllib,urllib2
import time
import zmq
import re
import lxml.etree as etree

sys.path.extend(('.', '..'))
import serialize

def build_xlike_url(article):
	"""
	Return the URL to which to sent `article`, depending on article['lang'].
	**Return None** if the article language is not supported.
	"""
	#return None
	xlang = {'cat':'ca', 'eng':'en', 'spa':'es'}.get(article.get('lang'))
	if not xlang: return None
	return 'http://sandbox-xlike.isoco.com/services/analysis_%s/analyze' % xlang

tst="""ON a rainy day in the
late 17th century, an enterprising agent of the British East India
Company named Job Charnock sailed along the Hooghly River, a tributary
of the Ganges that flows from high in the Himalayas into the Bay of
Bengal, and pitched a tent on its swampy banks. The company bought three
riverside villages. Soon they would become a port - flowing with opium,
muslin and jute - and then, as the capital of British India until 1912,
draw conquerors, dreamers and hungry folk from all over the world.
"""

def add_xenrycher_data(article):
	"""
	Query XLIKE enrycher-like services to obtain the rych version of article.
	If things go well, store the rych version in article['xrych'].
	Returns None.
	"""
	try:
		# build query string
		query = urllib.urlencode({'text': article['cleartext'].encode('utf8','replace')}).replace('+','%20')
		# do the request
		req = urllib2.Request(url=build_xlike_url(article), data=query)
		f = urllib2.urlopen(req, timeout=1)
		retval = f.read().decode('utf8','replace')
		# reformat the XML
		try: retval = etree.tostring(etree.fromstring(retval), pretty_print=True)
		except: pass
		article['xrych'] = retval
		print 'OK, %d bytes of xrych xml' % len(retval or '')
	except:
		import tempfile
		traceback.print_exc()
		#tmp_path = tempfile.mktemp(prefix='isoco', suffix='.tmp')
		#with open(tmp_path,'w') as f:
		#	f.write(query)
		#print 'written', tmp_path

if __name__=='__main__':
	zmqctx = zmq.Context()

	sock_txt = zmqctx.socket(zmq.SUB)
	sock_txt.connect ("tcp://localhost:13372")
	sock_txt.setsockopt(zmq.SUBSCRIBE, "")

	sock_rych = zmqctx.socket(zmq.PUB)
	sock_rych.setsockopt(zmq.HWM, 100)
	sock_rych.bind('tcp://*:13373')

	try:
		while True:
			while not zmq.select([sock_txt], [], [], 3)[0]:
				time.sleep(.1)
			article = sock_txt.recv_pyobj()

			if build_xlike_url(article):
				print 'processing %r (%s)' % (article['id'], article['lang'],)
				add_xenrycher_data(article)
			else:
				print '(%s %s)' % (article['id'], article['lang'])

			sock_rych.send_pyobj(article)
	except:
		traceback.print_exc()
	finally:
		sock_txt.close()
		sock_rych.close()
		zmqctx.term()
