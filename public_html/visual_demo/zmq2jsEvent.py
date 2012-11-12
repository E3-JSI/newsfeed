#!/usr/bin/env python

import zmq
import cgi, cgitb
import json
import sys
import time, datetime
import random

def utc_time_str(t):
	"yyyy-mm-dd hh:mm:ss string representing the UTC variant of a given datetime object"
	return datetime.datetime.utcfromtimestamp(time.mktime(t.replace(microsecond=0).timetuple())).isoformat().replace('T',' ')+' (UTC)'

def main():
	cgitb.enable()
	
	zmqctx = zmq.Context()
	zmqsock = zmqctx.socket(zmq.SUB)
	zmqsock.setsockopt(zmq.SUBSCRIBE, '')
	zmqsock.connect('tcp://kopernik.ijs.si:13374')
	
	print "Cache-Control: no-cache"
	print "Connection: Keep-Alive"
	print "Content-Type: text/event-stream"
	print "\n"
	sys.stdout.flush()
	
	t0 = time.time()
	while time.time()-t0<10:
		# get data
		article = zmqsock.recv_pyobj()

		# ignore articles from non-public feeds
		if 'public' not in article.get('acl_tagset', []):
			continue

		# hackish: ignore outdated articles
		age_days = (datetime.datetime.now() - (article['publish_date'] or article['found_date']).replace(tzinfo=None)).days
		if age_days > 7:
			continue
			
		# compute some pretty strings
		txt = article['cleartext']
		txt = '<p>'.join(txt.splitlines())
		gap_idx = min(txt.find(' ', 400), 500)
		if gap_idx == -1: gap_idx = 500
		if len(txt) > gap_idx:
			intro = txt[:gap_idx] + ' (...)'
		else:
			intro = txt

		# create a "javascript" event
		print "id:", article['id']
		print "data: %s\n" % json.dumps({
				'aid': article['id'],
				'url': article['url'],
				'feed_url': article['feed_url'],
				'title': article['title'],
				'intro': intro,
				'date': utc_time_str(article['publish_date'] or article['found_date']),
				'date_is_approx': article['publish_date'] is None,
				'geo': article['geo'], # or ('%f %f' % (360*random.random()-180, 360*random.random()-180))  # random part: debug only
				'pub_geo': article['source_geo'],
				'img': article['img'],
				})
		sys.stdout.flush()


if __name__ == '__main__':
	main()
