#!/usr/bin/env python
"""
A standalone script that continuously fetches news articles
from the Jozef Stefan Institute HTTP newsfeed at newsfeed.ijs.si.

End users of the newsfeed may wish to run this locally; or
use an equivalent downloader of their own.
"""

import sys, os
import glob
import errno
import urllib, urllib2
import time
import re
import traceback

# default feed
DEFAULT_FEED_URL = 'http://newsfeed.ijs.si/stream/'

def makedirs(path):
	"Create all directories on `path` as needed. If `path` already exists, do not complain."
	try:
		os.makedirs(path)
	except OSError as exc:
		if exc.errno != errno.EEXIST: raise  # EEXIST is the expected cause of exception; ignore it

def extract_timestamp(filename):
	"""
	If `filename` contains a ISO 8601 zulu formatted timetamp (yyyy-mm-ddThh:mm:ssZ), returns a pair
	(timestamp, filename without timestamp).
	Otherwise, returns (None, original filename).
	"""
	m = re.search(r'\d\d\d\d-\d\d-\d\dT\d\d-\d\d-\d\dZ', filename)
	if m is None:
		return m.group(0), filename
	else:
		template = filename.replace(m.group(0), '[time]')
		return m.group(0), template

class Fetcher:
	def __init__(self, feed_urls, start_time, output_dir, username, password):
		# Server which queries the DB
		self.feed_urls = feed_urls
		# Time of the last fetched article. Continue from here.
		self.last_seen = dict((feed_url, start_time) for feed_url in self.feed_urls)
		# Directory in which to put the fetched news
		self.output_dir = output_dir
		# Auth credentials for access to the feed
		self.username = username
		self.password = password
	def run_forever(self):
		while True:
			nothing_new = True
			for feed_url in self.feed_urls:
				# read data, dump to disk
				try:
					url = feed_url + '?after=' + urllib.quote(self.last_seen[feed_url])
					print "Trying %r" % url
					pass_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
					pass_mgr.add_password(None, url, self.username, self.password)
					auth_handler = urllib2.HTTPBasicAuthHandler(pass_mgr)
					q = urllib2.build_opener(auth_handler).open(url)

					# we got a file, save it to disk
					nothing_new = False
					filename = re.search('filename="([^"]+)"', q.headers['content-disposition']).group(1)
					makedirs(self.output_dir)
					f = open(self.output_dir+'/'+filename, 'wb')
					f.write(q.read())
					f.close()
					print 'Fetched', filename

					# extract the timestamp of the freshly fetch file (we'll continue from here)
					self.last_seen[feed_url], _ = extract_timestamp(filename)
					if self.last_seen[feed_url] is None:
						raise ValueError("Server returned a file with a malformed filename: %r; no timestamp can be parsed" % filename)
				except Exception as exc:
					if not (isinstance(exc, urllib2.HTTPError) and exc.getcode() == 404):
						traceback.print_exc()
				finally:
					try: q.close()
					except: pass
				
				if nothing_new:
					print "No new data yet, sleeping for a minute ..."
					time.sleep(60)


if __name__=='__main__':
	# specify cmd-line args
	from optparse import OptionParser
	parser = OptionParser(usage="Usage: %prog USERNAME:PASSWORD [options]")
	parser.add_option("-f", "--feed-url", dest="feed_urls", action="append", metavar='URL', 
		help="URL of the newsfeed. Default: %s" % DEFAULT_FEED_URL, default=[])
	parser.add_option("-o", "--output", dest="output_dir", metavar='DIRECTORY', 
		help="Output directory for gzip files. Default: current directory.", default='.')
	parser.add_option("-a", "--after", dest="after", metavar='TIMESTAMP',
		help="Fetch news articles newer than this timestamp only. Use the ISO format (yyyy-mm-ddThh:mm:ssZ). Default: latest timestamp in the output directory", default=None)

	# parse cmd-line args
	options, args = parser.parse_args()
	if options.feed_urls == []:
		options.feed_urls.append(DEFAULT_FEED_URL)
	if options.output_dir == None or len(args)!=1 or args[0].count(':')!=1:
		parser.print_help()
		sys.exit(1)
	if options.after == None:
		timestamps = [extract_timestamp(fn) for fn in glob.glob(options.output_dir+'/*.gz')]
		timestamps = [(ts, feed) for (ts, feed) in timestamps if ts]
		feeds = set(feed for (ts, feed) in timestamps)
		feed_latest = [max(ts for (ts, f) in timestamps if f==feed) for feed in feeds]
		options.after = min(feed_latest or ['0000-00-00T00-00-00Z'])
		print 'The --after option was automatically set to', `options.after`
	username, password = args[0].split(':')
	
	fetcher = Fetcher(feed_urls=options.feed_urls, start_time=options.after, output_dir=options.output_dir, username=username, password=password)
	fetcher.run_forever()
