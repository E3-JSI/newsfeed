#!/usr/bin/env python
"""
Listens to incoming cleartext and enryched documents on zmq;
packages them into gzip files;
makes those files available via HTTP.

The HTTP interface is public facing and the final stage of the pipeline.

ZMQ subscribes to: zmq2zmq_enrych.py
ZMQ subscribers:   none; HTTP is used from here on
"""

import sys, os
sys.path.extend(('.', '..'))

import threading
import traceback, errno
import datetime, time
import gzip
import re
import glob
import zmq
import bottle

import util
import serialize

# Apache's .htpasswd file with extra information: ACL tags users can access
HTPASSWD_FILE = '../newsfeed_users.htpasswd'

# The cache of gzipped files
MAX_PRECACHE_SIZE = 10 * 1024**2  # in bytes; files larger than this are gzipped and made available
MAX_PRECACHE_AGE = 1800  # in seconds; files older than this are gzipped and made available
MAX_CACHE_AGE = 365  # in days; files older than this get deleted from cache
CACHE_DIRECTORY = 'cache.v%s' % serialize.FORMAT_VERSION
FILENAME_TEMPLATE = CACHE_DIRECTORY+'/%(acl_tag)s/news-%(time)s.xml'


def compress_gzip(path):
	"""
	Compress file with path `path` to `path`.gz with gzip. Delete `path` from disk.
	"""
	f_in = open(path, 'rb')
	f_out = gzip.GzipFile(os.path.split(path)[1], mode='wb', fileobj=open(path+'.gz', 'wb'))
	while True:
		buf = f_in.read(1024*1024) # 1MB
		if not buf: break
		f_out.write(buf)
	f_out.close()
	f_in.close()
	
	# If we get here with no exception, the original is safe to delete
	os.remove(path)

def makedirs(path):
	"Create all directories on `path` as needed. If `path` already exists, do not complain."
	try:
		os.makedirs(path)
	except OSError as exc:
		if exc.errno != errno.EEXIST: raise  # EEXIST is the expected cause of exception; ignore it
	
def write_and_rotate(f, fn_template, data, root_xml_tag='article-set'):
	"""
	Write string `data` to filehandle `f`. If `f` is None or closed,
	write into a new file with path `fn_template`. `fn_template`
	can contain %(time)s which gets replaced with current timestamp.
	If the file grows over `MAX_PRECACHE_SIZE` or older than `MAX_PRECACHE_AGE`,
	it get gzipped.

	If `root_xml_tag` is given, makes sure each file is wrapped in an
	xml element of that name and that <?xml ...> header is present.
	
	Returns the file handle of the file `data` was actually written
	into.
	"""
	if f is None or f.closed:
		fn = fn_template % {
			'time': util.iso_utc_time(datetime.datetime.now()).replace(':','-') }
		makedirs(os.path.split(fn)[0])
		f = open(fn, 'wb')
		if root_xml_tag:
			f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
			f.write('<%s format-version="%s">\n' % (root_xml_tag, serialize.FORMAT_VERSION))

	f.write(data)
	f.flush()
	
	m = re.search(r'\d\d\d\d-\d\d-\d\dT\d\d-\d\d-\d\dZ', f.name)
	try:
		# Parse the timestamp from the filename. Forgive ye oh us sinners.
		file_start = time.mktime(datetime.datetime(*map(int, re.split('[^\d]', m.group(0))[:-1])).timetuple())
		file_age = time.mktime(datetime.datetime.utcnow().timetuple())  - file_start  # time.time() is not OK because of timezones
	except:
		print 'Warning: could not parse filename %r. Traceback follows.' % f.name
		traceback.print_exc()
		file_age = -1
		
	if f.tell() > MAX_PRECACHE_SIZE or file_age > MAX_PRECACHE_AGE:
		if root_xml_tag:
			f.write('</%s>' % root_xml_tag)
		f.close()
		compress_gzip(f.name)
		print 'created', f.name+'.gz'
		
	return f
	
@util.restart_on_crash(log_exprs=['article','active_f','acl_tag'])
def zmq_to_files():
	"""
	Infinite loop: listen on the zmq socket for cleartext and rych docs,
	pack them into gzip files.
	Articles are first accumulated in *.xml files; once those grow over 10MB,
	they are turned into *.gz.
	These files are collected in subdirectories of `CACHE_DIRECTORY`: for each ACL tag
	associated with the article, the article is stored into CACHE_DIRECTORY/acl_tag/ into
	a file as described above.
	"""
	sock_in = zmqctx.socket(zmq.SUB)
	sock_in.connect ("tcp://kopernik.ijs.si:13374")  # output sockets: 13371=cleartext, 13372=enryched, 13373=xlike-enryched, 13374=bloombergness
	sock_in.setsockopt(zmq.SUBSCRIBE, "")
	
	active_f = {}   # XML output files.  acl_tag -> active file object 
		
	while True:
		global article
		article = sock_in.recv_pyobj()

		# hackish: ignore outdated articles
		age_days = (datetime.datetime.now() - (article['publish_date'] or article['found_date']).replace(tzinfo=None)).days
		if age_days > 7:
			print 'skipping %s (%s, %d days old)' % (article['id'], (article['url']+'/FAKE/FAKE/').split('/')[2], age_days)
			continue

		# write the article for each ACL tag
		for acl_tag in article.get('acl_tagset'):
			fn_template = FILENAME_TEMPLATE.replace('%(acl_tag)s',acl_tag)  # hack: partial string interpolation

			# Get the file object into which we have to write this article. (None if no such .xml exists yet)
			if not active_f.get(acl_tag):
				# Reuse the xml file from the previous run, if any
				fn = get_cached_file(fn_template, reverse_order=True)
				active_f[acl_tag] = open(fn, 'ab') if fn else None

			# write the article
			print 'processing %s (ACL=%s; %s%s%s)' % (article['id'], acl_tag, 'txt'*int('cleartext' in article), ' rych'*int('rych' in article), ' xrych'*int('xrych' in article))
			xml = serialize.xml_encode(article)+"\n"
			active_f[acl_tag] = write_and_rotate(active_f[acl_tag], fn_template, xml)


def get_cached_file(fn_template, after='0000-00-00T00-00-00Z', reverse_order=False):
	"""
	Get the alphabetically (= chronologically) first file whose path
	fits `fn_template` but has the date component larger than `after`
	(an ISO timestamp string);
	the template should contain "%(time)s". Returns filename or None
	if no such file exists.
	At the same time, deletes any file that fits the template and is
	older than `MAX_CACHE_AGE`.
	If `reverse_order` is given, return the newest matching file instead
	of the oldest.
	"""
	after = after.replace(':','-')  # normalize 
	
	def file_age(fn):
		"Age of file in days. Assumes the filename template from outer scope."
		match = re_date.search(fn)
		time_parts = map(int, match.groups()[:-1])
		file_time = datetime.datetime(*time_parts)
		return (datetime.datetime.utcnow() - file_time).days
			
	re_date = re.compile(re.escape(fn_template).replace(
		re.escape("%(time)s"),
		r"(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d)-(\d\d)-(\d\d)(?:[-.]\d+)?Z"))
	glob_pattern = fn_template.replace("%(time)s", "*")
	fns = sorted(fn.replace('\\','/') for fn in glob.glob(glob_pattern))  # glob with windows compatibility
	fns = [fn for fn in fns if re_date.search(fn)]  # only keep dates that really match the pattern

	# delete obsolete cache entries
	while fns and file_age(fns[0]) > MAX_CACHE_AGE:
		os.remove(fns[0])
		del fns[0]
		print 'Deleted %s from cache' % fns[0]

	# get the requested file
	newer_files = [fn for fn in fns if fn > fn_template%{'time':after}]
	#print 'In cache (%s from %s): %s' % (glob_pattern, os.getcwd(), fns)
	#print 'User wants newer than', fn_template%{'time':after}
	#print 'New enough:', newer_files
	if not newer_files:
		return None
	return newer_files[0 if not reverse_order else -1]

def authenticate(user, password, acl_tag):
	"""
	Return True iff `user`:`password` is a valid combination and `user` has access to
	articles tagged with `acl_tag`.
	Configuration is read from `HTPASSWD_FILE`. If a line *immediately following* a user's
	line is of the form "#acl acltag1,acltag2,...,acltagN", user is granted access to these acl tags.
	"""
	# get the info about this user from htpasswd
	with open(HTPASSWD_FILE) as f: lines = f.readlines()
	user_lines = [
		(line.strip(), next_line.strip()) for (line, next_line) in zip(lines, lines[1:]+[''])
		if not line.strip().startswith('#') and line.lower().startswith((user or '').lower()+':')]
	
	# parse the info from the file
	if not user_lines: 
		print 'UNKNOWN USER: %r' % user
		return False
	line, next_line = user_lines[0]
	correct_password = line.split(':',1)[1]
	if next_line.startswith("#acl "): 
		allowed_acl_tags = [tag.strip() for tag in next_line[len("#acl"):].split(',')]
	else: 
		allowed_acl_tags = []  # no explicitly allowed ACL tags
		
	# check if permissions are OK. Every registered user has implicit access to the 'public' ACL tag
	return password==correct_password and acl_tag in allowed_acl_tags+['public']

@bottle.route('/')
@bottle.route('/:acl_tag')
@bottle.route('/:acl_tag/')
def http_serve_stream(acl_tag='public'):
	"""
	Return, as an HTTP binary file, the oldest(!) file for the given ACL tag (e.g. "public").
	Takes an optional GET parameter "after"; only files created after this timestamp are considered.
	"""
	# Authorization.
	try:
		username = password = None
		assert bottle.request.auth is not None, "No 'Authorization' HTTP header or 'HTTP_AUTHORIZATION' environment variable given."
		username, password = bottle.request.auth
		assert authenticate(username, password, acl_tag)
	except Exception, e:
		print 'DENIED REQUEST: http authorization token %r (user %r, password %r) requested acl_tag %r. Traceback follows.' % (
			bottle.request.environ.get('HTTP_AUTHORIZATION'), username, password, acl_tag)
		traceback.print_exc()
		return bottle.HTTPResponse("You don't have the permission to access this stream", status=401)
	else:
		print 'GRANTED: user %r, acl_tag %r' % (username, acl_tag)
	
	after = bottle.request.GET.get('after','0000-00-00T00-00-00Z')
	fn_template = FILENAME_TEMPLATE.replace('%(acl_tag)s',acl_tag)  # hack: partial string interpolation
	if not fn_template:
		return bottle.HTTPResponse(
			"<h1>404</h1>Unknown stream: %s. Check http://newsfeed.ijs.si/ for possible URLs.",
			status=404)
	
	path = get_cached_file(fn_template=fn_template+'.gz', after=after, reverse_order=(after==None))
	if path is None:
		return bottle.HTTPResponse("<h1>404</h1>No gzips created after %s on stream %r yet." % (util.xmlEscape(after), acl_tag), status=404)
	else:
		dir, fn = os.path.split(path)
		return bottle.static_file(fn, root=dir, download=acl_tag+'-'+fn, mimetype='application/x-gzip')


if __name__=='__main__':
	zmqctx = zmq.Context()
	# (the socket is created in its own thread; context should be created in the main thread)

	# Debug only: uncomment either of the two below for a single-threaded run
	#zmq_to_files(); 1/0
	#bottle.debug(True); bottle.run(host='0.0.0.0', port=13380); 1/0
	
	# zmq subscriber
	threading.Thread(target=zmq_to_files).start()

	# http server
	bottle.debug(True)
	threading.Thread(target=bottle.run, kwargs={'host':'0.0.0.0', 'port':13380}).start()
