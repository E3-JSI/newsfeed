#!/usr/bin/python

#
#	!!! TODO: ce je final_url hit, oznac feed_article za removal pa tisina.
#



#
# sucker za downloadanje articlov
# logika je simpl:
#  - najd request
#  - poskus downloadat [pazi: referer, sledenje redirectom]
#  - ce ne rata logiri in pomoznosti reenqueuei al pa oznac za problematicnga
#  - ce rata, posti article in odstran request iz urlpoola
#

#
# implementacija:
#  - glavn thread nabira requeste in jih fila v queue
#  - worker threadi dequeuajo po en request in ga sprocesirajo
#
# glavn thread more vedt kere requeste je ze obdelu, zato si jih na zacetku vse odklene, pol pa postopoma zaklepa; da ne enqueuea enga veckrat ..
# besides: post articla bi meu lahko check ce je vec k en article na feed_article - skode pa ni velke razn loada ...
#

#
# annoyance: Queue ne vzame Eventa, tko da bo wait na queue vsake par sekund timeoutu
#

#
# problematicn: ko da feed nove clanke, selectam enga, pol pa takoj poskusm naslednga - da bi zapovnu Q - pa ne gre, zato spim 60s.
# annoying.....
#
# !!! to se da popravt (a je res?) tko da ce ne dobis nazaj tok vrstic k si jih zahtevu, pocakas 20 sekund namest da gres takoj spet najedat
# hopefully bo v tem casu ze zdownloadu, je pa 3x mn k TIMEOUT...
#

#
# gls verzija: 
#  main thread (try_enqueue funkcija) selecta VSE
#    enqueued feed_article z
#	id > last_selected_id (da preskocmo stvari k jih ze mamo)
# 	kjer site ni disabled IN ni locked					(locked naceloma ne bo noben, admin disablani pa so lahko)
#	in feed_article ni locked IN je enqueued IN next_attempt je > now()	(enq morjo bit, locked naceloma ne bodo, next_att je pa sock timeout handling ipd)
#    (selecta se tud feed url, ker se nastav za Referer HTTP field)
#  in jih submita suckerjem.
#
# suckerji : run:process_request:do_request:DB_post_article updatajo feed_article na locked=false, enqueued=false
#


from common import *
import threading, Queue, socket
import urllib2
import cookielib
from traceback import *
import base64
import time
import gc
import sys
import zmq
import StringIO, gzip
import random
import datetime
import heapq
import collections
import pdb

SOCKET_TIMEOUT=30
DATABASE_TIMEOUT=5
DATABASE_SHORT_TIMEOUT=5
N_THREADS=37
THREAD_Q_TIMEOUT=60 # was 5
QUERY_MULT=113
MAX_DB_FETCH_LATENCY=2*60
MAX_DB_FULL_FETCH=6*3600
CLEANER_Q_SIZE=1000
RATELIMIT_TIMEOUT=4.0

def partition_work_by_site(L):
	W = collections.defaultdict(dict)
	for e in L: W[e['siteid']][e['id']] = e
	return W

def exc_str(exc):
	try:
		return unicode(exc).encode('utf8','replace')
	except:
		try:
			t = repr(exc)
		except:
			try: exc_type = repr(type(exc))
			except: exc_type = '(unknown type)'
			t = '<exception[expr_fallback]:%r:%r>' % (exc_type, getattr(exc,'args','(no args)'))
		if type(t)==unicode: t = t.encode('utf8','replace')
		return t

class RLQueue(object):
	# v queueju (heap) so tupli (next_suck, siteid, dict(fa.id:request, ...))
	# s setom dobimo avtomaticno deduplikacijo (otoh vrstn red ni po fa.id ASC)
	def __init__(self):
		self.Q = []
	def __len__(self):
		return len(self.Q)
	def extend(self, L):
		W = partition_work_by_site(L)
		when = time.time() + RATELIMIT_TIMEOUT
		if len(self.Q) == 0:
			self.Q = [(when, siteid, W[siteid]) for siteid in W]
			heapq.heapify(self.Q)
		else:
			active_sites = {x[1]:x[2] for x in self.Q}
			for siteid in W:
				if siteid in active_sites:
					active_sites[siteid].update(W[siteid])
				else:
					heapq.heappush(self.Q, (when, siteid, W[siteid]))
	
	def pop(self):
		if len(self.Q) == 0:
			pdb.set_trace()
			raise IndexError
		when, siteid, worklist = self.Q[0]
		if when > time.time(): time.sleep(max(0,when - time.time()))
		
		work = worklist.popitem()
		if len(worklist) == 0: heapq.heappop(self.Q)
		else: heapq.heapreplace(self.Q, (time.time() + RATELIMIT_TIMEOUT, siteid, worklist))
		
		return work[1]	# discard "art_id:" part

class sucker(threading.Thread):
	numthreads = 0
	
	def __init__(self, worklist, killcmd, db, R):
		#self.db = db
		# pyPgSQL vsaj ne trd da je threadsafety=2...
		self.db = DB_connect('article sucker: suck')
		self.worklist = worklist
		self.retQ = R
		self.killcmd = killcmd
		self.thread_id = sucker.numthreads
		sucker.numthreads += 1
		socket.setdefaulttimeout(SOCKET_TIMEOUT)
		threading.Thread.__init__(self)
		self.setDaemon(True)
	
	def run(self):
		print "thread %d ready to work" % self.thread_id
		while not self.killcmd.isSet():
			self.process_request()
	
	def process_request(self):
		work = None
		try:
			work = self.worklist.get(block=True, timeout=THREAD_Q_TIMEOUT)
		except (Queue.Empty):
			print "> [%d]   no work received in %d seconds" % (self.thread_id, THREAD_Q_TIMEOUT)
			return
		else:
			print ("working on request %s" % work[1]).encode('ascii', 'replace')
			self.do_request(work)
			if random.random() < 0.01:
				print "> [%d] GC" % (self.thread_id)
				gc.collect()

	def do_request(self, work):
		(ref_url, rq_url, art_id, art_feedid, art_feedsiteid, art_siteid, max_fau_seq) = work

		blacklisted_extensions = ['asx', 'dts', 'gxf', 'm2v', 'm3u', 'm4v', 'mpeg1', 'mpeg2', 'mts', 'mxf', 'ogm', 'pls', 'bup', 'a52', 'aac', 'b4s', 'cue', 'divx', 'dv', 'flv', 'm1v', 'm2ts', 'mkv', 'mov', 'mpeg4', 'oma', 'spx', 'ts', 'vlc', 'm4v', 'mp4', 'mp3', 'zip']

		rq_url_lc = rq_url.lower()
		if any(rq_url_lc.endswith(x) for x in blacklisted_extensions):
			# let's not be even remotely interested in this url.
			print "[%d] > ignoring %s" % (self.thread_id, rq_url)
			DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, None, max_fau_seq,903, '', '')
			DB_log(self.db, art_siteid, rq_url, 903, -1)
			print "[%d] > unlocking %d" % (self.thread_id, art_siteid)
			DB_site_access_unlock(self.db, art_siteid)
			return
			
		try:
			print "> open"
			rq = urllib2.Request(url=rq_url)
			rq.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US; rv:1.8.0.6) Gecko/20060728 Firefox/1.5.0.6')
			rq.add_header('Accept', 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,*/*;q=0.5')
			rq.add_header('Accept-Language', 'en-us,en;q=0.5')
			rq.add_header('Accept-Charset', 'ISO-8859-1,utf-8;q=0.7,*;q=0.7')
			if type(ref_url)==unicode: ref_url = ref_url.encode('utf8','replace')
			rq.add_header('Referer', ref_url)
			http_opener = urllib2.build_opener(urllib2.HTTPRedirectHandler)
			print "[%d] > request set up" % self.thread_id
			hnd = http_opener.open(rq)
			print "[%d] > opened" % self.thread_id
			# sync empty cookie jar
		except urllib2.HTTPError, exc:
			print "%d > excpt: httperror" % self.thread_id
			#err_page = exc.read()
			DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, None, max_fau_seq,918, '', exc_str(exc))#base64.b64encode(err_page))
			DB_log(self.db, art_siteid, rq_url, 918, exc.code)
			print "[%d] [%d] %s" % (self.thread_id, exc.code, rq_url)
		except urllib2.URLError, e:
			print "%d > excpt: urlerror" % self.thread_id
			DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, None, max_fau_seq,901, '', exc_str(e))
			DB_log(self.db, art_siteid, rq_url, 901, -1)
			print "[%d] %s" % (self.thread_id, rq_url)
		except socket.timeout, e:
			DB_retry_article(self.db, art_id)
			print "[%d] timeout; url set for retry" % self.thread_id
			print format_exc()
		except (socket.gaierror, socket.herror, socket.error), e:
			DB_retry_article(self.db, art_id)
			print "[%d] socket error?" % self.thread_id
			print format_exc()
		except Exception, e:
			# ne mormo vsega pohandlat ....
			print "[%d] > excpt" % self.thread_id
			DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, None, max_fau_seq, 900, '', exc_str(e))
			DB_log(self.db, art_siteid, rq_url, 900, -1)
			print ("[%d] %s" % (self.thread_id, rq_url)).encode('ascii', 'replace')
		else:
			print "[%d] > ok" % self.thread_id
			
			try:
				page = hnd.read()
			except:
				DB_retry_article(self.db, art_id)
				print "[%d] caught exc, url set for retry ----" % self.thread_id
			else:
				code = hnd.code
				headers = str(hnd.headers)
				final_url = hnd.url
				size = len(page)
				
				if size > 2000000:
					print "[%d] downloaded a suspiciously large file [aid = %d, len = %d]; discarding." % (self.thread_id, art_id, size)
					DB_log(self.db, art_siteid, rq_url, 904, -size)
					DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, final_url, max_fau_seq, 904, headers, '')
					return
					
				if code == None:
					print "[%d] someone is screwing with us. discard. [aid = %d, len = %d]; discarding." % (self.thread_id, art_id, size)
					DB_log(self.db, art_siteid, rq_url, 905, -size)
					DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, final_url, max_fau_seq, 905, headers, '')
					return

				if hnd.headers.get('content-encoding') == 'gzip':
					print "[%d] decompressing..." % self.thread_id
					try:
						contentIO = StringIO.StringIO(page)
						gzipFile = gzip.GzipFile(fileobj=contentIO)
						page = gzipFile.read()
					except Exception, e:
						DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, None, max_fau_seq, 917, headers, exc_str(e))
						DB_log(self.db, art_siteid, rq_url, 917, -1)
						print "decompression failed."
						DB_site_access_unlock(self.db, art_siteid)
						return
						
				
				print "%d > post" % self.thread_id
				
				#
				# catch: page encoding is not known, so we need to treat it like a bytestream until it gets parsed by BeautifulSoup
				#        but database expects an utf8 string. utf8 knows invalid byte sequences
				#        (alternative: SQL_ASCII: noninterpreted bytestream; don't want to insert that into db.connection)
				#        therefore, all pages (including already-utf8...) need to be encoded (...again)
				
				# log pred post - ce slucajno poginemo zarad ctl-c -> daemon, hocmo vsaj log, ne clanka...
				
				DB_log(self.db, art_siteid, rq_url, code, size)
				DB_post_article(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, rq_url, final_url, max_fau_seq, code, headers, page)
				self.retQ.put((art_id, headers, page), block=True)	# prevent potential infinite retQ growth if dead cleaners
				print "[%d] > log" % self.thread_id
				print "[%d] [%d] %s" % (self.thread_id, code, rq_url)
				
			#	print "[%d] cleanup" % (self.thread_id)
			#	decoded = decoder.decodeText(page, hnd.headers)
			#	cleaned = cleaner.parseCleartext(decoded, final_url)
			#	DB_post_cleaned(self.db, art_id, art_feedid, art_feedsiteid, art_siteid, cleaned)
			#	print "[%d] done cleaning" % (self.thread_id)
				
		print "[%d] > unlocking %d" % (self.thread_id, art_siteid)
		DB_site_access_unlock(self.db, art_siteid)

def cleaner_sink(Q):
	db = DB_connect('article sucker: sink')
	
	zmqctx = zmq.Context()
	sock = zmqctx.socket(zmq.PUSH)
	sock.setsockopt(zmq.HWM, 100)
	sock.bind('tcp://*:1234')

	while True:
		id, headers, page = Q.get()
		print "sending", id, "to the cleaners"
		try:
			sock.send_multipart([str(id),headers,page])#, flags=zmq.NOBLOCK)
		except:
			print "%% send_multipart failed."
			DB_unlock_cleaning(db, id)

FIND_ALL_REQS = """
	SELECT 
		f.URL AS ref_url, argmin(fau.seq, fau.url) as URL, a.id, a.feedid, a.feedsiteid, a.siteid, max(fau.seq) as max_fau_seq
		FROM feed_article AS a
		INNER JOIN feed AS f ON a.feedid = f.id
		INNER JOIN site ON a.siteid = site.id
		INNER JOIN feed_article_urls AS fau ON a.id = fau.fa_id
	WHERE
		NOT site.disabled
		AND NOT site.locked
		AND a.enqueued
		AND a.next_attempt < NOW()
		AND a.id > %s
	GROUP BY
		ref_url, a.id, a.feedid, a.feedsiteid, a.siteid
"""
#"""
#	SELECT 
#		f.URL AS ref_url, a.URL, a.id, a.feedid, a.feedsiteid, a.siteid
#		FROM feed_article AS a
#		INNER JOIN feed AS f ON a.feedid = f.id
#		INNER JOIN site ON a.siteid = site.id
#	WHERE
#		NOT site.disabled
#		AND NOT site.locked
#		AND a.enqueued
#		AND a.next_attempt < NOW()
#		AND a.id > %s
#"""


last_full_select = time.time()
last_fetch_ts = time.time()
last_req_id = -1
	
def try_enqueue(db, T, Q):
	"""
	called once for one enqueued feed_article to be transfered from private worklist (T) to sucker queue (Q)
	 if T is empty or last_fetch_ts+MAX_DB_FETCH_LATENCY < now(), fetch everything new (i.e. with id > last_req_id)
	 from the database and add it to T. if T is still empty, sleep for a while
	 sleep timeout depends on Q being empty (or not.)
	only enqueue articles on sites the were idle for 3s+.
	
	!bn: BUG: if article is rescheduled for download, it will return to the db and stay there until
	          crawler is restarted .. because of last_req_id. fix later. much later.
	"""
	global last_full_select, last_fetch_ts, last_req_id
	
 	print "O_o ... try to add one feed_article to sucker queue (T.len=%d, Q.len=%d)" % (len(T), Q.qsize())
	
	if len(T) == 0 or (last_fetch_ts + MAX_DB_FETCH_LATENCY) < time.time():
		print "     .. worklist is empty, nagging the database."
		cur = db.cursor()
		
		fetch_min_req_id = last_req_id
		if (last_full_select + MAX_DB_FULL_FETCH) < time.time():
			fetch_min_req_id = -1
			last_full_select = time.time()
		
		cur.execute(FIND_ALL_REQS, (fetch_min_req_id,))
		R = cur.fetchall()
		db.commit()
		T.extend(R)
		last_req_id = max(last_req_id, max(x['id'] for x in R) if len(R) > 0 else -1)
		last_fetch_ts = time.time()
		print "      .. after db fetch .. T.len = %d" % len(T)
	
	if len(T) == 0:
		"     .. still no work in db. sleep for a while. ZZZZzzz."
		if Q.qsize() > 0:			# neki je se v queueju, selectat pa ne mormo nic - mogoce so zaklenjeni sajti ?
			time.sleep(DATABASE_SHORT_TIMEOUT)
		else:
			time.sleep(DATABASE_TIMEOUT)	# there is obviously nothing in the database, wait a bit and return
		return
	
	print "O_O really try to enqueue work (T.len=%d, Q.len=%d prior to Q.put(T.pop()))" % (len(T), Q.qsize())
	rq = T.pop()					# at least try to avoid hitting the same site multiple sequentially
	Q.put(rq, block=True)

def main():
	threads = []
	Q = Queue.Queue(QUERY_MULT*N_THREADS)	# fetcher->sucker(s)
	R = Queue.Queue(CLEANER_Q_SIZE)		# sucker(s)->cleaner_sink

	socket.setdefaulttimeout(SOCKET_TIMEOUT)
	db = DB_connect('article sucker: discovery')
	
	if not(len(sys.argv) > 1 and sys.argv[1] == "skip-unlock"):
		print "Unlocking sites & feedarticles"
		DB_unlock_sites(db)
		DB_unlock_feedarticles(db)
	
	sink = threading.Thread(target=cleaner_sink, args=(R,))
	sink.daemon = True
	sink.start()
	
	for i in range(N_THREADS):
		evt = threading.Event()
		thr = sucker(Q, evt, db, R)
		threads.append((thr,evt))
		thr.start()
	try:
		T = RLQueue()		# private request queue
		while True:
			try_enqueue(db, T, Q)
	except (KeyboardInterrupt):
		# merge down
		for thr,evt in threads:
			evt.set()
		for thr,evt in threads:
			thr.join()
	except:
		for thr,evt in threads:
			evt.set()
		for thr,evt in threads:
			thr.join()
		print format_exc()
	
if __name__ == '__main__':
	main()
