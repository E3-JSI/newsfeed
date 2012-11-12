#!/usr/bin/python

"""
Performs various postprocessing on the articles from the 'news' database.
Usage: cleanDb.py <action> [options]

Possible values for <action> are:
  decode -- converts base64-encoded articles to unicode, in-place
  cleartext -- fills the processed_article table with cleartext articles
  langdet -- determines the language of cleartext articles

Possible options:
  -n<NUM> - number of threads. Default: 1.
  -v - very verbose. Show all debug output.
      
For details, see source code.
""" 

import sys, os; sys.path.append('.')
import psycopg2, psycopg2.extras
import traceback
import time, random
from multiprocessing import Process
import util
from article_extractor import dechrome
from langdet import langdet, iso_map
import cld
reload(util); reload(dechrome)

import logging
from logging import debug, info, warn, error, exception, critical
logger = logging.getLogger()
if __name__=='__main__':
	logger.name = 'cleanDb'
	logger.setLevel(logging.INFO)  # Options: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50
	formatter = logging.Formatter(fmt='[pid%(process)d-%(threadName)s] %(levelname)8s %(asctime)s %(message)s')
	for handler in [
		logging.StreamHandler(sys.stderr),
		logging.FileHandler(filename='log/'+logger.name+'.log', encoding='utf8')
	]:
		handler.setFormatter(formatter)
		logger.addHandler(handler)


def openConnection(appname=None):
	"""
	Creates a new(!) connection to the database and returns a tuple (connection, cursor for this connection).
	"""
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
	import socket; ip = socket.gethostbyname(socket.getfqdn())
	if ip.startswith('212.235.215.') or ip.startswith('95.87.154.'):
		# We're on a IJS subnet
		conn = psycopg2.connect(database='news', host='maximus', user='mitjat', password='XXX_GITHUB_XXX')
	else:
		conn = psycopg2.connect(database='news', host='localhost', port=15432, user='mitjat', password='XXX_GITHUB_XXX')
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute("SET bytea_output TO 'escape'")
	if appname: cur.execute("SET application_name TO %s", (appname,))
	conn.commit()
	return (conn,cur)

class ProcessingError(Exception):
	"""
	Something went wrong when processing a single article.
	Nothing fatal, but further steps in the pipeline make no sense for this article.
	"""
	pass
class NoTextFoundError(ProcessingError):
	"""
	Raise during cleartexting (see handleSingleCleartext()) if no convincing
	text is found within HTML.
	"""

def handleSingleDecode(cur, articleId, headers, txt):
	"""
	Normalize a single article into utf8. Write the results in the DB but also return them.
	`headers` are raw HTTP headers (string).
	`txt` is a byte string -- the original article content.
	Returns the reencoded article (a byte string). Throws ProcessingError if something went wrong.
	"""
	headers = util.parseHttpHeaders(headers)
	mimeType = None if 'content-type' not in headers else headers['content-type'].split(';')[0][:20]
	conn = cur.connection

	# decode base64 and unknown charset, store the result
	debug('decoding %s ... ' % articleId)
	import binascii
	try:
		if len(txt)>10000000:
			raise Exception, 'Text too long: length=%s' % len(txt)
		txt_utf8 = str(txt)
		txt_utf8 = util.decodeText(txt_utf8, headers)
		txt_utf8 = buffer(txt_utf8.encode('utf8'))
		cur.execute("UPDATE article SET content=%s, mimetype=%s WHERE feed_articleid=%s", (txt_utf8, mimeType, articleId))
		cur.execute("UPDATE feed_article_meta SET is_utf8='1' WHERE id=%s", (articleId,))
		if cur.rowcount==0:
			cur.execute("INSERT INTO feed_article_meta (id, is_utf8) values (%s, '1')", (articleId,))  # defaults are ok for the other columns
			warn('Article %s was just decoded to utf-8 but did not have a feed_article_meta row; created.', articleId)
	except psycopg2.Error:
		raise
	except util.MimeTypeError:
		cur.execute("UPDATE article SET content=NULL, mimetype=%s WHERE feed_articleid=%s", (mimeType, articleId))
		cur.execute("UPDATE feed_article_meta SET is_nontext='1', is_utf8='e' WHERE id=%s", (articleId,))
		if cur.rowcount==0:
			cur.execute("INSERT INTO feed_article_meta (id, is_nontext, is_utf8) values (%s, '1', 'e')", (articleId,))  # defaults are ok for the other columns
			warn('Article %s was just decoded to utf-8 but did not have a feed_article_meta row; created.', articleId)
		info('Article %s did not have a text/* MIME type' % articleId)
		raise ProcessingError('Article did not have a text/* MIME type')
	except:
		cur.execute("UPDATE feed_article_meta SET is_utf8='e' WHERE id=%s", (articleId,))
		if cur.rowcount==0:
			cur.execute("INSERT INTO feed_article_meta (id, is_utf8) values (%s, 'e')", (articleId,))  # defaults are ok for the other columns
			warn('Article %s was just decoded to utf-8 but did not have a feed_article_meta row; created.', articleId)
		exception('Error decoding article %s' % articleId)
		raise ProcessingError
	else:
		debug('article decoded')

	conn.commit()
	return txt_utf8
	

def runDecode():
	"""
	For every already-downloaded article (enqueued=false in feed_article table) that is not
	yet decoded (is_utf8='0' in meta), re-encodes article's contents into utf8.
	Runs indefinitely, does not return.
	"""

	conn, cur = openConnection()
	conn.set_isolation_level(1) # use transactions
	
	while True:
		# Get an uncoverted row
		info('selecting and locking ...')
		cur.execute("SELECT id FROM feed_article_meta m JOIN feed_article fa USING (id) WHERE NOT fa.enqueued AND m.is_utf8='0' LIMIT 200")
		articleIds = [row['id'] for row in cur]
		conn.commit()
		if len(articleIds) == 0:
			info('nothing to do, trying again')
			time.sleep(15)
			continue
		
		cur.execute("SELECT * FROM article WHERE feed_articleid IN (%s)" % ','.join(map(str,articleIds)))
		articleRows = cur.fetchall()
		# DB dirt: it's possible that an article is marked as downloaded, but is not really available in the article table. Mark those up.
		missingIds = set(articleIds) - set(row['feed_articleid'] for row in articleRows)
		if missingIds:
			cur.execute("UPDATE feed_article_meta SET is_utf8='m' WHERE id IN (%s)" % ','.join(map(str, missingIds)))
			warn("Marked as is_utf8='m': these articles were not available in the article table: " + ','.join(map(str, missingIds)))
		
		info('got %d IDs and %d actual articles' % (len(articleIds), len(articleRows)))
		with open('log/articles_decoded.log','a') as f: f.write(','.join(map(str,articleIds))+',\n')

		for articleRow in articleRows:
			try:
				handleSingleDecode(
					cur=cur,
					articleId=articleRow['feed_articleid'],
					headers=articleRow['header'],
					txt=articleRow['content'])
			except ProcessingError:
				pass
	conn.close()


def handleSingleCleartext(cur, articleId, html, commit=True):
	"""
	Extracts article body from a single article's html. Writes the result in the DB plus returns it.
	`html` is a utf8-encoded byte string.

	Returns a unicode string -- the article body. The first line of the body contains the title
	(can be an empty string; unrelated to the one in feed_article_meta obtained from RSS).

	Raises ProcessingError or its subclass NoTextFoundError if something goes wrong or if
	the html contains no convincing body.
	If `commit` is given, commits the transaction at the end.
	"""
	debug('processing %s ...' % articleId)
	conn = cur.connection
	try:
		cleartext = dechrome.parseCleartext(html)
		title = cleartext.split('\n')[0]
	except Exception, e:
		raise ProcessingError(e)

	# Debug: print output to a file for later inspection
	if 0:
		try:
			cur.execute("SELECT url FROM feed_article WHERE id=%s", (articleId,)); url=cur.fetchone()['url']
			debug('Article %s has url %s', articleId, url)
			if cleartext.startswith('\n'): cleartext = '???'+cleartext
			html_out = ('%s: <a href="%s">%s</a>' % (articleId, url, url)) + ('<pre><span style="font-weight:bold; font-size:20px">'+util.xmlEscape(cleartext)+'</pre>').replace('\n','</span>\n',1)
			with open('clean_output.html','a') as f: f.write(html_out.encode('utf8')); f.flush()
		except:
			exception("Error while producing debug output")

	if not cleartext:
		cur.execute("DELETE FROM processed_article WHERE feed_articleid=%s", (articleId,))
		cur.execute("UPDATE feed_article_meta SET is_cleartext='e' WHERE id=%s", (articleId,))
		raise NoTextFoundError
	else:
		try:
			cur.execute("UPDATE processed_article SET content=%s WHERE feed_articleid=%s AND mode='cleartext'", (cleartext, articleId,))
			if cur.rowcount == 0:
				cur.execute("INSERT INTO processed_article (content, feed_articleid, mode) VALUES (%s, %s, 'cleartext')", (cleartext, articleId,))
			cur.execute("UPDATE feed_article_meta SET is_cleartext='1', title=COALESCE(title,%s) WHERE id=%s", (title,articleId,))
		except psycopg2.DataError, e:
			exception("Couldn't insert article %d in the DB (often an encoding issue)" % articleId)
			conn.rollback()
			cur.execute("DELETE FROM processed_article WHERE feed_articleid=%s", (articleId,))
			cur.execute("UPDATE feed_article_meta SET is_cleartext='e' WHERE id=%s", (articleId,))
			raise ProcessingError(e)

	if commit: conn.commit()
	debug('Article %s converted to cleartext: kept %d bytes out of %d.', articleId, len(cleartext or ''), len(html or ''))
	return cleartext
	

def runCleartext():
	"""
	For every row in the 'article_feed_meta' table with is_utf8=true (implying 
	utf8-encoded row in the article table) but is_cleartext=false, extracts the clear text version of
	article's contents and inserts it into the processed_article table.
	processing_stage's 1 bit is set to 1.
	Suitable for multithreaded/multiprocess execution.
	Runs indefinitely, does not return.
	"""
	
	conn, cur = openConnection()
	conn.set_isolation_level(1) # use transactions

	while True:
		info('selecting ...')
		# Fetch some non-cleartexted IDs
		cur.execute("SELECT id, COALESCE(title,'') AS title FROM feed_article_meta WHERE is_cleartext='0' AND is_utf8='1' LIMIT 1000 FOR UPDATE") 
		if cur.rowcount == 0:
			info('nothing to do, sleeping ...')
			time.sleep(15)
			continue

		# Lock the rows, store the titles for later
		info('got %d meta rows' % cur.rowcount)
		articleTitles = dict((row['id'],row['title']) for row in cur)
		articleIds = articleTitles.keys()
		with open('log/articles_cleartexted.log','a') as f: f.write(','.join(map(str,articleIds))+',\n')

		# Get the htmls
		cur.execute("SELECT feed_articleid, content FROM article WHERE feed_articleid IN (%s)" % ','.join(map(str, articleIds)))
		info('got %d data rows' % cur.rowcount)
		articleRows = cur.fetchall()

		# Wart: there are articles that need to be cleartexted but are not in the 'article' table. Mark them up, unlock them.
		missingIds = set(articleIds) - set(row['feed_articleid'] for row in articleRows)
		if missingIds:
			cur.execute("UPDATE feed_article_meta SET is_cleartext='m' WHERE id IN (%s)" % ','.join(map(str, missingIds)))
			warn("Marked as is_cleartext='m': these articles were not available in the article table: " + ','.join(map(str, missingIds)))

		# Do the conversion
		for articleRow in articleRows:
			try:
				handleSingleCleartext(
					cur=cur,
					articleId=articleRow['feed_articleid'],
					html=str(articleRow['content']),  # convert buffer to str
					title=articleTitles[articleRow['feed_articleid']])
			except ProcessingError:
				pass


_CLD_LANGS = [
	iso_map.iso2to3.get(iso2,iso2) for iso2 in
	[dict(cld.LANGUAGES)[x] for x in cld.DETECTED_LANGUAGES]]
def handleSingleLangdet(cur, D, articleId, text, ignore_cld_langs=True, commit=True):
	"""
	Detect the language of a single article. Write it in the DB, plus return it.
	`cur` is an output cursor.
	`D` is the langdet database (see langdet.load_langdet_db())
	`text` is a unicode string -- the cleartext version of the article.

	Returns a pair (iso language code, alternate language code) or (None,None) in
	case of no convincing results.
	Raises ProcessingError if something goes wrong.

	If `commit` is given, commits the transaction at the end.
	If `ignore_cld_langs` is given, return (None,None) if one of CLD-supported
	languages is detected.
	"""
	lc_iso, lc_alt, tgm_count = langdet.langdet_s(text, D[:-2], *D[-2:])
	debug('Article %d has language (%s / %s)', articleId, lc_iso, lc_alt)

	if ignore_cld_langs and lc_iso in _CLD_LANGS:
		return (None, None)
	
	try:
		cur.execute("UPDATE feed_article_meta SET lang_iso=%s, lang_altcode=%s, lang_is_cld=%s WHERE id=%s", (lc_iso, lc_alt, False, articleId))
		if commit: cur.connection.commit()
		return (lc_iso, lc_alt)
	except ProgrammingError, e:
		print "error updating fam.language. retired?"
		cur.connection.rollback()
		return (None, None)

def handleSingleLangdet_cld(cur, articleId, text, commit=True):
	"""
	Like handleSingleLangdet(), but using Google's CLD library.
	If `commit` is given, commits the transaction at the end.
	"""
	try: lc_alt = cld.detect(text.encode('utf8','replace'))[1]
	except:
		print repr(text)
		raise
	if lc_alt == 'un' or lc_alt == 'xxx':  # "un" means "unknown"
		lc_alt = lc_iso = None
	else:
		lc_iso = iso_map.iso2to3[lc_alt.split('-')[0]]
	debug('Article %d has language (%s / %s)', articleId, lc_iso, lc_alt)
			
	cur.execute("UPDATE feed_article_meta SET lang_iso=%s, lang_altcode=%s, lang_is_cld=%s WHERE id=%s", (lc_iso, lc_alt, True, articleId))
	if commit: cur.connection.commit()
	return (lc_iso, lc_alt)
			

def runLangdet():
	# !bn: todo: langid dict samo enkrat nalozit za vse threade!
	conn, cur = openConnection()	# input cursor
	conn2, cur2 = openConnection()	# update/commit
	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
	
	info('loading langdet_nb.pck ...')
	D = langdet.load_langdet_db('langdet/langid_nb.pck')
	info('... done')
	
	while True:
		info('creating cursor ...')
		cur.execute("DECLARE foo NO SCROLL CURSOR FOR SELECT m.id, p.content FROM processed_article p INNER JOIN feed_article_meta m ON p.feed_articleid = m.id WHERE m.lang_iso IS NULL AND m.is_cleartext = '1' LIMIT 100000")
		info('... done')
		
		c = 0
		while True:
			cur.execute("FETCH foo")
			if cur.rowcount == 0: break
			
			id, text = cur.fetchone()
			id = int(id)

			try: handleSingleLangdet(cur=cur2, D=D, articleId=id, text=text)
			except ProcessingError: pass
			
			c += 1
			if (c % 100 == 0) and not (c == 0):
				info('processed 100 articles')
		
		info('processed %d available articles, sleeping.', c)
		
		cur.execute("CLOSE foo")
		conn.commit()
		
		time.sleep(120)


if __name__=='__main__':
	try:
		if os.name=='nt':
			import win32process
			win32process.SetPriorityClass(win32process.GetCurrentProcess(),win32process.IDLE_PRIORITY_CLASS)
		else:
			os.nice(10)
		info('NOTE: Process priority set to low.')
	except:
		exception('WARNING: Failed to set the priority of this process to low. Continuing with default priority. Reason:')

	if '-v' in sys.argv:
		logger.setLevel(logging.DEBUG)
		
	try:
		task = sys.argv[1]
	except IndexError:
		task = None
	if sys.argv==['-c']: 	# indicative of process being run in emacs interactive shell
		task = ''

	try:
		task2func = {
			'decode': runDecode,
			'cleartext': runCleartext,
			'langdet': runLangdet
		}
		try:
			func = task2func[task]
		except:
			critical('INVALID/UNKNOWN ACTION: %s\n%s', task, __doc__)
			sys.exit(1)  

		nThreads = [arg[2:] for arg in sys.argv if arg.startswith('-n') and arg[2:].isdigit()]
		nThreads = int(nThreads[0]) if nThreads else 1
		warn('Using %d threads', nThreads)
			
		if nThreads==1:
			# single-threaded
			func()
		else:
			for i in range(nThreads):
				Process(name=str(i), target=func).start()
			# got to keep the main thread alive
			while True:
				time.sleep(1)
			
	except SystemExit:
		pass
	except:
		exception('Unexpected error')
		
