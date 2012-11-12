import os, sys
import re, htmlentitydefs, string
import urllib2
import time, datetime
import random
import gzip, StringIO
import inspect, traceback

import socket
socket.setdefaulttimeout(5) #in seconds


def htmlUnescape(text):
	"""
	Taken from http://effbot.org/zone/re-sub.htm
	Removes HTML or XML character references and entities from a text string.

	@param text The HTML (or XML) source text.
	@return The plain text, as a Unicode string, if necessary.
	"""
	def fixup(m):
		text = m.group(0)
		if text[:2] == "&#":
			# character reference
			try:
				if text[:3] == "&#x":
					return unichr(int(text[3:-1], 16))
				else:
					return unichr(int(text[2:-1]))
			except ValueError:
				pass
		else:
			# named entity
			try:
				text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
			except KeyError:
				pass
		return text # leave as is
	return re.sub("&#?\w+;", fixup, text)

def textifyHtml(html):
	"""
	Convert HTML `html` to text in a simple way: block-level elements
	get surrounded by whitespace, other whitespace disappears,
	all HTML tags are stripped.
	"""
	if html is None: return None  # just in case
	txt = html
	txt = re.sub('</?\s*(br|p|div|td|th|h\d)[^>]*>', '\n', txt)
	txt = re.sub("<.*?>", "", txt)
	txt = normalizePunctuation(txt, normalizeWhitespace=True)
	txt = '\n'.join(line.strip() for line in txt.splitlines())
	txt = re.sub(" +"," ", txt)
	txt = re.sub("\n+","\n", txt)
	return txt

def xmlEscape(txt, errors='ignore'):
	"""
	Replace "weird" chars with their XML entities.
	For characters not allowed by XML (e.g. chr(0), chr(7), ...), raise ValueError
	if `errors` is not set to "ignore"; silently skip otherwise.
	"""
	allowedChars = set(
		string.uppercase +
		string.lowercase +
		string.digits +
		'.,;:!?_-+/!@#$%*()=[]\\\'"| \t\n\r')
	knownMappings = {'&':'&amp;', '<':'&lt;', '>':'&gt;'}

	chars = list(txt)
	for (i,c) in enumerate(chars):
		if c not in allowedChars:
			cc = ord(c)
			if 0x20<cc<0xD7FF or cc in (0x9, 0xA, 0xD) or 0xE000<cc<0xFFFD or 0x10000<cc<0x10FFFF:
				chars[i] = knownMappings.get(c, '&#%d;' % ord(c))
			else:
				if errors != 'ignore':
					raise ValueError(u"Character is not XML-encodable: %r" % c)
				else:
					chars[i] = '\x00'
	return ''.join(c for c in chars if c!='\x00')


_normalizedPunctuation = {
	0x00A0: u' ',    # non-breaking space
	0x2013: u'-',    # en dash
	0x2014: u' -- ', # em dash
	0x2015: u' -- ', # horizontal bar
	0x2212: u'-',    # minus sign
	0x2500: u'--',   # box drawing horizontal
	0x2501: u'|',    # box drawing vertical
	0x2215: u'/',    # division slash
	0x2044: u'/',    # fraction slash
	0x2018: u"'",    # quotation mark - single
	0x2019: u"'",    # quotation mark - single
	0x201A: u"'",    # quotation mark - single
	0x201B: u"'",    # quotation mark - single
	0x201C: u'"',    # quotation mark
	0x201D: u'"',    # quotation mark
	0x201E: u'"',    # quotation mark
	0x00BB: u'"',    # quotation mark >>
	0x00AB: u'"',    # quotation mark <<
	0x2039: u'"',    # quotation mark >
	0x203A: u'"',    # quotation mark <
	0x2022: u'*',    # bullet point
	0x2032: u"'",    # prime
	0x2033: u"''",   # double prime
	0x0060: u"'",    # inverted prime (`)
	0x02DD: u'"',    # double acute accent
	0x02DC: u'~',    # small tilde
	0x00A6: u'|',    # broken bar
	0x2026: u'...',  # ellipsis
	0x0133: u'ij',   # ligature
	0xFB00: u'ff',   # ligature
	0xFB01: u'fi',   # ligature
	0xFB02: u'fl',   # ligature
	0xFB03: u'ffi',  # ligature
	0xFB04: u'ffl',  # ligature
	0xFB06: u'st',   # ligature
	# The following codepoints are not defined in unicode. However, UnicodeDammit leaves them in the
	# text sometimes. Assume they come from Windows-1252, map accordingly.
	0x0091: u"'",    # quotation mark - single
	0x0092: u"'",    # quotation mark - single
	0x0082: u"'",    # quotation mark - single
	0x0084: u'"',    # quotation mark
	0x0093: u'"',    # quotation mark
	0x0094: u'"',    # quotation mark
	0x0095: u'*',    # bullet point
	0x0096: u'-',    # en dash
	0x0097: u' -- ', # em dash
	0x0085: u'...',  # ellipsis
	}
_normalizedWhitespace = {
	0x000A: u' ',    # \n
	0x000D: u' ',    # \r
	0x0009: u' ',    # \t
	}
def normalizePunctuation(txt, normalizeWhitespace=False):
	"""
	Maps "exotic" unicode codepoints into their ASCII couterparts. For example,
	em and en dash get mapped to a simple dash, smart quotes to '"', ellipsis
	gets expanded etc. See source for details.

	If normalizeWhitespace is given, also maps all whitespace (incl newlines) to spaces.
	"""
	if normalizeWhitespace:
		mapping = _normalizedPunctuation.copy()
		mapping.update(_normalizedWhitespace)
	else:
		mapping = _normalizedPunctuation
	return unicode(txt).translate(mapping)


def iso_utc_time(t):
	"ISO string representing the UTC variant of a given datetime object."
	return datetime.datetime.utcfromtimestamp(time.mktime(t.timetuple())).isoformat()+'Z'


def unique(lst, sorted=False):
	"""
	Return an iterator over the input list; only the first instance of each
	multiple entry is returned. If sorted==True is given implying that the
	input sequence is already sorted, this only affects performance, not the semantics.
	"""
	if sorted:
		ilst = iter(lst)
		lastSeen = ilst.next()
		yield lastSeen
		for el in ilst: #remaining elements
			if el==lastSeen:
				continue
			lastSeen = el
			yield el
	else:
		seen = set()
		addToSeen = seen.add
		for el in lst:
			if not hasattr(el,'__hash__') or el.__hash__==None:
				seen = list(seen)
				addToSeen = seen.append			
			if el not in seen:
				addToSeen(el)
				yield el


def decodeText_simple(text, headers):
	"""
	Takes a HTTP response body (=text) and the corresponding headers (a dict or dict-like
	object; httplib.HTTPResponse will do);
	outputs the text as a unicode string. The encoding is guessed using a combination of
	HTTP headers and the META ta inside HTML. If no encoding can be inferred, latin1 is assumed.
	Characters that can't be decoded are left as-is.
	Throws ValueError if headers do not indicate a text/* mime-type.

	Does not use any extra libraries, unlike decodeText(), which is more accurate.
	"""
	contentType = headers.get('content-type','text/html; charset=latin1')
	if not contentType.startswith('text/'):
		raise ValueError, "Can only convert HTTP responses with mime type text/*; got '%s' instead" % contentType

	# try to find the encoding in a meta tag (the regexp below does not cover all instances, but it's close)
	m = re.search('''<meta                     \s+
		http-equiv \s* = \s* .?Content-Type.?   \s+
		content    \s* = \s* .?text/\w+;?  \s+  charset=(  [^"';> ]+  )
		''', text, re.IGNORECASE | re.VERBOSE)
	if not m:
		# no luck with META tags; try HTTP headers
		m = re.search('charset=([\w0-9\-]+)', contentType)

	if m:
		charset = m.group(1).replace('windows-','cp')
	else:
		charset='latin1'

	return text.decode(charset, 'ignore')


class MimeTypeError(ValueError):
	pass

def decodeText(txt, headers=None):
	"""
	Takes a HTTP response body (=text) and the corresponding HTTP headers (a dict or dict-like
	object; httplib.HTTPResponse will do; see parseHttpHeaders() if you have a string);
	outputs the text as a unicode string. The encoding is guessed using BeautifulSoup.UnicodeDammit
	(which in turn uses chardet if installed), enhanced by the HTTP-suggested encoding.

	Raises MimeTypeError (subclass of ValueError) if headers do not indicate a text/* mime-type.
	"""
	from BeautifulSoup import UnicodeDammit

	# guess the charset suggested by HTTP headers
	httpCharset = []
	if headers:
		contentType = headers.get('content-type','')

		if not contentType.startswith('text/'):
			raise MimeTypeError("Can only decode text documents (mime type text/*; got %s)" % contentType)
	
		m = re.search('charset=([\w0-9\-]+)', contentType)
		if m:
			httpCharset = [ m.group(1).replace('windows-','cp') ]

	ud = UnicodeDammit(txt, isHTML=True, overrideEncodings=httpCharset) # overrideEncodings is not enforced by UnicodeDammit, it's just tried
	return ud.unicode


def parseHttpHeaders(headersTxt):
	"""
	Takes HTTP headers and parses them into a dict. Keys and values are lowercased.
	"""
	res = {}
	for line in headersTxt.splitlines():
		if ':' not in line:
			continue
		key, val = line.split(':',1)
		key = key.strip().lower()
		val = val.strip().lower()
		res[key] = val
	return res


class Request2(urllib2.Request):

    def __init__(self, url, data=None, headers={},
                 origin_req_host=None, unverifiable=False):
        # unwrap('<URL:type://host/path>') --> 'type://host/path'
        self.__original = unwrap(url)
        self.type = None
        # self.__r_type is what's left after doing the splittype
        self.host = None
        self.port = None
        self.data = data
        self.headers = {}
        for key, value in headers.items():
            self.add_header(key, value)
        self.unredirected_hdrs = {}
        if origin_req_host is None:
            origin_req_host = request_host(self)
        self.origin_req_host = origin_req_host
        self.unverifiable = unverifiable

def readUrl(url, silent=True, unicodeIfPossible=True):
	"""
	Retrieve the contents of the specified HTTP URL.
	In case of a text/* MIME, decodes it using the encoding from the HTTP headers
	(META is ignored)	and returns an unicode string. If MIME is different or
	unicodeIfPossible==False is given, returns	a byte string containing the original content, non-decoded.
	Unless silent==True, all errors are ignored silently and an empty string is returned.
	"""

	try:
		from uastrings import userAgentStrings
	except:
		print 'WARNING - module uastrings.py not found. Function util.readUrl will now use a fixed user-agent string.'
		userAgentStrings = ['Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US; rv:1.9.0.6) Gecko/2009011913 Firefox/3.0.6']

	req = urllib2.Request(url=url)
	req.add_header('user-agent', random.choice(userAgentStrings))
	#req.add_header('referer', 'http://news.google.com')
	#req.add_header('connection', 'keep-alive') # doesn't work
	#req.add_header('keep-alive','300')
	req.add_header('accept-language', 'en-us,en;q=0.5')
	req.add_header('accept-encoding', 'gzip,deflate')
	req.add_header('accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
	req.add_header('accept-charset','ISO-8859-1,utf-8;q=0.7,*;q=0.7')
	
	try:
		t = time.time()
		f = urllib2.urlopen(req)
		content = f.read()
		if not silent:
			print 'Fetched %d bytes in %.3f seconds (%s)' % (len(content), time.time()-t, url)

		# if content is compressed, decompress it
		if f.headers.get('content-encoding') == 'gzip':
			contentIO = StringIO.StringIO(content)
			gzipFile = gzip.GzipFile(fileobj=contentIO)
			content = gzipFile.read()
			
		# enforce unicode
		if unicodeIfPossible:
			try: content = decodeText(content, f.headers)
			except ValueError: pass
		return content
	except:
		import traceback, sys
		traceback.print_exc(file=sys.stderr)
		if silent:
			return ''
		else:
			raise


def levenshtein(first, second):
    """Find the Levenshtein distance between two strings."""
    if len(first) > len(second):
        first, second = second, first
    if len(second) == 0:
        return len(first)
    first_length = len(first) + 1
    second_length = len(second) + 1
    distance_matrix = [range(second_length) for x in range(first_length)]
    for i in range(1, first_length):
        for j in range(1, second_length):
            deletion = distance_matrix[i-1][j] + 1
            insertion = distance_matrix[i][j-1] + 1
            substitution = distance_matrix[i-1][j-1]
            if first[i-1] != second[j-1]:
                substitution += 1
            distance_matrix[i][j] = min(insertion, deletion, substitution)

    return distance_matrix[first_length-1][second_length-1]


def findAll(lst, el):
	"""
	Returns a list of positions of all occurences of el in list lst.
	"""
	pos = []
	next = 0
	while True:
		try:
			next = string.index(substr,next)+1
			pos.append(next-1)
		except:
			return pos

		
def writeToFile(data, fn):
	"""
	Fills the file fn with data data.
	If fn already exists, it is overwritten, otherwise created.
	Data is utf-8 encoded prior to writing if needed.
	"""
	f = open(fn, 'w')
	if isinstance(data,unicode):
		data = data.encode('utf8')
	f.write(data)
	f.close()

def log_calls(func):
	"""
	A function decorator that prints each invocation of the decorated function 
	(along with the arguments) to stdout.
	"""
	def logged_func(*args, **kwargs):
		log = (">> %s(" % func.__name__) + ', '.join(map(repr,args))
		if kwargs: log += ", "+", ".join("%s=%r" % kv for kv in kwargs.items())
		log += ")"
		print log
		return func(*args, **kwargs)
	return logged_func

def restart_on_crash(log_exprs=[]):
	"""
	A function decorator that re-runs the wrapped function in case it raises an exception.
	This is repeated until the function succeeds.
	
	`log_exprs` is a list of strings, each string being an expression whose value at the time
	of exception is displayed. Example:
	>>> @restart_on_crash(log_exprs=['b', 'a+b'])
	>>> def divider(a):
	>>> 	import random; random.seed(time.time())
	>>> 	for t in range(5):
	>>> 		print a, 'divided by', b, 'is', a/b
	>>> 	print 'done'
	
	The error report is also written to a (hardcoded) file.
	"""
	def decorator(func):
		REPORT_FILE = os.path.abspath('./_crash_report.txt')
		def wrapped_func(*args, **kwargs):
			alles_gut = False
			while not alles_gut: 
				try:
					func(*args, **kwargs)
					alles_gut = True
				except:
					print '%s() was restarted at %s because of the following error:' % (func.func_name, datetime.datetime.now().isoformat())
					traceback.print_exc()
				
					try:
						# find the most nested invocation of `func` in the traceback
						func_frame = None
						tb = sys.exc_info()[2]
						while True:
							if tb.tb_frame.f_code == func.func_code:
								func_frame = tb.tb_frame
							if not tb.tb_next: break
							tb = tb.tb_next
						# evaluate the expression-to-be-logged in the scope of func
						with open(REPORT_FILE, 'w') as f:
							f.write('Crash in function %s at %s\n\n' % (func.func_name, datetime.datetime.now().isoformat()))
							traceback.print_exc(file=f)
							f.write('\n\nLogged variables/expressions:\n')
							for log_expr in log_exprs:
								try: log_val = repr(eval(log_expr, globals(), func_frame.f_locals))
								except: log_val = '(error while evaluating expression; %r)' % sys.exc_info()[1]
								f.write('>>> %s: %s\n' % (log_expr, log_val))
						print 'More info can be found in %r' % REPORT_FILE
					except:
						print 'Additionally, an error was encountered trying to write the crash report to %r:' % REPORT_FILE
						traceback.print_exc()
		return wrapped_func
	return decorator
