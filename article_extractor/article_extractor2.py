import re
import sys; sys.path.append('.')
import article_extractor
import sys; sys.path.append('..')
import util
import struct
import hashlib
import lxml.html, lxml.etree, lxml.html

htmlWhitespace = re.compile(r'(<br ?/? ?>|<p[ >])')
htmlTags = re.compile(r'<\s*/?\s*(\w+)[^>]*>')
htmlComments = re.compile(r'<!--.*?-->', re.DOTALL)
txtWhitespace = re.compile(r'[ \t]+')
multipleNewline = re.compile(r'(\n\s*)+')

def _load_copyright_ngrams(path):
	"""
	Return a set of ngram hashes read from `path` which should contain lines
	with two space-separated numbers: n and n-gram hash.
	Such files are produced by `dump_common_maximal()` in find_freq_ngrams.cpp.
	"""
	try:
		with open(path) as f:
			return set(int(line.split()[1]) for line in f)
	except Exception, e:
		print 'Warning: failed to load copyright-ngrams data from %r.' % path
		print 'Reason:', e
		return set()
stop_ngrams = _load_copyright_ngrams("../copyright_removal/freq_ngrams.txt")



def md5_64(txt):
	"Lower 64 bits of md5. Cast as an uint64."
	return struct.unpack("<Q", hashlib.md5(txt).digest()[8:])[0]

def remove_copyright(txt, n, stop_ngrams=stop_ngrams):
	"""
	Takes a cleartext version of a document (unicode) and removes all `n`-grams whose
	`md5_64()` hashes are contained in `stop_ngrams`.
	Detected n-grams can overlap.
	Takes unicode, returns unicode.
	"""
	s = txt.encode('utf8')+' '  # C++ computes hashes on utf8-encoded strings.
	a = b = 0                   # Like in find_freq_ngrams.cpp, s[a:b] is the current n-gram
	for i in range(n):
		b = s.find(' ',b+1)
		if b==-1: break
	if b==-1: return txt  # no ngrams, hence no changes

	kill_ranges = []  # (start,end) character index spans that need to be removed
	while b!=-1:
		#print md5_64(s[a:b]), `s[a:b]`
		# check if current n-gram needs to be removed
		if md5_64(s[a:b]) in stop_ngrams:
			if kill_ranges and kill_ranges[-1][1] >= a: kill_ranges[-1][1] = b+1
			else: kill_ranges.append([a,b+1])
		# advance to the next n-gram
		a = s.find(' ',a)+1
		b = s.find(' ',b+1)

	if not kill_ranges: return txt  # no changes
		
	slices = [slice(0, kill_ranges[0][0])]
	for i in range(len(kill_ranges)-1):
		slices.append(slice(kill_ranges[i][1], kill_ranges[i+1][0]))
	slices.append(slice(kill_ranges[-1][1], -1))  # -1 to remove the trailing space character
	s = ''.join(s[slc] for slc in slices)

	return s.decode('utf8', 'replace')

def get_cleartext(html, logger=None):
	"""
	Converts a full-page html (utf8) to the cleartext (unicode) containing just the article body.
	The first line of the return value is the title (can be empty). If there was an
	error or if the html is suspected not to contain an article, an empty string is returned.

	`logger` should be None or a logging.Logger instance.

	`html` is usually text (unicode or utf8) can also be a lxml tree; in that case, some heuristic
	cleanup is performed first.
	
	This calls the glib html->cleartext function, then does a bit of cleanup
	and error checking.
	"""

	if type(html) == lxml.html.HtmlElement:
		# time for heuristic cleanup
		xDoc = html
		if xDoc is None: return ''
		for el in xDoc.findall('.//*'):
			info = (el.get('id','')+':'+el.get('class','')).lower()
			# if the element is suspicious, replace it with "barrier" (a bunch of <img> and <a> tags)
			# that the C module is very unlikely to include in the result
			if re.search('foot|header|^nav|naviga[ct]|[ck]omm?ent|dis[kc]us|user|notice|spe[cz]ial|about', info) \
					and not re.search('main|article|content', info) and el.getparent() is not None:
				idx = el.getparent().index(el)
				el.getparent()[idx+1:idx+1] = [lxml.etree.fromstring('<a href="blah.com"><img src="http://shite.com" /></a>') for i in range(20)]
				el.drop_tree()
		html = lxml.etree.tostring(xDoc, encoding='utf8')

	
	# If the output is very non-html-looking, don't bother with C++, it will only crash
	if '\000' in html:
		return ''

	# Do the decoding, but watch out for weirdness in return values
	txt = article_extractor.get_cleartext(html)
	try:
		txt = txt.decode('utf8')
	except UnicodeDecodeError:
		if logger:
			logger.exception('Article %s was cleartexted, but cleartext was not in utf8. Saved cleartext to /tmp/non_utf8. Exception:')
			try:
				with open('/tmp/non_utf8','w') as f: f.write(txt)
			except: 
				pass
		txt=''

	if len(txt) < 200:
		# This can't be good/real
		return ''
		
	# Fix up the output. Step 1: remove HTML tags.
	# TODO: Need to strip tags from titles as well (rss crawler, gnews crawler).
	# Move some of the code below to util.py, reuse.
	global txtr; txtr = txt   # for debug
	# Step 1a: small normalizations
	txt = txt.rstrip('<')   # glib output glitch; this is present only sometimes
	txt = txt.replace('\r\n','\n').replace('\r','\n')
	txt = htmlComments.sub('', txt)
	txt = htmlWhitespace.sub(' \n\\1', txt)
	# Step 1b: strip html tags (not elements!) except <script> and <style>
	txt = htmlTags.sub(lambda m: m.group(0) if m.group(1).lower() in ('script','style') else '', txt)
	# Step 1c: if any tags remain, they are bad. Remove them with lxml (cheap at this point).
	if htmlTags.search(txt):
		xRoot = lxml.html.fromstring(txt)
		for c in xRoot:
			xRoot.remove(c)
		txt = xRoot.text_content()		
	
	# Step 2: decode HTML entities, normalize punctuation (e.g. weird quotes)
	txt = util.normalizePunctuation(util.htmlUnescape(txt))

	# Step 3: normalize whitespace
	txt = multipleNewline.sub('\n', txt)
	txt = txtWhitespace.sub(' ', txt)

	# Step 4: add empty title (old articles in the db have the first row reserved for the title)
	txt = '\n'+txt
	# Step 5: remove copyrights and similar boilerplate
	txt = remove_copyright(txt, 9)

	if type(txt) != unicode:
		print rstr
		txt = txt.decode('utf8')
	return txt

if __name__=='__main__':
	print repr(remove_copyright(
			'majhna sem bila, piske sem pasla piske so civkale jaz sem pa rasla',
			3,
			map(md5_64, ['sem bila, piske', 'piske sem pasla', 'jaz sem pa']),
			))
	
	import psycopg2, psycopg2.extras
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
	conn = psycopg2.connect(database='news', host='maximus', user='mitjat', password='XXX_GITHUB_XXX')
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute("SET bytea_output TO 'escape'")

	cur.execute("SELECT content FROM article WHERE feed_articleid=15008406 --29991787; --39606438; --3256641")
	html = str(cur.fetchone()['content'])
	txt = get_cleartext(html)
	print '%d bytes html -> %d bytes text' % (len(html), len(txt))
	print txt.encode('utf8')
