# -*- coding: utf-8 -*-

from __future__ import division

import sys, os, time, random
sys.path.extend(('.','..'))
import traceback
from util import *
import urllib,urllib2,socket,urlparse
import lxml, lxml.html, lxml.html.clean, lxml.html.soupparser


# logging setup
import logging
from logging import debug, info, warn, error, exception, critical
if __name__=='__main__':
	logger = logging.getLogger()
	logger.name = 'fetcher'
	logger.setLevel(logging.DEBUG)  # Options: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50
	formatter = logging.Formatter(fmt='[pid%(process)d-%(threadName)s] %(levelname)8s %(asctime)s %(message)s')
	logger.handlers = []
	for handler in [
	   logging.StreamHandler(sys.stderr),
	   logging.FileHandler(filename=logger.name+'.log', encoding='utf8')
	]:
	   handler.setFormatter(formatter)
	   logger.addHandler(handler)


HTML_INLINE_TAGS = ['b','u','i','a','font','span','em','strong']

def textify(el, descendInto=HTML_INLINE_TAGS+['br','p']):
	"""
	Recursively get all the text from the given element;
	however, only recurse into the elements from descendInto.
	Replaces <br>s with newlines and strips all other newlines.
	"""
	if el.tag == lxml.etree.Comment:
		return ''
	elif el.tag == 'br':
		return '\n' + (el.tail or '')
	else:
		retVal = (el.text or '') + \
			''.join(textify(x, descendInto) if descendInto=='all' or x.tag in descendInto else (x.tail or '') for x in el) + \
			(el.tail or '')
		if el.tag == 'p':
			retVal = '\n%s\n' % retVal
		return retVal

def xmlizeParagraphs(text):
	"""
	Takes a plain string in which newlines are assumed to denote paragraph breaks.
	(textify() outputs such text); then replaces newlines with <P> marks and
	escapes XML special characters.
	"""
	text = xmlEscape(text)
	return '\n'.join('<p>'+x.strip()+'</p>' for x in text.split('\n') if x.strip())
	
def avgTextDepth(el):
	"""
	Average number of levels of nesting below `el` per character, for all alphabetical
	characters contained in `el`.
	"""
	def cumulativeDepth(el):
		return dataLen(el) + sum(cumulativeDepth(x) for x in el)

	elLen = dataLen(el)
	return cumulativeDepth(el)*1.0 / elLen if elLen else 0

def contentBearingParas(el):
	"""
	Returns a list of all <p>-children (direct descendants!) of el which look
	like the contain article content. Also return standalone <br>s which
	appear in between.
	"""
	ps = el.findall("p")
	ok = set()
	for p in ps:
		# only accept paragraphs of sufficient length
		# that are not suspiciously over-structured
		if dataLen(p) > 40 \
		and dataLen(p) / (len(p.xpath('.//text()[string-length(normalize-space(string())) > 3]')) or 1)>30:  
			ok.add(id(p))			
	# second pass -- if there is a <p> which does not look OK but its surrounding
	# <p>s do, it's probably OK as well
	for i in range(1,len(ps)-1):
		if id(ps[i]) not in ok and id(ps[i-1]) in ok and id(ps[i+1]) in ok:
			ok.add(id(ps[i]))
	# build the result set
	res = [p for p in ps if id(p) in ok]
	# exclude the last paragraph if it looks like <p><i>Mike is our reporter from Italy. He has a wife and two kids.</i></p>
	if res and avgTextDepth(res[-1]) >= max(1.7, sorted(map(avgTextDepth,res))[-min(len(res),max(2,len(res)*2//10))]):
		debug("Popping short and structured last paragraph (avgTextDepth %.2f): %s" % (avgTextDepth(res[-1]), textify(res[-1])[:50].strip()+'[...]'))
		res.pop()
		if res and id(res[-1]) not in ok: res.pop()
	return res

def isProperElement(el):
	"""
	Returns False for elements that are likely part of footer, header,
	navigation or "user comments" section. Also, returns false for
	<blockquote> elements since those are unlikely to be at the top of
	the parse-subtree that contains all the content.
	"""
	info = (el.get('id','')+':'+el.get('class','')).lower()
	return (re.search('foot|header|^nav|navigat|comment|discuss|user', info) == None) \
		and (el.get('exQuote') != 'yes')

def bareForm(txt, retainNumbers=False):
	"""
	Strips all numbers (unless retainNumbers=True), whitespace and punctuation from
	the given text, then lowercases it.
	"""
	if txt is None: return ''
	return ''.join(ch for ch in txt.lower() if ch.isalpha() or (retainNumbers and ch.isalnum()))

def dataLen(el_or_txt):
	"""
	A measure of information content in a DOM element or plaintext string.
	"""
	if isinstance(el_or_txt, lxml.etree.ElementBase):
		el_or_txt = el_or_txt.text_content()
	return len(bareForm(el_or_txt).encode('utf8'))  # utf8 makes chinese characters 3 times "longer"

def parseTitle(xDoc, url):
	"""
	Finds the title of an article (passed in as a BeautifulSoup parse tree).
	If nothing convincing is found, returns an empty string.
	Basic heuristic: first, try to find an <H1>. If it does not exist, take
	what's in <TITLE>, strip page name (e.g. "BBC News").
	"""
	global xTitle, titleCandidates, metaTitle, h1s
	titleCandidates = []
	
	h1s = xDoc.findall('.//h1')
	if h1s and len(h1s)==1:
		h1 = h1s[0]
		if h1.find('.//img') is None and not hasAncestor(h1,'a') and h1.find('.//a') is None:
			titleCandidates.append(textify(h1, 'all').replace('\n',' ').strip())

	metaTitle = xDoc.find(".//meta[@name='title']")
	if metaTitle is not None:
		titleCandidates.append(textify(metaTitle, 'all').replace('\n',' ').strip())
	
	xTitle = xDoc.find('.//title')
	if xTitle is None:
		return ''
	title = xTitle.text_content()
	if not title.strip():
		return ''
	titleCandidates.append(title.strip())

	return parseTitleFromCleartext(titleCandidates, url)


def parseTitleFromCleartext(titleCandidates, url):
	"""
	Given a list of strings that could be titles, return the most likely title.
	The return value is one of `titleCanidates` or a substring of one of them
	or the empty string if nothing looks convincing.
	`url` is there to aid heuristics, it's never opened.
	"""
	if not titleCandidates: return ''
	
	# get the relevant parts of the URL (i.e. those that are likely to
	# represent the site name and therefore appear in <title>)
	if url:
		hostname = urlparse.urlparse(url).hostname
		siteNames = hostname.split('.')
		if siteNames[0] == 'www':
			del siteNames[0]
		if len(siteNames)>=3 and siteNames[-2]=='co':   # [news, guardian, co, uk] -> [news, guardian, guardian.co.uk]
			siteNames[-2:] = ['.'.join(siteNames[-3:])]
		else:                                           # [news, guardian, com] -> [news, guardian, guardian.com]
			siteNames[-1] = '.'.join(siteNames[-2:])
	else:
		siteNames = []

	siteNames.extend(['news','agency','international','herald','tribune','press','association', 'daily', 'weekly', 'times', 'globe', 'radio', 'online'])
	siteNames = map(bareForm, set(siteNames))
	info('Words that should not appear in the title: '+repr(siteNames))
		
	parts = re.split(r' -- | - | \| | >> ', titleCandidates[-1])
	if len(parts) == 1:
		# also try the less common delimiters (which just maybe are in the text, though not meant as delimiters at all)
		parts = re.split(r' = | \* |_|: |\|| " ', titleCandidates[-1])
	titleCandidates.extend([parts[0], parts[-1]])  # the real title always comes either first or last
	for candidate in titleCandidates:
		# obtain stripped-down words appearing in the title candidate
		words = map(bareForm, candidate.split())
		words = [w for w in words if w]
		# how many of those words are indicative of this candidate being the news site name?
		nBadWords = 0
		for word in words:
			if word in siteNames or (len(word)>3 and [sn for sn in siteNames if word in sn]):
				nBadWords += 1
		# also take acronymical urls into account, e.g. 'Financial Times'..'ft.com'
		if ''.join(w[0] for w in words) in siteNames:
			nBadWords += 2
		# short candidates are suspicious as well
		if len(words) <= 1:
			nBadWords += 2
		elif len(words) <= 3:
			nBadWords += 1
		info('nBadWords==%d for title candidate: %s' % (nBadWords, candidate))
		# if the candidate looks too suspicious, discard it
		if nBadWords >= 2 or nBadWords >= max(len(words),dataLen(candidate)/7)/2:  # we check dataLen because of chinese
			continue
		return candidate

	return ''
		
	
def findLeadIn(txt):
	"""
	If the input text begins with what looks like a lead-in (e.g. "Ljubljana, Apr 16 (STA) -- "),
	returns it. Otherwise, returns an empty string. Html entites should already have been resolved.
	"""
	separators = (' -- ',' - ',' | ',': ')
	regexp = '(%s)' % '|'.join(re.escape(sep) for sep in separators)  # enclosing the splitting exprs in () will cause them to be returned in the split list
	parts = re.split(regexp, txt[:200])
	if len(parts)==1:
		return ''

	# Try to include as many parts as possible in the lead-in.
	# Every second part of the list is a separator; skip those
	for leadInLen in range(len(parts),0,-2):  
		leadIn = ' '.join(parts[0:leadInLen:2]).replace('( ','(').replace(') ',')')
		nLeadInWords = len(leadIn.split())
		if nLeadInWords<=4 or (nLeadInWords - leadIn.count(',') - leadIn.count('(') <= 3):
			# reconstruct the lead-in (mangled by here)
			return ''.join(parts[:leadInLen+1])

	# even the first part on its own is too long to look like a lead-in
	return ''
	
def isDateline(txt):
	"""
	Returns True iff the input text appears to be a dateline (i.e. a short line containing the
	date and sometimes the location of the corresponding article with no or little other content).
	"""
	txt = re.sub('[^a-z0-9\s]',' ',txt.lower())
	words = txt.split()

	if len(words)>50:
		return False

	# one of the keywords below + some nuber nearby means we can be pretty sure of ourselves
	for keyword in ('published', 'updated', 'date'):
		if re.search(r'%s.{0,20}\d' % keyword,  txt):
			return True

	# date-related keywords
	intros = ('published','updated','posted','date')
	weekdays = ('monday','tuesday','wednesday','thursday','friday','saturday','sunday')
	weekdays2 = ('mon','tue','wed','thu','fri','sat','sun')
	months = ('january','february','march','april','may','june','july','august','september','october','november','december')
	months2 = ('jan','feb','apr','may','jun','jul','aug','sept','sep','oct','nov','dec')
	allKeywords = intros+weekdays+weekdays2+months+months2

	# find positions of keywords and numbers
	pos = []
	for kw in allKeywords:
		pos.extend(findAll(words, kw))
	for i,word in enumerate(words):
		if word.isdigit() and (1 <= int(word) <= 31  or  1850 <= int(word) <= 2100):
			pos.append(i)
	pos = list(unique(sorted(pos)))
	
	# if a cluster of 3 keywords appears within 4 words, we have a date
	for i in range(len(pos)-2):
		if pos[i+2] <= pos[i]+4:
			return True

	return False
	
def isTitle(txt, title):
	"""
	Returns true iff txt seems to contain the given title and nothing much else.
	The matching is done somewhat fuzzily. 
	"""
	txt = bareForm(txt)
	title = bareForm(title)
	if len(txt) > 4*len(title):
		return False
	score = (levenshtein(txt,title) - abs(len(txt)-len(title))) / max(len(txt), len(title))
	return score < .1
	
def hasAncestor(el, ancestor):
	"""
	Returns True iff the given BeautifulSoup elements has a given ancestor tag.
	If the ancestor parameter is a string, searches for an ancestor with a corresponding
	tag name; if it is a BS element, searches for that exact element.
	"""
	while el is not None:
		if (el.tag==ancestor) or (el==ancestor):
			return True
		el = el.getparent()
	return False

def previousElement(el):
	"""
	Traverse elements in the reverse prefix order (i.e. like reading the
	opening tags of a serialized XML in reverse). Return the element
	following `el` in this order; None if el is the first element.
	"""
	ret = el.getprevious()  # previous *sibling*, if it exists
	if ret is not None:
		# get the rightmost descendant
		while list(ret): ret = ret[-1]
	else:
		ret = el.getparent()
	return ret
			
def findPreviousElement(el, filterFunc):
	"""
	Return the first element `x` before `el` (in the order implied by
	findPreviousElement() for which filterFunc(x) returns True). 
	Returns None if there are no matches.
	"""
	x = el
	while True:
		x = previousElement(x)
		if x is None: break
		if filterFunc(x): return x
	return None

def publisherSpecificCleanup(txt, url=''):
	"""
	Perform cleanup hacks on cleartext `txt` specific to single
	(important) publishers.
	"""
	# bloomberg
	lines = txt.splitlines()
	while lines and (lines[-1].strip().startswith('To contact the editor') or lines[-1].strip().startswith('To contact the reporter')):
		lines.pop()
	txt = '\n'.join(lines)

	return txt

def parseCleartext(txt, url='', returnParseTree=False, silent=True):
	"""
	Takes HTML code from any page and returns what is most likely the "meat" of the page:
	a plain-text article with no HTML markup (except <P>s), no page navigation, headers, footers etc.
	If returnParseTree==True is given, returns the pair (cleartext article version, lxml parse tree
	of the original html).
	If no part of the input looks convincing enough, returns an empty string.

	In case of an internal error: if `silent`, returns an empty string; otherwise, throws the exception.
	
	The first line of the returned cleartext is always the page title (may be empty).
	
	Input shuld be a unicode object. Output is unicode as well, with HTML escape sequences (e.g. &lt;) decoded
	and some puctuation normalized (e.g. em-dash normalizes to an ASCII minus sign; see util.normalizePunctuation()).
	
	The url parameter is optional and represents the URL from which the HTML was fetched. This function
	does not perform any fetching. The url parameter is only used to aid some heuristics.

	The basic heuristic: find the HTML element containing <P>'s with lots of text. If none
	exist, find a <DIV> or <TD> with lots of text and <BR>'s.

	This method uses the lxml HTML parser extensively.
	"""
	global xDoc, xIntro, xContentStart
	
	if not url:
		info('WARNING: no url passed to parseArticle. The title extraction heuristic will suffer.')
		
	def retVal(cleartext, parseTree):
		"""
		Strip cleartext of empty lines (except the first one -- empty title), collapse non-newline whitespace,
		then return the modified cleartext plus possibly the parse tree.
		"""
		cleartext = re.sub('[ \t]+',' ',cleartext)
		cleartext = '\n'.join(line.strip() for (i,line) in enumerate(cleartext.splitlines()) if i==0 or line.strip())
		cleartext=unicode(cleartext)  # lxml returns str objects if contents are ascii-only; otherwise unicode

		cleartext = publisherSpecificCleanup(cleartext)

		if returnParseTree:
			return (cleartext, parseTree)
		else:
			return cleartext
		
	try:
		if type(txt) != unicode: txt = txt.decode('utf8','replace')
		
		# some uncommon doctypes cause BeautifulSoup (which lxml falls back on) to crash;
		# remove them all just in case
		txt = re.sub(r'<!DOCTYPE[^>]*>','',txt)
		txt = re.sub(r'<\?xml[^>]*encoding *=?[^>]*>','',txt)
		# Throw away comments and some other stuff
		try:
			#print 'before cleaning', type(txt)
			txt = lxml.html.clean.Cleaner(style=True, scripts=True, comments=True, annoying_tags=False, embedded=False, forms=False, frames=True, links=False, meta=False, page_structure=False, processing_instructions=True, remove_unknown_tags=False, safe_attrs_only=False, javascript=False, remove_tags=['nobr']).clean_html(txt)
			#print 'afer cleaning', type(txt)
		except:
			xDoc = lxml.html.soupparser.fromstring(txt)
			txt = lxml.etree.tostring(xDoc, encoding=unicode)
			txt = lxml.html.clean.Cleaner(style=True, scripts=True, comments=True, annoying_tags=False, embedded=False, forms=False, frames=True, links=False, meta=False, page_structure=False, processing_instructions=True, remove_unknown_tags=False, safe_attrs_only=False, javascript=False).clean_html(txt)
		txt = normalizePunctuation(txt, normalizeWhitespace=True)

		# Build a DOM tree
		xDoc = lxml.html.document_fromstring(txt, parser=lxml.html.HTMLParser(recover=True))

		# Throw away invisible elements (inline styles only)
		for x in xDoc.xpath("//*[contains(translate(@style,'DISPLAY:NONE ','display:none'),'display:none')]|//noscript"):
			x.drop_tree()

		# Replace <blockquote>s with <p>s and <q>s with <span>s;
		# remember they were quotes with a new exQuote atrtibute
		for quote in xDoc.xpath('.//blockquote|.//q'):
			quote.tag = {'blockquote':'p', 'q':'span'}[quote.tag]
			quote.set('exQuote', 'yes')

		content = ''
		minArticleLen = 350

		# Check if the article uses schema.org markup
		explicitContent = xDoc.xpath(".//*[@itemprop='articleBody']")
		explicitContent = [x for x in explicitContent if x.getparent() is not None and not any(hasAncestor(x.getparent(), ec) for ec in explicitContent)]
		if explicitContent:
			content = ''.join(textify(ec, descendInto='all') for ec in explicitContent)
			if len(content) < minArticleLen: content = ''
			xContentStart = explicitContent[0]

		if not content:
			for liberateParas in (False, True):
				xParents = unique(p.getparent() for p in xDoc.findall('.//p') if p.getparent() is not None)

				if liberateParas:
					# Unwrap all elements which are the single children of their parent
					for x in xDoc.findall('.//*'):
						if len(x)==1 and x[0].tag=='p' and bareForm(x[0].tail)=='' and bareForm(x.text)=='':
							x.drop_tag()
				
				# Find the first element containing <P>s with lots of text (or a later one with much more text)
				for xParent in xParents:
					if not isProperElement(xParent):
						continue
					paras = contentBearingParas(xParent)
					contentCandidate = '\n'.join(textify(x) for x in paras)
					cLen = dataLen(contentCandidate)
					debug('%50s       %d bytes of text in %d paras (%s ...)' % ('%s #%s .%s' % (xParent.tag, xParent.get('id',''), xParent.get('class','')), cLen, len(xParent.findall('.//p')), lxml.html.tostring(xParent)[:100]))
					if cLen > minArticleLen and cLen > 2*dataLen(content):
						content = contentCandidate
						xContentStart = paras[0]
						global ppp; ppp=paras  # DEBUG ONLY

				if content:
					break  # don't do the liberateParas stuff if not necessary

		if not content:
			# Nothing found so far; the article body very likely is not in <P>'s.
			# Try <DIV>'s and <TD>'s as well, but only those (if any) that really have lots of very clean text
			for xParent in xDoc.xpath('.//div|.//td'):
				if xParent.find('p') is not None: continue  # this element was already discarded before, with more fine-tuned rules
				contentCandidate = textify(xParent)#, descendInto=['font','span'])
				debug('%s #%s .%s\t    %d bytes of text' % (xParent.tag, xParent.get('id',''), xParent.get('class',''), len(contentCandidate)))
				cLen = dataLen(contentCandidate)
				nNastyChildren = len(xParent.xpath('.//img|.//a'))
				if (len(contentCandidate) > 1.5*minArticleLen  and  cLen > 20*nNastyChildren) or \
					(len(contentCandidate) > minArticleLen  and  cLen > 30*nNastyChildren): #0.8*len(unicode(xParent))):
					content = contentCandidate
					xContentStart = xParent
					break

		if not content:
			# No convincing chunk of text found
			return retVal('', xDoc)

		# Find the title of the article
		title = parseTitle(xDoc, url)

		
		# Find the first/intro paragraph of the article. It is often marked up differently
		# and found in the parse tree just before the rest of the text. Take the first
		# string that has more than 40 "real" characters (extensive whitespace is common)
		forbiddenWords = set(u'photo foto (c) ©'.split())
		xIntro = findPreviousElement(xContentStart,
			lambda el: dataLen(el.text_content()) > 50 and not hasAncestor(el,'a') and not hasAncestor(xContentStart,el))
		# We might have found just the last snippet of the intro. Climb the parse tree through
		# inline-level parents, but not above the level of being content's sibling
		while xIntro is not None and xIntro.getparent() is not None and \
		not hasAncestor(xContentStart, xIntro.getparent()) and \
		(xIntro.tag in HTML_INLINE_TAGS):
			xIntro = xIntro.getparent()
		# If it really is the intro, there will be very little or no text between it and the body
		padding = 0; x = previousElement(xContentStart)
		if xIntro is not None:
			while not hasAncestor(x, xIntro):
				padding += dataLen((x.text or '')+(x.tail or ''))
				x = previousElement(x)
			info('Intro candidate (%d characters before body): %r' % (padding, textify(xIntro,'all')))

		if xIntro is not None:
			intro = textify(xIntro,'all')
		# Make sure this really is the intro -- a series of heuristics find false positives
		try:
			assert xIntro is not None, "no xIntro candidate"
			assert not (padding > 70), "padding too large"
			assert not (findLeadIn(content)), "main body already contains the lead-in"
			assert not (isDateline(intro)), "it's a dateline"
			assert not (hasAncestor(xIntro,'a')), "it's a link"
			assert not (hasAncestor(xIntro,'head')), "it's in <head>"
			assert not (xIntro.find('img') is not None), "contains an image"
			assert not (5*len(xIntro.findall('.//*')) > len(intro.split())), "contains too much markup"
			assert not [True for w in forbiddenWords if w in intro.lower()], ("contains one of the forbidden words: "+str(forbiddenWords))
			assert not (isTitle(intro, title)), "it's the title"
		except AssertionError, err:
			intro = ''
			info('Intro candidate rejected: '+err.args[0])
			

		return retVal(title+'\n' + intro + content, xDoc)		

	except Exception,e:
		if silent:
			traceback.print_exc()
			return retVal('', None)
		else:
			raise

	
# quick self-test/debug
if __name__ == '__main__':
	url='http://www.bloomberg.com/news/2012-08-30/merkel-seeks-solar-talks-to-prevent-china-dumping-case-in-europe.html'
	url = 'http://www.pcadvisor.co.uk/news/software/3379083/hp-launches-open-webos-into-beta-pushes-ahead-with-hiring/?olo=rss'
	url = 'http://www.neuepresse.de/Nachrichten/Panorama/Uebersicht/Deutsche-wollen-mehr-fuer-Geschenke-ausgeben'
	if len(sys.argv)>1: url = sys.argv[1]
	
	print 'fetching... %r' % url
	raw = urllib.urlopen(url).read()
	html = decodeText(raw)
	print 'fetched'

	t0 = time.time()
	txt = parseCleartext(html, url)
	print 'parsed in %.2f seconds: %s' % (time.time()-t0, url)

	print txt.encode('utf8')
	#os.system('echo %s | clip' % url)
