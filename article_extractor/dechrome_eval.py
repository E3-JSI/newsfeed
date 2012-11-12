"""
A relatively dirty script for evaluating various article cleaners.
Prints the diffs of the outputs of the following algorithms:
- Tomaz Hocevar's WWW article (= article_extractor2.py)
- Tomaz Hocevar's WWW article improved (= article_extractor2.py with xDoc as input)
- dechrome.py, the heuristic approach (used to be a part of articleUtils.py)
on three datasets:
- english articles
- non-english alphabet (i.e. 1 sound = 1 glyph) articles
- syllabary (i.e. 1 sound = 1 syllable/word) articles

Everything is hardcoded.
"""

import os, sys; sys.path.extend(('.','..'))

import dechrome; reload(dechrome)
import article_extractor2 as ae; reload(ae)

from collections import defaultdict
import logging
logging.getLogger().setLevel(logging.WARNING)

import cld
import diff_match_patch
dmp = diff_match_patch.diff_match_patch()

def openConnection(cursor_name=None):
	"Creates a NEW (!) connection to the database and returns a tuple (connection, cursor for this connection)."
	import psycopg2, psycopg2.extensions, psycopg2.extras
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
	conn = psycopg2.connect(database='news', host='maximus', user='mitjat', password='XXX_GITHUB_XXX')
	if cursor_name:
		cur = conn.cursor(cursor_name, cursor_factory=psycopg2.extras.DictCursor)
	else:
		cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	return (conn,cur)	

def prettyDiff(txt1,txt2):
	diff = dmp.diff_main(txt1,txt2)
	dmp.diff_cleanupSemantic(diff)
	prettyDiff = dmp.diff_prettyHtml(diff)
	if type(prettyDiff)==unicode: prettyDiff=prettyDiff.encode('utf8','replace')
	return prettyDiff
		
conn,cur = openConnection('bla')

# some article ids just for repeatability
articleIds = '32179139,25051497,36347611,36353698,32179140,32179141,25051498,32179142,36347612,32179143,32179144,25051499,32179231,36353639,32179333,32179544,25051500,32179545,36353402,21306643,25051502,21306812,21306815,21306816,21306814,21306811,21306813,21306808,36325939,36353641,36347613,21306775,36353403,25051501,36288164,21306642,36281231,36353404,36353405,36347101,36353406,36346461,36277543,36346145,22590909,22590910,21306771,21306773,22590911,21306772,21306774,22590912,22590905,22590904,22590903,21306818,36212543,36353612,36210047,36353625,36353407,21306819,36353408,36355670,32179546,21306822,22590913,21306824,21306823,22590915,22590916,21306826,21306825,22590917,22590914,21306820,36346156,21306821,32179547,32179548,32179549,32179550,36353627,36355136,32179551,32179679,36355137,36355138,36344126,32179762,25051503,32179788,36355139,25051504,23799507,40832160,36346151,36196872,23799508,32179789'
#cur.execute('SELECT id, COALESCE(final_url,url) AS url, content FROM feed_article JOIN article ON (feed_article.id=article.feed_articleid) WHERE id IN (%s) LIMIT 100' % articleIds)
#cur.execute("SELECT id, lang_iso, lang_altcode, COALESCE(final_url,url) AS url, content FROM feed_article JOIN article ON (feed_article.id=article.feed_articleid) JOIN feed_article_meta m USING (id) WHERE url ~ '.*\\.(tw|my|sy|ph|mm|cn|jp)/.*' and not url ~ '.*eng.*' LIMIT 100")
cur.execute('SELECT id, COALESCE(final_url,url) AS url, content FROM feed_article JOIN article ON (feed_article.id=article.feed_articleid) WHERE content IS NOT NULL LIMIT 10000')


LANG_GROUPS = ('english', 'alphabet', 'syllabary')
aids = defaultdict(list)  # lang group -> list of article ids.
done_hosts = set()

for lg in LANG_GROUPS:
	f = open('./evaluation/compare_%s.html' % lg,'w')
	f.write('<style>table{table-layout:fixed}td{vertical-align:top;border:solid #ccc 1px;border-top:solid black 2px;}.tiny{font-size:70%; width:45%}</style>')
	f.write('<table><tr><th>Article info<th>diff(old Tomaz, new Tomaz)<th class="tiny">diff(Mitja, new Tomaz)')
	f.close()

for i,row in enumerate(cur):
	url = row['url']
	host = url.split('/')[2]
	if host in done_hosts: continue
	done_hosts.add(host)
	print i, row['id'], host
	lang = cld.detect(row['content'])[1].split('-')[0]

	if lang == 'un':
		continue
	elif lang == 'en':
		lang_group = 'english'
	elif lang in 'ja ko zh hi ms ml te ta jw oc ur gu th kn pa fa km'.split():
		lang_group = 'syllabary'
	else:
		lang_group = 'alphabet'

	if len(aids[lang_group])>=50: continue
	aids[lang_group].append(row['id'])
		
	try: html = str(row['content']).decode('utf8')
	except: continue
	txt2, xDoc = dechrome.parseCleartext(html, url, returnParseTree=True)  # heuristic approach
	txt2 = txt2.encode('utf8')
	txt1_old = ae.get_cleartext(html.encode('utf8')).encode('utf8')  # Tomaz 
	txt1 = ae.get_cleartext(xDoc).encode('utf8')  # Tomaz improved

	f = open('./evaluation/compare_%s.html' % lang_group,'a')
	f.write('<tr><td><b>%s</b> &nbsp; %s<br><a style="font-size:80%%" target="_blank" href="%s">%s</a><br>%s <td class="tiny">%s<td class="tiny">%s \n' % (
			len(aids[lang_group]),	row['id'], url.encode('utf8'), url.encode('utf8')[:30], lang,
			prettyDiff(txt1_old, txt1), prettyDiff(txt2,txt1),
			))
	f.close()

for lg in LANG_GROUPS:
	f = open('./evaluation/compare_%s.html' % lg,'a')
	f.write('</table>  <b>Used articles</b>: %r' % aids[lg]);
	f.close()
