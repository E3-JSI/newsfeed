#
# feed sucker common functions
#

import re
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import random
import urlparse
import pdb

#from BeautifulSoup import BeautifulSoup, Tag, NavigableString
import urllib
import urlparse

import datetime, pytz
import traceback

import util
import iso_map

#crashes: from article_extractor import dechrome
#crashes: import lxml, lxml.html, lxml.html.clean, lxml.html.soupparser
#does not crash: import BeautifulSoup
#stalls: import lxml
import urllib,urllib2,socket,urlparse 

################ database ###############

prepared = []

def extract_hostname(URL):
	#m = re.compile('[^:]*://([^/#\?]*)[/#\?]*.*').match(URL)
	#if m: return m.group(1)
	sp = urlparse.urlsplit(URL)
	assert sp.scheme.lower() in ['http', 'https']
	#assert ' ' not in URL	# aparently dela ce je presledk not .. a ga urllib escapa?
	assert ' ' not in sp.netloc
	assert len(sp.netloc) >= 5
	return sp.netloc

def mk_timestamp(t):
	if not t: return '1000-01-01 00:00:01'
	else: return str(t.tm_year) + "-" + str(t.tm_mon) + "-" + str(t.tm_mday) + " " + str(t.tm_hour) + ":" + str(t.tm_min) + ":" + str(t.tm_sec)

###

def DB_connect(appname=None):
	db = psycopg2.extras.DictConnection("host=maximus.ijs.si dbname=news user=news password=XXX_GITHUB_XXX")
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
	
	c = db.cursor()
	c.execute("SET bytea_output TO 'escape'")	# za psycopg2 < 2.4
	if appname:
		c.execute("SET application_name TO %s", (appname,))
#	c.execute('SET DATESTYLE TO \'ISO\'')
	db.commit()
	
	return db

def DB_exec(db, cmd):
	c = db.cursor()
	c.execute(cmd)
	c.close()
	db.commit()

def DBod(curs):
	r = curs.fetchone()
	if r:
		return dict(r)
	else: return None

def DB_prepare(db, clin):
	c = db.cursor()
	for classes, name, types, command in prepared:
		if len(classes & clin) > 0:
			c.execute("PREPARE %s (%s) AS %s" % (name, ','.join(types), command))
	c.close()
	db.commit()

## feed sucker
# tle je treba dodat da site ne sme bit zaklenjen pa to ...
SQL_FIND_FEEDS = "SELECT * FROM feed WHERE NOT disabled AND next_scan < NOW() ORDER BY next_scan ASC LIMIT 1"
SQL_FIND_NEXT = "SELECT (next_scan-NOW())::reltime::int AS next FROM feed WHERE NOT disabled ORDER BY next_scan ASC LIMIT 1"

def DB_get_next_random_feed(db):
	#print "get next random feed"
	c = db.cursor()
	c.execute(SQL_FIND_FEEDS)	# lock for update
	r = DBod(c)
	return r

def DB_release_feed(db, f):
	pass

def DB_get_next_feed_timestamp(db):
	#print "get timeout"
	c = db.cursor()
	c.execute(SQL_FIND_NEXT)
	r = DBod(c)
	return r

def DB_log_feed_suck(db, feed_id, feed_site_id, http_code, note=None, n_e=None, n_i=None, unchanged=None):
	#print "logging feed=%d code=%d note=%s" % (feed_id, http_code, note)
	c = db.cursor()
	c.execute("INSERT INTO feed_suck_log (feedid, feedsiteid, HTTP_code, note, n_e, n_i, unchanged) VALUES (%s, %s, %s, %s, %s, %s, %s)", (feed_id, feed_site_id, http_code, note, n_e, n_i, unchanged))
	db.commit()

def DB_disable_feed(db, id, now=False, flag=3):
	if now:
		print "db> Disabling feed %d with flag %d" % (id,flag)
		c = db.cursor()
		c.execute("UPDATE feed SET disabled=true, flag=%s, last_scan=now() WHERE id=%s", (flag,id,))
		db.commit()
	else:
		print "db> Conditionally disabling feed %d with flag 11" % id
		c = db.cursor()
		c.execute("UPDATE feed SET failures=failures+1, disabled = disabled OR (failures>4), flag=11 WHERE id=%s", (id,))
		db.commit()

def try_slice(l, n):
	try:
		return l[:n]
	except:
		return None

def DB_update_feed(db, feed, suck, last_ts):
	#print "db> Updating feed %d and feed info" % feed['id']

	c = db.cursor()
	
	etag=title=description=language=copyright=pub_date=ttl=None	
	
	if 'etag' in suck: etag=suck.etag
	if 'title' in suck.feed: title=try_slice(suck.feed.title,1000)
	if 'description' in suck.feed: description=try_slice(suck.feed.description,10000)
	if 'language' in suck.feed:
		language = (suck.feed.language or '').split('-')[0]
		if language not in iso_map.iso3to2:
			language = iso_map.iso2to3.get(language, None)		
	if 'copyright' in suck.feed: copyright=try_slice(suck.feed.copyright,100000)
	if 'updated_parsed' in suck.feed: pub_date=suck.feed.updated_parsed
	if 'ttl' in suck.feed: ttl=suck.feed.ttl

	c.execute("UPDATE feed SET last_etag=%s, failures=0, pruning_ts_last=%s WHERE id=%s", (psycopg2.Binary(etag) if etag else None, last_ts, feed['id']))
		
	# !bn: tegale res ni treba ob vsakmu feed-updateju flushat v bazo .. zamenji s stored proceduro? -- sej updatea se samo na 7 dni; selecta se pa vedno..
	c.execute("UPDATE feed_ps SET title=%s, description=%s, language=%s, copyright=%s, pub_date=%s, published_ttl=%s, updated=now() WHERE feedid = %s AND updated < (now() - (7*86400)::reltime)", (title, description, language, copyright, mk_timestamp(pub_date), ttl, feed['id']))
	db.commit()

def DB_update_feed_scan(db, feed):
	#print "db> Updating feed next_scan"
	c = db.cursor()
	c.execute("UPDATE feed SET last_scan=NOW(), next_scan = (NOW() + (%s*(0.5+random()))::int::reltime) WHERE id = %s", (feed['effective_ttl'], feed['id']))
	db.commit()

# !bn: upam da se ti dve funkciji [zgorna, spodna] ne kliceta socasno ?
def DB_update_feed_stat(db, feed, n_e, n_i):
	if n_i == 0: unchanged = feed['unchanged_iter'] + 1
	else: unchanged = 0
	if n_e == 0: overlap = 1
	else: overlap = (n_i+0.0) / float(n_e)
	
	#print "db> Updating feed stat to %d %f" % (unchanged, overlap)
	
	c = db.cursor()
	c.execute("UPDATE feed SET unchanged_iter=%s, last_overlap=%s, found_total=found_total+%s, found_new=found_new+%s WHERE id=%s", (unchanged, overlap, n_e, n_i, feed['id']))
	db.commit()

###

def DB_find_site(db, hostname):
	c=db.cursor()
	c.execute("SELECT * FROM site WHERE hostname = %s LIMIT 1", (hostname,))
	r = DBod(c)
	return r

# !! perf: select po neindexiranem URLju
def DB_find_feed(db, URL):
	c=db.cursor()
	c.execute("SELECT * FROM feed WHERE URL = %s LIMIT 1", (URL,))
	r = DBod(c)
	return r

# !bn:storedproc kandidat
def DB_find_insert_site(db, hostname, feed_site=False, news_site=False):
	assert hostname, "Won't insert NULL into site(hostname). fail."
	site = DB_find_site(db, hostname)
	if not site:
		c = db.cursor()
		c.execute("INSERT INTO site (hostname, is_a_feed_site, is_a_news_site) VALUES (%s,%s,%s)", (hostname,feed_site,news_site))
		db.commit()
		return DB_find_site(db, hostname)
	else:
		if (not site['is_a_feed_site'] and feed_site) or (not site['is_a_news_site'] and news_site):
			c = db.cursor()
			c.execute("UPDATE site SET is_a_feed_site=%s, is_a_news_site=%s WHERE id=%s", 
				(feed_site or site['is_a_feed_site'], news_site or site['is_a_news_site'], site['id']))
			db.commit()
			return DB_find_site(db, hostname)
		return site

def DB_find_insert_feed(db, URL, regex='(.*)', disabled=False, trust_level=2, ftype=None):
	feed = DB_find_feed(db, URL)
	if feed:
		return feed
	else:
		hostname = extract_hostname(URL)
		try:
			site = DB_find_insert_site(db, hostname, feed_site=True)
		except:
			raise
		sid = site['id']
		c = db.cursor()
		c.execute("INSERT INTO feed (siteid, URL, regex, disabled, trust_level, type) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id", (sid, URL, regex, disabled, trust_level, ftype))
		fid = c.fetchone()[0]
		c.execute("INSERT INTO feed_ps (feedid, feedsiteid) VALUES (%s, %s)", (fid, sid))
		db.commit()
		return DB_find_feed(db,URL)

def conv_time(t, default=None):		# assume utc input for now ; !bn: check|fix!
	try:
		return datetime.datetime(*t.updated_parsed[:6], tzinfo=pytz.UTC) if ('updated_parsed' in t and t.updated_parsed) else default
	except:
		return default

# input: feedparsers result
def post_entry(db, feed, entry, acl=None, cutoff_ts = None):
  global title
  try:
    pubdate = conv_time(entry, None)
    
    if cutoff_ts:
      if pubdate == None:
        raise Exception("uberfail!")
      elif pubdate < cutoff_ts:	# strictly lt !
        return (0,0,0,0,1)
    
    if not 'link' in entry or not entry.link:
      print "[%8d] --- faulty link" % (feed['id'],)
      return (1,0,0,0,0)
    
    grs = [x for x in entry.keys() if x.startswith('gml_') or x.startswith('georss_')]
    title = util.textifyHtml(entry.title).replace('\n',' ') if 'title' in entry else None
#    title = (dechrome.parseTitleFromCleartext([title], entry.link) or None) if title else None
    gml = entry.georss_point if ('georss_point' in entry and entry.georss_point) else None
    tags = [x['term'] for x in entry.tags] if 'tags' in entry else None

    # explicitly linked images
    img_links = [link.href for link in getattr(entry,'links',{}) if getattr(link,'type','').startswith('image/') and link.get('href','').strip()]
    img = img_links[0] if img_links else None
    # images emebedded in summary html
    if not img:
       img_links = re.findall('<img[^>]* src="([^"]+)"', getattr(entry,'summary',''))
       img = img_links[0] if len(img_links) == 1 else None
    
    ## fugly hack
    #if grs and len(grs) > 0: fff.write('%s\n' % str(grs))
    #if tags and len(tags) > 0: fff.write('%s\n' % str(tags))
    #if gml: fff.write('gml=%s\n' % str(gml))
    #fff.flush()    
    
    return DB_insert_and_enqueue_article(db, feed, entry.link, pubdate=pubdate, title=title, gml=gml, tags=tags, img=img, acl=acl) + (0,)
  except:
    print traceback.format_exc()
    return (1,0,0,0,0)


prepared.append(({'feedsuck'}, 'check_article', ('text',), "SELECT fa_feed_id AS feedid, fa_id AS id FROM feed_article_urls WHERE url_hash(url) = url_hash($1) AND url=$1"))
#prepared.append(({'feedsuck'}, 'check_article', ('text',), "SELECT feedid, id FROM feed_article WHERE (url_hash(url) = url_hash($1) AND url=$1) or (url_hash(final_url) = url_hash($1) AND final_url=$1)"))
prepared.append(({'feedsuck'}, 'check_article6', ('text',),"""
SELECT fa_feed_id AS feedid, fa_id AS id
FROM feed_article_urls
WHERE 
	month_id(ts) IN (month_id(now()), month_id(now())-1, month_id(now())-2)
	AND
	url_hash6(url) = url_hash6($1)
"""))
# !bn: v razmislek: ne preverja se eksplicitno urlja, ker je 48bit hash .. in ce je v 3 mescih collision .. tough luck.

#prepared.append(({'feedsuck'}, 'check_article6', ('text',),"""
#SELECT feedid, id FROM feed_article
#WHERE 
#	(
#		(month_id(found) in (month_id(now()), month_id(now())-1, month_id(now())-2))
#		and (
#			(
#			url_hash6(url)		= url_hash6($1)
#--			AND url=$1
#			)
#			or 
#			(
#			url_hash6(final_url)	= url_hash6($1)
#--			AND final_url=$1
#			)
#		)
#	)
#"""))

# oba queryja se da zdruzt z OR-om (check6pogoji or check3pogoji) in LIMIT 1; ampak zaenkrat(?) optimizer ne opaz da lahk short-circuita drug del pogojev ce ze dobi hit v check6
# v check6 ne preverjat url string matcha ... sicer nismo nc naredl .. 90% testov je pozitivnih, za kar mora it db v f_a tabelo gledat...
# ^ reasoning fail: db nardi bitmap heap scan recheck condition tud ce ne primerjamo urlja. mogoce samo ne bo urlja iz toasta nalagu, pa dobimo x2 ?
# ce se pa 2 urlja v 3e6 sekundah (34d17) ujemata v 48bit hashu ... sucks to be them.

# vzorc: [--STAT--] c6: 15220, c4: 4171, f:808
#        [--STAT--] c6: 3209916, c4: 849209, f:95274
# pred spremembami feed_articla

d_ca_stat = {'c6': 0, 'c4': 0, 'f':0}
d_ca_n = 0

def DB_check_article(db, URL):
	global d_ca_stat, d_ca_n
	d_ca_n = d_ca_n + 1
	if d_ca_n % 200 == 0: print '[--STAT--] c6: %d, c4: %d, f:%d' % (d_ca_stat['c6'], d_ca_stat['c4'], d_ca_stat['f'])
	c = db.cursor()
	c.execute("EXECUTE check_article6(%s)", (URL,))		# try last 2 months and 6B hash first
	if c.rowcount > 0:					# found a hit already, don't continue
		d_ca_stat['c6'] += 1
		return DBod(c)
	else:
		c.execute("EXECUTE check_article(%s)", (URL,))	# if no match, try the entire database
		if c.rowcount > 0: d_ca_stat['c4'] += 1
		else: d_ca_stat['f'] += 1
		return DBod(c)

## article insert

def check_feed_auth(url, trust_level):
	if not url: return False
	scheme = url[0:4].lower()
	if not scheme == 'http': return False
	suffix = url[-4:]
	if suffix == 'mp3': return False
	if trust_level == 1:
		return True
	elif trust_level == 2:
		return True
	elif trust_level >= 3:
		return False
	else:
		return True

###

def DB_note_overlap(db, lf, rf):
	c = db.cursor()
	c.execute("SELECT * FROM feed_overlap where lfeed=%s AND rfeed=%s", (lf, rf))
	if c.rowcount == 0:
		c.execute('INSERT INTO feed_overlap (lfeed, rfeed) VALUES (%s, %s)', (lf,rf))
	else:
		c.execute('UPDATE feed_overlap SET count = count + 1 WHERE lfeed=%s AND rfeed=%s', (lf,rf))
	db.commit()

# !bn: resn kandidat za stored proceduro
def DB_find_insert_article(db, feed, URLm, title=None, pubdate=None, gml=None, tags=None, img=None, acl=None, enqueue=True, provider_site=None):
	feed_id = feed['id']
	feed_site_id = feed['siteid']
	rx = feed['regex']
	rdecode = feed['parse_unquote']
	#print "feed:%d, regex: %s" % (feed_id, rx)
	URL = URLm
	try:
		URL = re.compile(rx).match(URLm).group(1)
	except:
		# !bn: mal 'na suho' -- ce ne rata, pa pac ne ...
		pass
	#print "inserting real url: %s" % URL
	if rdecode: URL = urllib.unquote(URL)
	
	if len(URL) > 4095:
		db.commit()
		return (1,0,0,0)	# fail

	art=DB_check_article(db, URL)
	if art:
		lf = art['feedid']
		rf = feed['id']
		if lf == rf: return (0,1,0,0)	# same feed
		if lf > rf: lf,rf = rf,lf
		DB_note_overlap(db, lf, rf)
		db.commit()
		return (0,0,1,0)			# other feed
	
	try:
		# !bn: se en lookup po bazi
		hostname = extract_hostname(URL)
		site = DB_find_insert_site(db, hostname)
	except:
		db.rollback()
		print URLm, " is fucked url."
		return (1,0,0,0)
	
	c = db.cursor()
	c.execute("INSERT INTO feed_article (siteid, feedsiteid, feedid, enqueued, acl_tagset, publisher_siteid) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id", (site['id'], feed_site_id, feed_id, enqueue, acl, provider_site))
	artid = c.fetchone()[0]
	c.execute("INSERT INTO feed_article_urls (fa_id, fa_feed_id, url) VALUES (%s, %s, %s)", (artid, feed_id, URL))
	c.execute("INSERT INTO feed_article_meta (id, title, pub_date, tags, geo, img) VALUES (%s, %s, %s, %s, %s, %s)", (artid, title, pubdate, tags, gml, img))
	#print URL, c.query
	db.commit()
	
	return (0,0,0,1)				# added

def DB_insert_and_enqueue_article(db, feed, URL, title=None, pubdate=None, gml=None, tags=None, img=None, acl=None):
	if not check_feed_auth(URL, feed['trust_level']): raise Exception("Wrong trust level.")
	return DB_find_insert_article(db, feed, URL, title=title, pubdate=pubdate, gml=gml, tags=tags, img=img, acl=acl)

#!bn: TODO: kaj ce insertamo dva articla k mata enak final_url? (oz en 'final', drug 'one-and-only') .. zaenkrat nobenga to ne mot. fail!
def DB_post_article(db, art_id, art_feedid, art_feedsiteid, art_siteid, url, final_url, max_fau_seq, code, header, content, mode = None):
	assert mode==None

	c = db.cursor()
	
	# check for collitions.
	# final_url in f_a_u.url where not f_a_u.fa_id != art_id
	# if match, merge, otherwise, insert
	
	try:
		c.execute("INSERT INTO article (feed_articlefeedid, feed_articlefeedsiteid, feed_articlesiteid, feed_articleid, HTTP_code, header, content) VALUES (%s,%s,%s,%s,%s,%s,%s)", 
			(art_feedid, art_feedsiteid, art_siteid, art_id,
			 code, header.decode('ascii', 'replace'), psycopg2.Binary(content)))
	except:
		# poskusl insertat ze obstojec article in dobil IntegrityError: Key exists
		db.rollback()
		DB_log(db, art_siteid, url, 902, -1)
		return
		
#	c.execute("UPDATE feed_article SET final_url=%s, enqueued=false WHERE id=%s", (final_url, art_id))	# !bn: merge: update statistike, itn.
	c.execute("UPDATE feed_article SET enqueued=false WHERE id=%s", (art_id,))	# !bn: merge: update statistike, itn.
	if final_url is not None and not final_url == url:
		c.execute("INSERT INTO feed_article_urls (seq, fa_id, fa_feed_id, url) VALUES (%s, %s, %s, %s)", (max_fau_seq+1, art_id, art_feedid, final_url))
	db.commit()
	
	c.execute("UPDATE feed_article_meta SET is_http_error=%s WHERE id = %s", (int(code) >= 400, art_id))
	
	if c.rowcount == 0:
		print "db> could not update feed_article_meta, trying insert."
		db.rollback()
		c.execute("INSERT INTO feed_article_meta (id, is_http_error) VALUES (%s, %s)", (art_id, int(code) >= 400))
	
	if int(code) < 400: c.execute("SELECT nextval('articles_downloaded')")
	db.commit()

def DB_post_cleaned(db, art_id, art_feedid, art_feedsiteid, art_siteid, cleaned):
	c = db.cursor()
#	c.execute("INSERT INTO article (feed_articlefeedid, feed_articlefeedsiteid, feed_articlesiteid, feed_articleid, HTTP_code, header, content) VALUES (%s,%s,%s,%s,%s,%s,%s)", 
#		(art_feedid, art_feedsiteid, art_siteid, art_id,
#		 code, header.decode('ascii', 'replace'), psycopg2.Binary(content)))
#	c.execute("UPDATE feed_article SET final_url=%s, enqueued=false, locked=false WHERE id=%s", (final_url, art_id))
	c.execute("INSERT INTO processed_article (feed_articleid, mode, content) VALUES (%s,%s,%s)", (art_id, "cleartext", cleaned))
	db.commit()

def DB_unlock_cleaning(db, artid):
	return		# just don't.
	c = db.cursor()
	c.execute("UPDATE feed_article_meta SET locked=false WHERE id = %s", (artid,))
	db.commit()

def DB_retry_article(db, fa_id):
	c = db.cursor()
	c.execute("UPDATE feed_article SET last_attempt=NOW(), next_attempt=NOW()+1200::reltime WHERE id=%s", (fa_id,))
	db.commit()

###

def DB_site_access_unlock(db, site_id):
	c = db.cursor()
	c.execute("UPDATE site SET last_request=NOW(), next_request=NOW()+request_interval, locked=false WHERE id = %s", (site_id,))
	db.commit()

###

def DB_tag_feed(db, art_id, art_feedid, art_feedsiteid, art_siteid, name, value):
	c = db.cursor()
	c.execute("INSERT INTO feed_tags (feed_articleid, feed_articlefeedid, feed_articlefeedsiteid, feed_articlesiteid, name, value) VALUES (%s, %s, %s, %s, %s, %s)",
		(art_id, art_feedid, art_feedsiteid, art_siteid, name, value))
	db.commit()

# !bn: site je treba dat kot spremenljivko, ker jo zihr mamo...
def DB_log(db, siteid, URL, code, size):
	c = db.cursor()
	c.execute("INSERT INTO access_log (siteid, URL, HTTP_code, size) VALUES (%s,%s,%s,%s)", (siteid, URL, code, size))
	db.commit()

###

def DB_unlock_sites(db):
	DB_exec(db, "UPDATE site SET locked=false WHERE locked")

# !bn: pogoj "enqueued AND " dodan zato ker drugac nardi seq. scan cez celo tabelo
#      itak hocmo unlockat samo taksne ki dejansko so enqueueani...
def DB_unlock_feedarticles(db):
	return
	DB_exec(db, "UPDATE feed_article SET locked=false WHERE enqueued AND locked")

SQL_FIND_REQUESTS = """
SELECT MAX(feed_article.id) AS min_fa_id, site.id as siteid
	INTO TEMPORARY temp_requests
	FROM feed_article
	INNER JOIN site ON feed_article.siteid = site.id
	WHERE
		feed_article.rndpos BETWEEN %s AND %s
		AND NOT site.disabled
		AND NOT site.locked
		AND site.next_request < NOW()
		AND feed_article.enqueued
		AND NOT feed_article.locked
		AND feed_article.next_attempt < NOW()
	GROUP BY site.id
	ORDER BY RANDOM()
	LIMIT %s
"""
# 			ref_url, rq_url, art_id, art_feedid, art_feedsiteid, art_siteid
SQL_RETR_REQUESTS="""
SELECT f.URL, a.URL, a.id, a.feedid, a.feedsiteid, a.siteid
	FROM feed_article AS a
	INNER JOIN temp_requests AS t 		ON a.id = t.min_fa_id
	INNER JOIN feed AS f 			ON a.feedid = f.id
"""
SQL_LOCK_REQUESTS="UPDATE feed_article SET locked=true WHERE id IN (SELECT min_fa_id FROM temp_requests)"
SQL_LOCK_SITES="UPDATE site SET locked=true WHERE id IN (SELECT siteid FROM temp_requests)"
SQL_DROP_TEMPORARY="DROP TABLE temp_requests"

def DB_find_requests(db, N_req, rndp_eps = 0.001):
	c = db.cursor()
	rndp = random.random()
	c.execute(SQL_FIND_REQUESTS, (rndp - rndp_eps, rndp + rndp_eps, N_req))
	c.execute(SQL_RETR_REQUESTS)
	r = c.fetchall()
	c.execute(SQL_LOCK_REQUESTS)
	c.execute(SQL_LOCK_SITES)
	c.execute(SQL_DROP_TEMPORARY)
	db.commit()
	return r
