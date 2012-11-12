#!/usr/bin/env python

"""
Listens to db NOTIFYs to find freshly cleartexted articles.
Fetches extended info about each article from the DB and publishes
everything on zmq.

Subscriber: zmq2zmq_enrych.py
"""

import sys; sys.path.append('..')
sys.path.append('../langdet'); import iso_map
import psycopg2
import select
import zmq
import traceback
from urlparse import urlparse

def lat_lon_dict(coords):
	"""
	Converts '12.53 44.1' -> {'lat':12.53, 'lon':44.1).
	For None or invalid input, returns an empty dict.
	"""
	if coords is None: return {}
	try:
		lat, lon = map(float, coords.split(';')[0].split())
		return {'lat':lat, 'lon':lon}
	except:
		traceback.print_stack()
		print 'WARNING: Unknown geo coords expression: %r' % coords
		return {}

def kill_none_vals(d):
	"""
	Remove (in-place!) all keys from dict `d` which have a value of None.
	"""
	for k in d.keys():
		if d[k] is None: del d[k]
	
def DB_get_full_article(cur, article_id):
	"""
	Return a dict with "all" the end-user-relevant info about article `article_id`.
	If the ID does not exist or is not cleartexted yet, return None.
	See source code for dict keys.

	WARNING: the whole zmq pipeline represents articles as row-dicts from this query,
	so be careful when changing column names.

	WARNING 2: commits the connection belonging to `cur`.
	"""
	# If publisher_siteid is not given and the site hostname and article's URL disagree, deduce the publisher.
	cur.execute("""
		SELECT publisher_siteid, siteid, site.hostname AS site_hostname, (SELECT url FROM (
			SELECT *, row_number() OVER (ORDER BY seq DESC) rank
			FROM feed_article_urls WHERE fa_id=%s) urls
			WHERE urls.rank = 1) AS final_url
		FROM feed_article JOIN site ON (feed_article.siteid=site.id) WHERE feed_article.id=%s""", (article_id, article_id))
	row = cur.fetchone()
	hostname = urlparse(row['final_url']).netloc
	if hostname != row['site_hostname'] and row['publisher_siteid'] is None:
		cur.execute("UPDATE feed_article SET publisher_siteid=(SELECT id FROM site WHERE hostname=%s) WHERE id=%s RETURNING id, publisher_siteid", (hostname, article_id,))
		newrow = cur.fetchone()
		print 'Updated: %s now has publisher_siteid %s (=%s; siteid is %s=%s)' % (newrow['id'], newrow['publisher_siteid'], hostname, row['siteid'], row['site_hostname'])
		cur.connection.commit()

	cur.execute("""
		SELECT
			-- == ARTICLE == --
			fa.id AS id,
			(SELECT url FROM (
				SELECT *, row_number() OVER (ORDER BY seq DESC) rank
				FROM feed_article_urls WHERE fa_id=%s) urls
				WHERE urls.rank = 1) AS url,
			meta.pub_date AS publish_date,
			fa.found AS found_date,
			article.retrieved AS retrieved_date,
			meta.title AS title,
			proc.content AS cleartext,
			COALESCE(meta.lang_iso, fps.language) AS lang,
			meta.img AS img,
			meta.tags,
			meta.geo,
			-- == SOURCE (i.e. SITE) == --
			site.hostname AS source_hostname,
			site.name AS source_name,
			site.tags AS source_tags,
			site_tld_country.name AS _source_tld_country, -- for postprocessing only; not exposed to zmq
			site_tld_country.geo AS _source_tld_geo,      -- for postprocessing only; not exposed to zmq
			-- == FEEDS == --
			feed.url AS feed_url,
			COALESCE(fps.manual_title, fps.title) AS feed_title,
			-- == OTHER == --
			fg.story_id AS google_story_id,
			fa.acl_tagset
		FROM
			feed_article fa
			LEFT OUTER JOIN site ON (COALESCE(fa.publisher_siteid, fa.siteid) = site.id)
			LEFT OUTER JOIN feed ON (fa.feedid = feed.id)
			LEFT OUTER JOIN feed_ps fps ON (fps.feedid = feed.id)
			LEFT OUTER JOIN feed_article_meta meta ON (fa.id = meta.id)
			LEFT OUTER JOIN processed_article proc ON (proc.feed_articleid = fa.id AND proc.mode='cleartext')
			LEFT OUTER JOIN article ON (article.feed_articleid = fa.id)
			LEFT OUTER JOIN feed_article_googles fg ON (fg.feed_articleid = fa.id)
			LEFT OUTER JOIN country site_tld_country ON (site.tld=site_tld_country.iso3166_1)
		WHERE
			fa.id = %s
			--AND proc.content != ''
			--AND proc.content IS NOT NULL
	""", (article_id, article_id,))

	try:
		if not cur.rowcount:
			return None

		else:
			ret = dict(cur.fetchone())
	
			# normalize the language if coming from feed_ps (hack for old entries; should be three-letter iso in the future)
			ret['lang'] = (ret['lang'] or '').split('-')[0]
			if ret['lang'] not in iso_map.iso3to2:
				ret['lang'] = iso_map.iso2to3.get(ret['lang'], None)

			# Drop the first line of cleartext which is a copy of the title for historical reasons. Also leave an empty first line in case people drop it later.
			if ret['cleartext']:
				ret['cleartext'] = '\n'+'\n'.join(ret['cleartext'].splitlines()[1:])
			
			# Normalize tags (remove empty tags, create empty list if necessary)
			ret['tags'] = [t for t in (ret['tags'] or []) if t]
			ret['source_tags'] = [t for t in (ret['source_tags'] or []) if t]

			# No ACL tags implicitly means "only the 'public' tag"
			if not ret['acl_tagset']:
				ret['acl_tagset'] = ['public']
			if ret['google_story_id'] and ret['lang']=='eng':
				ret['acl_tagset'].append('render')

			# Parse the tags to extract geo information for the publisher/source
			ret['source_geo'] = lat_lon_dict(ret['_source_tld_geo'])   # to be overwritten
			ret['source_geo']['city'] = None                           # to be overwritten
			ret['source_geo']['country'] = ret['_source_tld_country']  # to be overwritten

			source_tags_dict = dict(tag.split(':') for tag in (ret['source_tags'] or []) if tag.count(':')==1)
			ret['source_geo'].update(lat_lon_dict(source_tags_dict.get('geo') or None))
			ret['source_geo']['city'] = source_tags_dict.get('city')
			ret['source_geo']['country'] = source_tags_dict.get('country')
			ret['source_tags'] = [t for t in ret['source_tags'] if not t.startswith('city:') and not t.startswith('country:') and not t.startswith('region:') and not t.startswith('geo:')]
			kill_none_vals(ret['source_geo'])

			# Parse the geo tag for the article
			ret['geo'] = [lat_lon_dict(coords) for coords in ret['geo'].split(';')]  if ret['geo']  else []
			ret['geo'] = [x for x in ret['geo'] if x]
			map(kill_none_vals, ret['geo'])
			
			return ret
		
	finally:
		cur.connection.commit()

			
def main():
	global article, conn
	from cleanDb import openConnection
	conn, cur = openConnection('cleartext feed')
	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)	# disable transactions, so we get notifys realtime
	cur_listen = conn.cursor()
	cur_listen.execute("LISTEN have_cleartext")
  
	zmqctx = zmq.Context()
	sock = zmqctx.socket(zmq.PUB)
	sock.setsockopt(zmq.HWM, 100)
	sock.bind('tcp://*:13371')

	try:
		while True:
			if select.select([conn],[],[],5) == ([],[],[]):
				print '(nothing to do)'
			else:
				conn.poll()
				notifies = conn.notifies
				while notifies:
					notify = notifies.pop()
					article_id = int(notify.payload)
					try:
						article = DB_get_full_article(cur, article_id)
						if article is None:
							print "skipping %s (not found)" % article_id
							continue
						elif not article['cleartext']:
							print "skipping %s (no cleartext)" % article_id
							continue
						sock.send_pyobj(article)
						print "ok %s" % article_id + ('(old)' if article['found_date'].year<2012 else '')
					except:
						print "!!! error while processing %s" % article_id
						traceback.print_exc()
	except:
		traceback.print_exc()
		return
	finally:
		sock.close()
		zmqctx.term()

def test():
	from pprint import pprint
	import psycopg2, psycopg2.extras
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
	conn = psycopg2.connect(database='news', host='maximus', user='mitjat', password='XXX_GITHUB_XXX')
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute("SET bytea_output TO 'escape'")

	pprint(DB_get_full_article(cur,66748005))
		
if __name__ == '__main__':
	if sys.argv == ['-c']:
		test()
	else:
		main()
	
