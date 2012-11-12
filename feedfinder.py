#!/usr/bin/env python

import common

import lxml
import lxml.etree
import lxml.html
import sys
import os
import urllib2
import StringIO
import urlparse
import socket

stat = { 'docs':0, 'found':0, 'added':0, 'skipped':0, 'faildocs': 0, 'serfail': 0 }

log = open(('feedfinder-%d.log' % os.getpid()), 'w')
oklog = open(('feedfinder-ok-%d.log' % os.getpid()), 'w')

def sanitize(s):
    return s.encode('ascii','replace')

def src_db():
    """
    fetch pages from database, return (url, page)
    """
    db = common.DB_connect()
    cur = db.cursor()
    
    print 'creating cursor for random page get @ 17 pages / site. wait 10 min for first page :)'
    cur.execute("SET bytea_output TO 'escape'")		# psycopg2 na koperniku ne razume pg9+ binary podatkov
    cur.execute("SET random_page_cost=0.2")
    # !bn: todo: where rn > rowcount - 17 ?
    # tko da zadnje extractas.
    cur.execute("DECLARE foo no scroll cursor for SELECT (CASE WHEN (f.final_url IS NULL) THEN f.url ELSE f.final_url END) AS url, a.content FROM (SELECT f.id as id, row_number() OVER (PARTITION BY f.siteid ORDER BY f.found DESC) AS rn FROM feed_article f INNER JOIN feed_article_meta m ON m.id = f.id WHERE m.is_utf8 = '1') i INNER JOIN article a ON i.id = a.feed_articleid INNER JOIN feed_article f ON f.id = i.id WHERE rn < 17")
    print 'cursor created, start working.'
    
    while True:
        cur.execute('FETCH foo')
        if cur.rowcount == 0: return
        url,page = cur.fetchone()
        try:
            oklog.write(sanitize(('%s\n' % url)))
            oklog.flush()
        except:
            pass
        yield url, page

def src_web(urllist, skip=0):
    """
    fetch a list of urls from urllist, download each one, return (url, page)
    """
    urls = [x.strip() for x in open(urllist).readlines()]
    
    for url in urls[skip:]:
        try:
            u = urllib2.urlopen(url)
            page = u.read()
            yield url, page		# url is not normalized in any way!
        except:
            pass

def process_page(db, cur, dbfz, url, page):
    print 'processing', url
    
    stat['docs'] += 1
    
    #
    # !bn: pazi: custom html parser z utf-8 encodingom dela ok na StringIO(utf-8 str)
    # AMPAK, article.content je utf-8 str (oz buffer), k ga je treba najprej .decode('utf-8')
    # za obicajno uporabo!
    #
    
    parser = lxml.html.HTMLParser(encoding='utf-8')
    
    try:
        p = lxml.etree.parse(StringIO.StringIO(page), parser)
        links = p.xpath("//link[@rel='alternate']")
        lhr = [l.attrib['href'] for l in links if 'href' in l.attrib]
    except:
        try:
            log.write(sanitize(('par: %s\n' % url)))
            log.flush()
        except:
            pass
        return
    
    if len(links) == 0:
        if 'rss' in page or 'RSS' in page:
            stat['faildocs'] += 1
            print '... has some sort of fail RSS.'
            ass = p.xpath('//a')
            for a in ass:
                try:
                    sa = lxml.etree.tostring(a)
                    if 'rss' in sa or 'RSS' in sa:
                        if 'href' in a.attrib:
                            lhr.append(a.attrib['href'])
                except:
                    stat['serfail'] += 1
                    try:
                        log.write(sanitize(('ser: %s\n' % url)))
                        log.flush()
                    except:
                        pass
    
    stat['found'] += len(lhr)
    
    # normalize hrefs
    feeds = [urlparse.urljoin(url, h) for h in lhr]
    for feed in feeds:
        if feed in dbfz:
            stat['skipped'] += 1
            continue
        dbfz.add(feed)
        try:
            common.DB_find_insert_feed(db, feed, disabled=False, trust_level=99, ftype='PARSED')
            db.commit()
        except:
            try:
                log.write(sanitize(('fif: %s\n' % url)))
                log.flush()
            except:
                pass
        stat['added'] += 1
    
    print stat

def main():
    socket.setdefaulttimeout(20)

    db = common.DB_connect()
    cur = db.cursor()

    if sys.argv[1] == 'db': src = src_db()
    else: src = src_web(sys.argv[1], int(sys.argv[2]))

    # use dbfz as rss url lookup set to decrease load on the database
    print 'selecting existing urls'
    cur.execute('SELECT url FROM feed')
    dbfz = {x[0] for x in cur.fetchall()}

    for url, page in src:
        process_page(db, cur, dbfz, url, page)

if __name__ == '__main__':
    main()
