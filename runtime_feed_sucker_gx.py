#!/usr/bin/env python
#-*- indent-tabs-mode: nil -*-

"""
  async version of feed sucker, gevent version, now with concurrent db fetch!
  
  while true:
  every once in a while, submit db->feed_queue thread; make sure to remove duplicates
    (select one most eager feed for every site)
  submit work to greenlet.Pool
  store results
  
"""

import gevent, gevent.monkey, gevent.pool, gevent.queue
import gevent_psycopg2
gevent.monkey.patch_all()
#gevent.monkey.patch_socket()
gevent_psycopg2.monkey_patch()
import common
import feedparser
import socket
import time, datetime, pytz
import sys
import traceback
import pdb
import re
import libxml2
import random

SOCK_TIMEOUT = 10		# no point in waiting ...
N_THREADS = 257
IDLE_WAIT = 100
FEED_GET_INTERVAL = 60
FEED_GET_QUERY = "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY siteid ORDER BY next_scan) FROM feed WHERE NOT disabled AND next_scan < now()) subq WHERE row_number < 2"

#fff = open('feed_er.log','w')

# prevent excessive load times of external entity defs from w3.org
def dont_load_external(URL, ID, context):
  print '**** tried to load', URL
#  fff.write('%s %s\n' % (str(time.time()), URL))
#  fff.flush()
  return ''
#libxml2.setEntityLoader(dont_load_external)

def suck_feed(feed, Q):
  try:
    print "[%8d] starting suck <%s>" % (feed['id'], feed['url'])
    # !bn: TODO: kaj se zgodi ce dobimo etag-match? upam da ne kill feed?
    #  -- result.status == 304, entries = [], etag=NULL !
    et = str(feed['last_etag']) if 'last_etag' in feed else ''
    data = feedparser.parse(feed['url'], etag=et)
    print "[%8d] parsed" % (feed['id'],)
    Q.put((feed, data, None))
  except:
    Q.put((feed, None, traceback.format_exc()))
  #print 'end suck feed'

fuckups = 0

# feed.pruning_mode:
#  NULL = normal: don't
#  '0' = normal: don't, determined by learning
#  'L' = learning mode -- discover rule
#  'D' = input dataset is sorted by time, descending, with no missing pubdates
#  'A' = "" ascending
#  'X' = no rule found
#  'Y' = rule verification failed

stats_sum = (0,)*5

def process_result(db, feed, result, exception):
  global fuckups
  global stats_sum
  
  print "[%8d] processing result. [%s] <%s>" % (feed['id'], feed['pruning_mode'], feed['url'].encode('ascii', 'replace'))
  try:
    if exception:			# something awful happened
      common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 979, exception)
      common.DB_disable_feed(db, feed['id'], now=True, flag=40)
    elif not result:		# something possibly even more awful might have happened
      common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 978, '')
      common.DB_disable_feed(db, feed['id'], now=True, flag=41)
      #pdb.set_trace()
    elif 'status' not in result:	# how did that happen ? O.o
      common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 977, '')
      common.DB_disable_feed(db, feed['id'], now=True, flag=42)
      #pdb.set_trace()
    elif 'feed' not in result:	# how did that happen ? O.o!!
      print 'FFFUUUUU!'
      common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 967, '')
      common.DB_disable_feed(db, feed['id'], now=True, flag=46)
      #pdb.set_trace()
    elif result['status'] >= 400:	# feed gone
      common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 976, result['status'])
      common.DB_disable_feed(db, feed['id'], now=True, flag=43)
      #pdb.set_trace()
    elif 'entries' not in result:
      common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 975, result['status'])
      common.DB_disable_feed(db, feed['id'], now=True, flag=44)
      #pdb.set_trace()
    else:
      # completely skip 304 responses; DO NOT update etag in DB: some 304 responses don't echo etag
      if not result['status'] == 304:

        # implement entry submission pruning here.
        # feed is updated 3x in this block. fix me maybe?
        #
        # poskusmo najdt za zacetk samo time-ordered-descending (D)
        # ce so prisotni VSI pubdateji, in ce so VSI urejeni >=
        #
        # ce pise v dbju 'L', vpisemo v DB 'D' 
        # ce pise v dbju 'D', insertamo samo tiste entryje k so >= od zadnga timestampa
        
        # wtf: povsod uporabljamo pytz.UTC; v db se inserta ___+02. pravilno povecan.
        # psycopg2 screwy?

        gt = lambda x,y: x>=y
        issorted = lambda u: all(map(gt, u[:-1], u[1:]))
        mktime = lambda e: common.conv_time(e, datetime.datetime(1,1,1,tzinfo=pytz.UTC))
        
        foofeed = any('updated_parsed' not in e for e in result.entries)
        stamped_entries = [mktime(e) for e in result.entries]
        if any(e > datetime.datetime.now(tz=pytz.UTC) for e in stamped_entries): foofeed = True
        latest_entry_ts = max(stamped_entries) if len(stamped_entries) > 0 else datetime.datetime(1,1,1,tzinfo=pytz.UTC)
  
        # learning
        if feed['pruning_mode'] == 'L':
          cur = db.cursor()
          if foofeed:
            print '[%8d] +++ setting feed to pruning: DISABLED.' % (feed['id'],)
            cur.execute("UPDATE feed SET pruning_mode='0' WHERE id=%s", (feed['id'],))
          elif issorted(stamped_entries):
            print '[%8d] +++ setting feed to pruning: time sorted descending.' % (feed['id'],)
            cur.execute("UPDATE feed SET pruning_mode='D' WHERE id=%s", (feed['id'],))
          elif issorted(list(reversed(stamped_entries))):
            print '[%8d] +++ setting feed to pruning: time sorted ascending.' % (feed['id'],)
            cur.execute("UPDATE feed SET pruning_mode='A' WHERE id=%s", (feed['id'],))
          else:
            cur.execute("UPDATE feed SET pruning_mode='X' WHERE id=%s", (feed['id'],))
          db.commit()
        
        # verification
        prune = '0'
        if feed['pruning_mode'] == 'D':
          if issorted(stamped_entries) and not foofeed: prune = 'D'
          else:
            cur = db.cursor()
            cur.execute("UPDATE feed SET pruning_mode=%s WHERE id=%s", ('Y' if not foofeed else 'F', feed['id'],))
            db.commit()
        elif feed['pruning_mode'] == 'A':
          if issorted(list(reversed(stamped_entries))) and not foofeed: prune = 'A'
          else:
            cur = db.cursor()
            cur.execute("UPDATE feed SET pruning_mode=%s WHERE id=%s", ('Y' if not foofeed else 'F', feed['id'],))
            db.commit()
        
        R = [common.post_entry(db, feed, entry, acl=None, cutoff_ts=feed['pruning_ts_last'] if prune in ('A', 'D') else None) for entry in result.entries]
        
        stats = map(sum,zip((0,0,0,0,0), *R))
        stats_sum = map(lambda x,y:x+y, stats_sum, stats)
        n_new = stats[3]
        common.DB_update_feed_stat(db, feed, len(result.entries), n_new)	# feed(n_i, n_e, total, total_new)
        common.DB_update_feed(db, feed, result, latest_entry_ts)					# feed_ps, feed(etag, failures), feed(pruning_ts_last)
        
        common.DB_log_feed_suck(db, feed['id'], feed['siteid'], result['status'], n_e=len(result.entries), n_i=n_new, unchanged=feed['unchanged_iter'])
        
        if result.bozo and result.bozo==1 and result.bozo_exception:
          common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 973, str(result.bozo_exception))
        
        print "[%8d] suck complete with (%d/%d/%d/%d/%d/%d) -> (%d/%d/%d/%d/%d)" % ((feed['id'],) + tuple(stats) + (len(result.entries),) + tuple(stats_sum))
      else:
        print '[%8d] feed unchanged since last scan' % (feed['id'],)
      
    common.DB_update_feed_scan(db, feed)		# nextscan = now + ttl*rnd
  except:
#    sys.exit(-1)
    raise
    common.DB_log_feed_suck(db, feed['id'], feed['siteid'], 974, traceback.format_exc())
    common.DB_disable_feed(db, feed['id'], now=True, flag=45)
    #pdb.set_trace()
  db.commit()

def process_results(db, Q):
  c = 0
  for result in iter(Q.get, StopIteration):
    c += 1
    process_result(db, *result)
  return c

def fetch_potential_feeds(db, work):
  print "fetching feed list."

  cur = db.cursor()
  cur.execute(FEED_GET_QUERY)
  feeds = cur.fetchall()
  db.commit()
  
  print "submitting new work"

  old_fids = { feed['id'] for feed in work[0] }
  new_fids = { feed['id'] for feed in feeds }

  new_useful_feeds = new_fids - old_fids
  work[0].extend(feed for feed in feeds if feed['id'] in new_useful_feeds)
  
  random.shuffle(work[0])	# randomize scan order
  
  work[1] = time.time()
  print "db fetch done"
  

def main():
  socket.setdefaulttimeout(SOCK_TIMEOUT)
  db = common.DB_connect('feed sucker [gx]')
  dbd = common.DB_connect('feed sucker [gx:discovery]')
  cur = db.cursor()
  common.DB_prepare(db, {'feedsuck'})
#  cur.execute('UPDATE feed SET next_scan = now() + (effective_ttl * random())::integer::reltime')
  db.commit()

  work = [[], 0]	# feed list, last fetch
  
  Q = gevent.queue.Queue(maxsize=0)	# functions as a channel: put blocks until get
  work_pool = gevent.pool.Pool(size=N_THREADS)
  workers = []

  dbw = gevent.spawn(process_results, db, Q)

  while True:
    if (work[1] + FEED_GET_INTERVAL) < time.time():
      # submit db fetch work
      work[1] = time.time()	# don't spawn it next time around the loop..
      workers.append(work_pool.spawn(fetch_potential_feeds, dbd, work))
    else:
      # submit some normal work
      if len(work[0]) == 0:
        time.sleep(5)
        continue
      else:
        print "submitting work; queue size = %d, active workers = %d" % (len(work[0]),len(workers))
        workers.append(work_pool.spawn(suck_feed, work[0].pop(), Q))
    
    # housekeeping
    active_workers = []
    for worker in workers:
      if worker.ready():
        worker.join()
      else:
        active_workers.append(worker)
    workers = active_workers
        
    # catch Ctl-C, put StopIteration, etc..

#    greenlets = [work_pool.spawn(suck_feed, feed, Q) for feed in feeds]
#    gevent.joinall(greenlets)
#    Q.put(StopIteration)
#    dbw.join()
    

if __name__ == '__main__':
  #import cProfile
  #cProfile.run('main()')
  main()
