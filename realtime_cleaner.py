#!/usr/bin/env python

import zmq
import time
from cleanDb import *
import signal, termios, fcntl

def main():
  db, cur = openConnection('realtime cleaner')
  db.autocommit = True  # we don't need transactions
  
  zmqctx = zmq.Context()
  zmqsock = zmqctx.socket(zmq.PULL)
  zmqsock.setsockopt(zmq.HWM, 20)
  zmqsock.connect('tcp://*:1234')

  info('loading langdet_nb.pck ...')
  D = langdet.load_langdet_db('langdet/langid_nb.pck')
  info('... done')
  
  while True:
    id, headers, page = zmqsock.recv_multipart()
    articleId = int(id)

    print 'got article', id
    try:
      #print "starting decode"
      utf8 = handleSingleDecode(cur, articleId, headers, txt=page)
      cleartext = handleSingleCleartext(cur, articleId, html=str(utf8), commit=False)
      lc_iso, lc_alt = handleSingleLangdet_cld(cur, articleId, text=cleartext, commit=False)
      if lc_iso is None:
        used_blazn = True
        lc_iso, lc_alt = handleSingleLangdet(cur, D, articleId, text=cleartext, ignore_cld_langs=True, commit=False)
      else:
        used_blazn = False
      cur.connection.commit()  # make cleartext and language known to the outside world at the same time
      print "  %5d bytes   %s %s   %s" % (
        len(cleartext), lc_iso, ('nonCLD' if used_blazn==True else '      '), cleartext[:_TERMINAL_WIDTH-30].encode('utf8', 'replace').replace('\n',' '))
    except NoTextFoundError:
      print "  (empty)"
    except ProcessingError:
      print "  ProcessingError:"
      print '\n'.join('  '+line for line in traceback.format_exc().splitlines())
    except:
      print "some exception"
      print traceback.format_exc()
      pass
    
    try:
       cur.execute("NOTIFY have_cleartext, '%s'", (articleId,))   # !bn: replace with "SELECT pg_notify('have_cleartext',id);" in a trigger
    except psycopg2.InterfaceError, e:
       print 'DB exception. Traceback follows. Sleeping for a minute, then reconnecting.'
       traceback.print_exc()
       time.sleep(60)
       try: db.close()
       except: pass
       try: db, cur = openConnection('realtime cleaner')
       except: pass
       
def handle_resize(signum, frame):
	"Update global variable _TERMINAL_WIDTH on SIGWINCH"
	global _TERMINAL_WIDTH
	try:
		h, w = map(int, os.popen('stty size', 'r').read().split())
	except:
		h, w = 25, 80
	_TERMINAL_WIDTH = w
		 
if __name__ == '__main__':
  #signal.signal(signal.SIGWINCH, handle_resize)  # zmq doesn't like it
  handle_resize(None, None)
  main()
