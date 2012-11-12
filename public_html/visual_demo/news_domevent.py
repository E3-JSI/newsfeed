#!/usr/bin/env python

import zmq
import cgi, cgitb
import json
import sys
import time

def main():
  cgitb.enable()
  
  zmqctx = zmq.Context()
  zmqsock = zmqctx.socket(zmq.SUB)
  zmqsock.setsockopt(zmq.HWM, 100)
  #zmqsock.setsockopt(zmq.IDENTITY, 'web-dom-event')
  zmqsock.setsockopt(zmq.SUBSCRIBE, '')
  zmqsock.connect('tcp://maximus.ijs.si:1236')
  
  print "Content-Type: text/event-stream\n"
  sys.stdout.flush()
  
  while True:
    evt = zmqsock.recv_json()
    print "event: news-event"
    # print "id: " -- ce pade konekcija dobimo Last-Event-ID header s tem idjem.
    #print "data: {aid: %s, title: %s, tags: %s, geo: %s}" % (evt[0], evt[1], evt[2], evt[3])
    print "data: %s\n" % json.dumps(evt)
    sys.stdout.flush()


if __name__ == '__main__':
  main()
