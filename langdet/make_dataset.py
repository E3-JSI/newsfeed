#!/usr/bin/env python

import sys
sys.path.append('..')
import cleanDb
import langdet
from collections import defaultdict
import re
import codecs

"""
 create a trigram dataset for selected languages
 use site tlds, exclude articles with predicted {eng,enz,fr,gl*,pt,de,ca,lad,ptb{pt:brasil},su,ru,unk,ar,it}
 except for 'es' dataset -- don't exclude gl.
 
 params: target dir, langset: {lang:tld,tld,tld,...}
"""

excluded_languages = {'eng','enz','fr','es','gl','pt','de','ca','lad','ptb','su','ru','ar','it'}	# to morajo bit lang_altcode
known_fails = {'es':{'gl'}}
Q = "SELECT string_agg(content, '') FROM (select content from processed_article p inner join feed_article_meta m on m.id=p.feed_articleid inner join feed_article a on m.id = a.id inner join site s on s.id = a.siteid where s.tld in %(tlds)s and m.lang_altcode not in %(excluded)s limit %(limit)s) AS foo"

def trigrams(l):
 for tgm in [l[i:i+3] for i in range(len(l)-2)]:
   if tgm[1] == ' ': continue
   tgm = ('<' if tgm[0] == ' ' else tgm[0]) + tgm[1] + ('>' if tgm[2] == ' ' else tgm[2])
   yield tgm

def main():
  db,cur = cleanDb.openConnection()
  
  dest_dir = sys.argv[1]
  languages = {lang:(liso,lname,set(tlds.split(','))) for lang,liso,lname,tlds in [x.split(':') for x in sys.argv[2:]]}
  
  tbl = open(dest_dir + '/table.txt', 'w')
  for lang in languages:
    liso,lname,tlds = languages[lang]
    print 'begin: ',lang,tlds	# tlds = set of tlds, lang = lang_altcode
    Qparams = {'tlds':tuple(tlds), 'excluded':tuple(excluded_languages - {lang} - known_fails.get(lang,set())), 'limit': 1000}
    print "Q params", Qparams
    cur.execute(Q, Qparams)
    print cur.rowcount, "rows"
    s = cur.fetchone()[0]
    s = re.sub(r"(\s|[0-9])+", " ", s, flags=re.MULTILINE)
    print 'strlen =', len(s)
    #print s
    #raw_input()
    
    tgms = defaultdict(int)
    for tgm in trigrams(s):
      tgms[tgm] += 1
    #print tgms
    
    tbl.write('%s\t%s\t%s\n' % (lang,liso,lname))
    codecs.open((dest_dir+'/%s-3grams.txt') % lang,'w', encoding='utf-8').write('\n'.join("%d %s"%(tgms[tgm],tgm) for tgm in tgms))
    
    print 'end'

if __name__ == '__main__':
  main()
