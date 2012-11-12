#!/usr/bin/env python

"""
prebere NLTKjev 3gram dataset v sys.argv[1]/{*-3grams.txt in table.txt}
(dataset: http://borel.slu.edu/crubadan/)

in zgradi tabelo verjetnosti za multinomski naivn bayes

language prior je ocenjen iz sum(trigrams|lang) / sum(trigrams), namesto iz razmerja dokumentov (ker ga nimamo:)

tabela je redka (5.2 jezika / trigram), zato je nima smisla zgostit z laplace-smoothed verjetnostmi za 0-probability trigrame;

naj rajs classifier vod evidenco kok 0-prob trigramov je bilo pri vsakem jeziku, in to v enmu kosu na konc uposteva.

ostale stevilke pa so laplace-smoothed.

output: pickled tuple:
 - tgmid: { tgm-text: ID_tgm }
 - langid: { lang_altcode: (ID_lang, lang_isocode, langname) }
 - langmap: { ID_lang: (lang_isocode, lang_altcode, langname) }
 - Mp: numpy.array(dtype=float64): stolpci: jeziki, vrstice: trigrami, log probability, laplace smoothed
 - Mip: numpy.array(dtype=int32): isto kot Mp, ampak Mip[x,y]=1 <=> Mp[x,y] > 0
 - P: prior log_e verjetnosti jezikov
 - U: log_e verjetnost neznanega trigrama za posamezne jezike

2012-02-17:
    * v excluded_langs je spisek jezikov, ki jih ignoriramo (za 'enz' :)
    * dataset nalozimo iz vec direktorijev (z enako strukturo); lang_alt kode se ne smejo prekrivat
"""

import sys, os
from collections import defaultdict
import scipy.sparse
import scipy as sp
import numpy as np
import math
import pickle

excluded_langs = {'enz'}

def read_lang_table(d):
    langs = [(d,) + tuple(x.split(None, 2)) for x in open(d + '/table.txt').readlines()]
    return langs	# array tuplov (dir, internal code, iso-639-3, name)

def load_lang(d, lc):
    f = open(d + '/' + lc + '-3grams.txt')
    tgt = [x.split() for x in f.readlines()]
    tgp = [(tgm.decode('utf-8'), int(c)) for c,tgm in tgt]
    return dict(tgp)

def transform(ds, dsum, tsum):
    "ds je cel dataset, dsum je summary dict (tgm:count), tsum pa vsota vseh tgmjev v korpusu"
    nsum = len(dsum)
    print "%d trigrams, %d unique -- wtf ?" % (tsum, nsum)
    
    tgmid = {t:i for i,t in enumerate(dsum)}
    print len(tgmid)
    print "dataset: tgms =", len(tgmid), "ds =", len(ds)
    
    M = scipy.sparse.lil_matrix((len(tgmid), len(ds)), dtype=scipy.float64)		# stolpci: jeziki, vrstice: trigrami, log probability, laplace smoothed
    Mi = scipy.sparse.lil_matrix((len(tgmid), len(ds)), dtype=scipy.int32)		# 'bool' prisotnosti
    P = np.zeros((1, len(ds)), dtype=np.float64)					# prior log prob, base e
    U = np.zeros((1, len(ds)), dtype=np.float64)					# unknown trigram logprob

    langid, langmap = {}, {}
    
    for i,(c,iso,n,d) in enumerate(ds):
        print iso, '-', n
        sum_tgms_lang = float(sum(d.viewvalues()))
        P[0,i] = math.log(sum_tgms_lang / tsum)
        U[0,i] = math.log(1.0 / (sum_tgms_lang + len(dsum)))
        for tgm, count in d.viewitems():
            M[tgmid[tgm], i] = math.log((count + 1.0) / (sum_tgms_lang + len(dsum)))
            Mi[tgmid[tgm], i] = 1
        langid[c] = (i,iso,n)
        langmap[i] = (iso,c,n)
    
    Mp = scipy.sparse.csr_matrix(M)
    Mip = scipy.sparse.csr_matrix(Mi)
    D = (tgmid, langid, langmap, Mp, Mip, P, U)
    pickle.dump(D, open('langid_nb.pck', 'w'))

def main(dirs):
    lts = [read_lang_table(d) for d in dirs]
    print "found %s languages" % (', '.join([str(len(lt)) for lt in lts]),)
    
    ds = []
    dsum = defaultdict(int)
    
    for xd, lc, liso, lname in sum(lts,[]):
        if lc in excluded_langs: continue
        ld = load_lang(xd, lc)
        ds.append((lc,liso,lname.strip(),ld))
        for tgm in ld:
            dsum[tgm] += ld[tgm]
    
    transform(ds, dsum, sum(dsum.viewvalues()))
    
    print "done"

if __name__ == '__main__':
    main(sys.argv[1:])
