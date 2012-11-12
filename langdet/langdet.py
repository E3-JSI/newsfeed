#!/usr/bin/env python

import pickle
import numpy as np
import scipy as sp
import scipy.sparse

import scriptdet

def trigrams(l, tgmid):
    "input: string, trigram->id map; output: tgmid generator"
    # !bn: newline-je bi bilo fajn pretvort v presledke ?
    for tgm in [l[i:i+3] for i in range(len(l)-2)]:
        if tgm[1] == ' ': continue
        tgm = ('<' if tgm[0] == ' ' else tgm[0]) + tgm[1] + ('>' if tgm[2] == ' ' else tgm[2])
        if tgm in tgmid: yield tgmid[tgm]

def langdet(s, D):
    "input: string, langID db; output: (lc_iso_639_3, lc_alt, trigram count)"
    tgmid, langmap, M, Mi, P, U = D
    tgmids = list(trigrams(s, tgmid))
    
    if len(tgmids) == 0: return ('unk', 'unk', 0)
    
    T = M[tgmids,:].sum(0)
    C = Mi[tgmids,:].sum(0)
    c = len(tgmids)
    W = c - C
    T = T + P + (np.array(W, dtype=np.float64) * U)
    
    #return T, len(tgmids)
    best_lang = T.argmax()
    return langmap[best_lang][0], langmap[best_lang][1], len(tgmids)
    #print W
    #return langmap[best_lang][0], langmap[best_lang][1], len(tgmids), T
    

def load_langdet_db(path='langid_nb.pck'):
    tgmid, langid, langmap, M, Mi, P, U = pickle.load(open(path))
    M.sort_indices()
    Mi.sort_indices()

    scripts,script_beg = scriptdet.load_scripts()

    return (tgmid, langmap, M, Mi, P, U, scripts, script_beg)

def langdet_s(s, D, scripts, script_beg):
    "input: langdet + scriptdet params"
    h = scriptdet.scriptdet(scripts, script_beg, s)
    
    # check for jp/cn/kr
    try:
        del h['Common']
    except:
        pass
    total_chars = sum(h.values())

    scriptmap = [
        ('Hangul', 5, 'kr', 'kor'),
        ({'Hiragana', 'Katakana'}, 5, 'ja', 'jpn'),  # order of scripts is relevant -- jpn before cmn
        ('Han', 5, 'zhh', 'cmn'), # langset ? (zhh: zh, han)
        ('Khmer', 5, 'km', 'khm'),
        ('Hebrew', 3, 'he', 'heb'),
        ('Arabic', 3, 'ar', 'arb'),
        ('Ethiopic', 3, 'am', 'amh'),
        ('Armenian', 3, 'hy', 'hye'),
        ('Bengali', 3, 'bn', 'ben'),
        ('Myanmar', 3, 'my', 'mya'),
        ('Georgian', 3, 'ka', 'kat'),
        ('Lao', 3, 'lo', 'lao'),
        ('Sinhala', 3, 'si', 'sin'),
        ('Thai', 3, 'th', 'tha'),
        ('Tibetan', 3, 'bo', 'bod'),
        ('Greek', 3, 'el', 'ell'),
        # Devanagari -> limit langdet to indian langset
        # ('Cyrillic', 3, {}, None),  # problem: bosnian: cyrillic AND latin.
        # ('Latin', 2, None, None)
        ]

    for script_name, script_ratio, lc_alt, lc_iso in scriptmap:
        if type(script_name) == str: script_name = {script_name}

        if script_ratio * sum(h[x] for x in script_name) > total_chars:
            if type(lc_iso) == str:
                return (lc_iso, lc_alt, None)
            else:
                # return langdet(s, D, lc_iso) # limit to lc_iso languages
                pass
    
    return langdet(s, D)
    
def main():
    import sys
    import codecs
    import time

    print "loading db .."
    D = load_langdet_db()
    langmap = D[1]
    print "... done."
    
    for fn in sys.argv[1:]:
        print fn
        if fn == '-':
            fl = sys.stdin.read().decode('utf-8')
        else:
            fl = codecs.open(fn, encoding='utf-8').readlines()
        
        timing = []
        for i in range(1):
            print '.',
            start = time.time()
            
            #T,nids = langdet(' '.join(fl), D)
            li,lc,nids = langdet_s(' '.join(fl), D[:-2], *D[-2:])
            print li,lc,nids
            stop = time.time()
            timing.append(stop-start)
        
            #q = []
            #for i in langmap:
            #    q.append((T[0,i], langmap[i][1]))
            #    q.sort(reverse=True)
            #print q[:10]
        
        print sum(timing) / len(timing), nids
    

if __name__ == '__main__':
    main()
