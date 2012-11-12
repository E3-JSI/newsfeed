#!/usr/bin/env python

# Scripts.txt comes from http://unicode.org/Public/UNIDATA/Scripts.txt

import re
import bisect
import collections
import sys

def load_scripts():
    d = [((None,None),None)]
    
    r = re.compile(r"([^; ]*)[ ]*; ([^# ]*) (#.*)?")
    for line in open('Scripts.txt'):
        m = r.match(line)
        if m:
            intervals,script = m.groups()[:2]
            if '.' in intervals: interval = intervals.split('..')
            else: interval = (intervals, intervals)
            interval = tuple(map(lambda x: int(x, 16), interval))
            if (interval[0]-1) == d[-1][0][1] and script == d[-1][1]:
                d[-1] = ((d[-1][0][0], interval[1]), d[-1][1])
            else:
                d.append((interval,script))
    sd = sorted(d[1:])
    interval_start = [x[0][0] for x in sd]
    return sd, interval_start

def find_script(scripts, script_beg, c):
    idx = bisect.bisect(script_beg, ord(c)) - 1
    interval = scripts[idx][0]
    if interval[0] <= ord(c) <= interval[1]:
        return scripts[idx][1]
    else:
        return '#unknown#'

def scriptdet(scripts, script_beg, s):
    hist = collections.defaultdict(int)
    for ch in s:
        script = find_script(scripts, script_beg, ch)
        if script:
            hist[script] += 1
    return hist

def main():
    ints, intss = load_scripts()
    print len(ints), "script intervals"

    print dict(scriptdet(ints, intss, sys.stdin.read().decode('utf-8')))

if __name__ == '__main__':
    main()
