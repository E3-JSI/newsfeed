#!/usr/bin/env python

import sys
import re

def main():
    p = re.compile(r'<script>.*</script>')
    r = re.compile(r'<[^>]*?>')
    print r.sub('',p.sub('', sys.stdin.read().replace('\n', ' ')))

if __name__ == '__main__':
    main()
