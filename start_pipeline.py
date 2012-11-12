#!/usr/bin/env python

"""
BROKEN.
"""

from subprocess import call

pipeline = [
	'db2zmq_cleartext.py',
	'zmq2zmq_enrych.py',
	'zmq2zmq_xenrych.py',
	'zmq2zmq_bloombergness.py',
	'zmq2http_all.py',
]
ports = range(13371, 13380)
assert len(pipeline) <= len(ports)+1

for cmd, port_in, port_out in zip(pipeline, [None]+ports, ports+[None])[:0]:
	with open('/tmp/pipeline_part','w') as f:
		f.write('echo -- %s --port-in=%s --port-out=%s' % (cmd, port_in, port_out))
	call(['tmux', 'split-window', '''bash --rcfile /tmp/pipeline_part)'''])
call(['tmux', 'split-window', 'bash --rcfile <(echo "cd ..; ./realtime_cleaner.py")'])
call(['tmux', 'select-layout', 'tiled'])
#call(['tmux', 'new-window'])


