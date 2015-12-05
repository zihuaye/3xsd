#!/usr/bin/env python
#
# A bench program to test HTTP performance, supporting pipelining request
#

import getopt, sys, time, select, threading
import _socket as socket

r = 'GET / HTTP/1.1\r\nHost: vm0:8000\r\nConnection: keep-alive\r\n\r\n'
request = b''

_n = 0
lock = threading.Lock()

def new_sock(port):
	sock = socket.socket()
	sock.connect(('localhost', port))
	return sock

def dp(sock, multi, pipeline):
	global r, _n, lock
	x=0
	for j in xrange(multi):
		sock.send(request)
		while 1:
			b = sock.recv(32768)
			_bn = b.count('200 OK')
			if _bn > 0:
				with lock:
					_n += _bn
				x += _bn
				if x >= pipeline:
					x=0
					break

if __name__ == '__main__':

	port = 8000
	n=1000
	concurrent = 100
	pipeline=2

        opts, args = getopt.getopt(sys.argv[1:], "p:c:n:o:")

        for o, a in opts:
                if o == "-p":
                        pipeline = int(a)
                elif o == "-c":
			concurrent = int(a)
                elif o == "-n":
			n = int(a)
                elif o == "-o":
			port = int(a)

	multi=int((n/concurrent)/pipeline)

	for i in xrange(pipeline):
		request = ''.join([request, r])

        ts = []
        for x in xrange(concurrent):
                ts.append(threading.Thread(target=dp, args=(new_sock(port),multi,pipeline)))
        start = time.time()
        for x in xrange(concurrent):
                ts[x].start()
        for x in xrange(concurrent):
                ts[x].join()
        end = time.time()
        t = end - start

	print "HTTP bench testing..."
	print "----------------------"
	print "Concurrent:", concurrent
	print "Pipelining:", pipeline
	print "Requests/Conn:", int(n/concurrent)
	print "----------------------"
	print "Sent requests:", n
	print "Get responses:", _n
	print "----------------------"
	print "Timing:", t, "secs,", t/n, "secs/r"
	print "Estimate:", int(n/t), "reqs/s"
	print ""
