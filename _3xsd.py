# _3xsd module
#
# 3xsd is a native epoll server serving TCP/UDP connections, a high performance static web server, a failover dns server,
# a http-based distributed file server(implementing), and a load-balance proxy-cache server.
#
# The xHandler to handle web requests, and the xDNSHandler to handle DNS query.
# The xZHandler to handle proxy(load-balance/cache)requests, and the xDFSHandler to handle DFS requests.
#
# Author: Zihua Ye (zihua.ye@qq.com, zihua.ye@gmail.com)
#
# Copyright (c) 2014-2015, licensed under GPLv2.
#
# All 3 party modules copyright go to their authors(see their licenses).
#

__version__ = "0.0.13"

import os, sys, io, time, calendar, random, multiprocessing
import shutil, mmap, sendfile, zlib, gzip, cStringIO, copy
import _socket as socket
import select, errno, gevent, dpkt, ConfigParser, hashlib, struct, shelve
from datetime import datetime
from gevent.server import StreamServer
from gevent.coros import Semaphore
from distutils.version import StrictVersion

Port = 8000     #Listening port number
Backlog = 1000  #Listening backlog
Conns = None    #gevent.pool type, connection limit
Workers = 0     #workers to fork, 0 for no worker
Homedir = None  #3xsd working home(document root)
Handler = None  #3xsd handler name, not an instance
Server = None   #3xsd server instance, init at startup
Xcache_shelf = 0#persistent storage of xcache(3zsd&3fsd)
_Name = '3xsd'  #program name, changes at startup

o_socket = socket
o_sendfile = sendfile.sendfile

def Handle_Gevent_Stream(sock, addr):
	#Handler __init__, pass params with no name, can help boosting call procedure
	Handler(sock, addr, Server, False, True)

class _Z_StreamServer(StreamServer):
	workers = 0
	_worker_id = 0

	xcache = {}
	x_reqs = {}
	max_accept = 500

	server_mode = ''
	zlb_mode = False

	def __init__(self, server_address, RequestHandlerClass, backlog=1000, spawn=None):
		StreamServer.__init__(self, server_address, RequestHandlerClass, backlog=backlog, spawn=spawn)
		if StrictVersion(gevent.__version__) >= StrictVersion('1.0'):
			self._stopped_event = self._stop_event

	def pre_start(self):
		if StrictVersion(gevent.__version__) >= StrictVersion('1.0'):
			StreamServer.init_socket(self)
		else:
			StreamServer.pre_start(self)

	def master_works(self):
		if hasattr(socket, "SO_REUSEPORT"):
			self.socket.close()

class _Z_EpollServer(StreamServer):
	workers = 0
	_worker_id = 0

	max_accept = 500
	reuse_port = True

	server_mode = b''
	zlb_mode = False

	_fds = []
	epoll = None

	#for 3wsd - web server
	conns = {}
	addrs = {}
	xcache = {}
	xcache_stat = {}
	x_reqs = {}
	resume = {}

	gzip_shelf = None
	_gzs = {}

	#for 3zsd - z server
	cb_conns = {}
	k_conns = {}
	zconns = {}
	zidles = {}
	zconns_stat = {}
	zaddrs = {}
	zhosts = {}
	zcache = {}
	zcache_stat = {}
	z_reqs = {}
	z_reqs_stat = {}
	z_reqs_cnt = {}
	z_resp_header = {}
	z_path = {}
	c_path = {}

	xcache_shelf = None

	def __init__(self, server_address, RequestHandlerClass, backlog=1000, spawn=None,
							tcp=True, recv_buf_size=16384, send_buf_size=65536):
		if tcp:
			self.recv_buf_size = recv_buf_size
			self.send_buf_size = send_buf_size
		else:
			self.recv_buf_size = 65536
			self.send_buf_size = 65536
		self.tcp = tcp
		StreamServer.__init__(self, server_address, RequestHandlerClass, backlog=backlog, spawn=spawn)
		self.handler = RequestHandlerClass(None, None, self)
		if hasattr(socket, "SO_REUSEPORT"):
			print("Good, this kernel has SO_REUSEPORT support")

	def master_works(self):
		if hasattr(socket, "SO_REUSEPORT"):
			#close master process's listening socket, because it never serving requests
			self.socket.close()

	def pre_start(self):
		self.init_socket(tcp=self.tcp)

	def set_socket_buf(self):
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.recv_buf_size)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.recv_buf_size)

	def init_socket(self, tcp=True):
		if tcp:
			if self.server_mode == 'z_lbs' or self.server_mode == 'x_dfs':
				self.zlb_mode = True
				#socket.socket = gevent.socket.socket
			self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
		else:
			socket.socket = gevent.socket.socket
			self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
		if self.reuse_addr == 1:
			self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		if hasattr(socket, "SO_REUSEPORT") and self.reuse_port:
			#good, this kernel has SO_REUSEPORT support
			self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
		self.set_socket_buf()
		if tcp:
			self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_DEFER_ACCEPT, 1)
		self.socket.bind(self.address)
		self.socket.setblocking(0)
		if tcp:
			self.socket.listen(self.backlog)
		else:
			self.handler.sock = self.socket

	def cleanupall(self):
		if self.tcp:
			self.epoll.unregister(self.socket.fileno())
			self.epoll.close()
		self.conns.clear()
		self.addrs.clear()
		self.xcache.clear()
		self.x_reqs.clear()
	
	def cleanup(self, fd):
		self.epoll.unregister(fd)
		self.conns[fd].close()
		self.conns.pop(fd)
		self.addrs.pop(fd)
		self.x_reqs.pop(fd)
	
	def cleanz(self, fd):
		try:
                	self.epoll.unregister(fd)
		except IOError as e:
			pass

		try:
			#clean c-z pair in cb_conns
			if self.cb_conns.get(fd, None):
				self.cb_conns[self.cb_conns[fd][1]] =  None
			self.cb_conns[fd] = None

			self.keep_connection = 0

                	#self.zconns[fd].close()  #will be closed by clean()
                	self.zconns.pop(fd, None)
                	self.zconns_stat.pop(fd, None)
                	self.zaddrs.pop(fd, None)
                	self.z_reqs.pop(fd, None)
                	self.z_reqs_cnt.pop(fd, None)
                	self.z_reqs_stat.pop(fd, None)
			self.zhosts.pop(fd, None)
			self.zcache.pop(fd, None)
			self.zcache_stat.pop(fd, None)
			self.z_resp_header.pop(fd, None)
			self.z_path.pop(fd, None)
		except:
			pass

	def cleanc(self, fd):
		try:
                	self.epoll.unregister(fd)
		except IOError as e:
			pass

		try:
			if self.cb_conns.get(fd, None):
				self.cb_conns[self.cb_conns[fd][1]] =  None
			self.cb_conns[fd] = None

			self.keep_connection = 0

			self.c_path.pop(fd, None)
			self.k_conns.pop(fd, None)
		except:
			pass

	def o_stack(self, o=None):
		if o == None:
			return
		elif o == "resume":
			print self.resume
		elif o == "zconns":
			print self.zconns
		elif o == "cb_conns":
			print self.cb_conns
		elif o == "xcache":
			print self.xcache
		elif o == "zcache":
			print self.zcache
		elif o == "zcache_stat":
			print self.zcache_stat
		elif o == "z_path":
			print self.z_path
		elif o == "z_resp_header":
			print self.z_resp_header

	def handle_event(self, events):
		for f, ev in events:
			if f == self.socket.fileno():
			    #new connection..
				try:
				#multi workers to accept same connection, only one can get it
					conn, addr = self.socket.accept()
				except:
					continue
				c = conn.fileno()
				#self.epoll.register(c, select.EPOLLIN | select.EPOLLET)
				self.epoll.register(c, select.EPOLLIN)
				self.conns[c] = conn
				self.addrs[c] = addr
			elif ev & select.EPOLLIN:
			    #read event..
				if f not in self.zconns:
					self._fds.append((f, 0))
				else:
					self._fds.append((f, 2))
			elif ev & select.EPOLLOUT:
			    #write event..
				if f not in self.zconns:
					self._fds.append((f, 1))
				else:
					self._fds.append((f, 3))
			elif ev & select.EPOLLHUP:
			    #connection closed..
				self.cleanup(f)
	
		if len(self._fds) > 0:
			#we have works to do, call handler
			self.handler(self._fds)
			del self._fds[:]

	def if_reinit_socket(self):
		if hasattr(socket, "SO_REUSEPORT") and self.reuse_port and self.workers > 0:
			self.init_socket(tcp=self.tcp)

	def serve_forever(self):
		#see if reinit socket is neccessary
		self.if_reinit_socket()
		#single master gets an epoll, multi workers get multi epolls
		self.epoll = select.epoll()
		#register fd and events to poll
		self.epoll.register(self.socket.fileno(), select.EPOLLIN)

		try:
			while 1:
				self.handle_event(self.epoll.poll())
		finally:
			self.cleanupall()
			self.socket.close()		

	def serve_dns(self):
		self.if_reinit_socket()
		self.epoll = select.epoll()
		self.epoll.register(self.socket.fileno(), select.EPOLLIN | select.EPOLLET)

		try:
			gevent.spawn(self.handler.probe_ips)  	#a separate greenlet to perform ip stat checking
			while 1:
				gevent.sleep(1e-20)		#a smallest float, works both at v0.13.8 and v1.0.x
				_events = self.epoll.poll(10)   #long-timeout helping cpu% lower
				if len(_events) > 0:
					self.handler(_events)
		finally:
			self.cleanupall()
			self.socket.close()		

	def serve_lbs(self):
		self.if_reinit_socket()
		self.epoll = select.epoll()
		self.epoll.register(self.socket.fileno(), select.EPOLLIN)

		try:
			#gevent.spawn(self.handler.check_zconns)  #a separate greenlet to perform zconn checking
			while 1:
				#gevent.sleep(1e-20)
				#self.handle_event(self.epoll.poll(10))
				self.handle_event(self.epoll.poll())
		finally:
			self.cleanupall()
			self.socket.close()		

	def serve_dfs(self):
		self.if_reinit_socket()
		self.epoll = select.epoll()
		self.epoll.register(self.socket.fileno(), select.EPOLLIN)

		try:
			#gevent.spawn(self.handler.check_dfs)  #a separate greenlet to perform dfs checking
			while 1:
				#gevent.sleep(1e-20)
				#self.handle_event(self.epoll.poll(10))
				self.handle_event(self.epoll.poll())
		finally:
			self.cleanupall()
			self.socket.close()		

class _xHandler:
	http_version_11 = "HTTP/1.1"
	http_version_10 = "HTTP/1.0"

	HTTP11 = 1
	HTTP10 = 0
	
	HTTP_OK = 200
	HTTP_NOT_MODIFIED = 304
	HTTP_BAD_REQUEST = 400
	HTTP_FORBITDDEN = 403
	HTTP_NOT_FOUND = 404
	HTTP_SERVER_ERROR = 500
	HTTP_SERVER_RESET = 502
	HTTP_SERVER_BUSY = 503
	HTTP_SERVER_TIMEOUT = 504
	
	PARSE_OK = 0
	PARSE_ERROR = -1
	PARSE_AGAIN = 1
	PARSE_MORE = 2

	EOL1 = b'\n\n'
	EOL2 = b'\n\r\n'
	
	xR_OK = 0
	xR_PARSE_AGAIN = 1
	xR_ERR_PARSE = -1
	xR_ERR_HANDLE = -2
	xR_ERR_403 = -3
	xR_ERR_404 = -4
	
	xResult = 0

	weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
	
	monthname = [None, 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
	         	'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

	index_files = ["index.html", "index.htm"]

	gzip_types = ["html", "htm", "js", "css", "txt", "xml"]

	mimetype = {'html': 'text/html', 'htm': 'text/html', 'txt': 'text/plain',
			'css': 'text/css', 'xml': 'text/xml', 'js': 'application/x-javascript',
			'png': 'image/png', 'jpg': 'image/jpeg', 'gif': 'image/gif', 'bin': 'application/octet-stream'}

	web_config = None
	web_config_parsed = False
	
	_r = b''  #request headers
	
	xcache_ttl = 5  	#normally, 5-10 seconds internal cache time of items
	xcache_size = 1000000   #1 million items, about 1/3GB(333MB) mem used
	x_shelf_size = 1000000  #1 million items in disk, about 30GB disk size with average item size 30KB

	gzip_on = False
	gzip_size = 1000	 #default >1KB file size can be gzipped
	gzip_max_size = 10000000 #default <=10MB file size can be gzipped

	multip = {'k':1000, 'K':1000, 'm':1000000, 'M':1000000, 'g':1000000000,'G':1000000000}

	def __init__(self, conn, client_address, server, native_epoll=True,
				gevent_stream=False, recv_buf_size=16384, send_buf_size=65536, pipelining=False):
		self.server = server

		self._native_epoll = native_epoll
		self._gevent_stream = gevent_stream

		if native_epoll:
			self.recv_buf_size = self.server.recv_buf_size
			self.send_buf_size = self.server.send_buf_size
		else:
			self.recv_buf_size = recv_buf_size
			self.send_buf_size = send_buf_size

		self.server_version = ''.join([_Name, '/',  __version__])
		self.server_pipelining = pipelining

		self.in_headers = {}
		self.out_headers = {}

		self.homedir = Homedir

		self.init_config()
		self.init_handler(conn, client_address)

		if self._gevent_stream:
			sendfile.sendfile = gevent_sendfile
			self.handle_request()
			self.clean()

	def init_config(self):
		if not self.web_config_parsed:
			try:
                		self.web_config = ConfigParser.ConfigParser()
                		if not self.web_config.read('3xsd.conf'):
					self.web_config.read('/etc/3xsd.conf')

				for name, value in self.web_config.items('3wsd'):
                                	if name == 'root':
						if value:
							self.homedir = value
                                	elif name == 'index':
						self.index_files = []
						for item in value.split(','):
							if item:
								self.index_files.append(item)
						if not self.index_files:
							self.index_files = ["index.html", "index.htm"]
                                	elif name == 'mime_types':
						for item in value.split(','):
							if item:
								k, v = item.split(':', 1)
								if k and v:
									self.mimetype[k] = v
                                	elif name == 'gzip':
						if value.lower() == "on":
							self.gzip_on = True
							self.server.gzip_shelf = shelve.open('gzip.shelf', writeback=True)
                                	elif name == 'gzip_size':
						if value[-1] in ['k','m','g','K','M','G']:
							_multip = self.multip[value[-1]]
							self.gzip_size = int(value[:-1])*_multip
						else:
							self.gzip_size = int(value)
                                	elif name == 'gzip_max_size':
						if value[-1] in ['k','m','g','K','M','G']:
							_multip = self.multip[value[-1]]
							self.gzip_max_size = int(value[:-1])*_multip
						else:
							self.gzip_max_size = int(value)
                                	elif name == 'gzip_types':
						for item in value.split(','):
							if item:
								if item not in self.gzip_types:
									if item[0] == '-':
										self.gzip_types.remove(item[1:])
									else:
										self.gzip_types.append(item)
			except:
				pass

			web_config_parsed = True

	def init_handler(self, conn, client_address, rw_mode=0):
		self.sock = conn
		self.addr = client_address
		
		self.out_body_file = self.out_body_mmap = self._c = self.accept_encoding = None
		self.out_body_size = self.out_body_file_lmt = self.cmd_get = self.cmd_head =  self.cmd_put =  self.cmd_delete = self.if_modified_since = self.keep_connection = 0
		self.has_resp_body = self.xcache_hit = False
		self.canbe_gzipped = self.gzip_transfer = self.gzip_chunked = False
		self.gzip_finished = self.next_request = True
		#self.vhost_mode = False
		self.transfer_completed = 1

		self.command = self.path = self.resp_line = self.resp_msg = self.out_head_s = self._r = self.hostname = self.xcache_key = b''
		self.c_http_ver = self.s_http_ver = self.r_http_ver = 1
		
		self.resp_code = self.HTTP_OK
		self.resume_transfer = rw_mode

	def __call__(self, fds):
		#should be called by native epoll server, can handle multi requests at one handler call, like: do 10 read events at a time with 10 connections
		for f, rw_mode in fds:
			self.init_handler(self.server.conns[f], self.server.addrs[f], rw_mode)

			if self.resume_transfer == 0:
				parse_stat = self.x_parse()
				if parse_stat == self.PARSE_OK or parse_stat == self.PARSE_MORE:
					if self.cmd_get == 1 or self.cmd_head == 1:
						self.x_GET()
						self.x_response()
					else:
						self.xResult = self.xR_ERR_HANDLE
				elif parse_stat == self.PARSE_AGAIN:
					self.xResult = self.xR_PARSE_AGAIN
					continue
				else:
					self.xResult = self.xR_ERR_PARSE
			elif self.resume_transfer == 1:
				#continue sending a large file
				self.x_response()
			else:
				self.xResult = self.xR_ERR_PARSE

			self.clean()

	def check_connection(self, c_http_ver, check_ims=True, gen_xcache_key=True):
		if c_http_ver == "HTTP/1.1":
			self.c_http_ver = 1
			if self.s_http_ver == self.HTTP11:
				self.r_http_ver = 1
			else:
				self.r_http_ver = 0
		else:
			self.c_http_ver = 0
			self.r_http_ver = 0

		if self.in_headers.get("Connection", "null").lower() == "keep-alive":
			self.keep_connection = 1
			self.r_http_ver = 1
		else:
			self.keep_connection = 0

		#accept_encoding = ['gzip', 'deflate', 'sdch'] or ['gzip'] or ['null']
		#self.accept_encoding = self.in_headers.get("Accept-Encoding", "null").replace(' ','').split(',')

		if self.server.zlb_mode:
			if gen_xcache_key:
				self.hostname = self.in_headers.get("Host", "127.0.0.1").split(':',1)[0]
				self.xcache_key = ''.join([self.hostname, self.path])
		else:
			self.xcache_key = self.path

		if check_ims:
			if self.in_headers.get("If-Modified-Since"):
				self.if_modified_since = 1
			else:
				self.if_modified_since = 0

	def date_time_string(self, ts=None):
		if ts is None: ts = time.time()
		year, month, day, hh, mm, ss, wd, y, z = time.gmtime(ts)
		s = "%s, %02d %3s %4d %02d:%02d:%02d GMT" % (
			self.weekdayname[wd],
			day, self.monthname[month], year,
			hh, mm, ss)
		return s

	def set_out_header(self, key, value):
		self.out_headers[key] =  value

	def set_resp_code(self, code):
		if self.r_http_ver == self.HTTP11:
			prefix = "HTTP/1.1"
		else:
			prefix = "HTTP/1.0"

		self.has_resp_body = False
		self.resp_code = code

		if code == 200:
			self.resp_msg = "OK"
			self.has_resp_body = True
		elif code == 301:
			self.resp_msg = "Move Permanent"
		elif code == 302:
			self.resp_msg = "Move Temporary"
		elif code == 304:
			self.resp_msg = "Not Modified"
		elif code == 404:
			self.resp_msg = "Not Found"
		elif code == 403:
			self.resp_msg = "Forbidden"
		elif code == 500:
			self.resp_msg = "Server Error"
		elif code == 502:
			self.resp_msg = "Server Reset"
		elif code == 504:
			self.resp_msg = "Server Timeout"

		self.resp_line = ''.join([prefix, ' ', str(code), ' ', self.resp_msg])

	def handle_request(self):
		#called when running at gevent mode
		while self.next_request:
			if self.x_parse() == self.PARSE_OK:
				if self.cmd_get == 1 or self.cmd_head == 1:
					self.x_GET()
				else:
					return
				self.x_response()
			else:
				self.xResult = self.xR_ERR_PARSE
				self.next_request = False
			if self.keep_connection == 0:
				self.next_request = False
							
	def x_parse(self):
		#get the request headers
		_doing_pipelining = _last_pipelining = False

		if self.sock.fileno() not in self.server.x_reqs:
			r = self._r
			_xreqs_empty = True
		else:
			r = self._r = self.server.x_reqs[self.sock.fileno()][0]
			_xreqs_empty = False

			if self.EOL2 in r or self.EOL1 in r:
				if self.server.x_reqs[self.sock.fileno()][1] == 0:
					#not pipelining requests, must done before
					self.server.x_reqs.pop(self.sock.fileno())
					r = self._r = b''
				else:
					_doing_pipelining = True

		while 1:
			try:
				if not _doing_pipelining:
					b = self.sock.recv(self.recv_buf_size)
					if b:
						r = self._r = ''.join([r, b])
					else:
						#peer closed connection?
						return self.PARSE_ERROR

				if self.EOL2 in r: 
					#headers mostly end with EOL2 "\n\r\n"
					if self.server_pipelining:
						#for http pipelining
						if r.count(self.EOL2) > 1: 
							c = r.split(self.EOL2, 1)
							r = c[0]
							self.server.x_reqs[self.sock.fileno()] = [c[1], 1]
						else:
							self.server.x_reqs[self.sock.fileno()] = [r, 1]
							_last_pipelining = True
					else:
						if not _xreqs_empty:
							#a big-headers request is all recieved
							self.server.x_reqs.pop(self.sock.fileno())
					break
				else:
					#not finished all headers, save recv data
					self.server.x_reqs[self.sock.fileno()] = [r, 0]
					return self.PARSE_AGAIN
				self.sock.setblocking(0)
			except socket.error as e:
				if e.errno == errno.EAGAIN:
					#no more request data, see if the whole request headers should be recieved
						if self.EOL2 in r or self.EOL1 in r:
							break
						else:
							#keeping connection, no request has been sent..
							#self.sock.setblocking(1)
							return self.PARSE_AGAIN
				else:
					#peer closed connection?
					return self.PARSE_ERROR

		a = r.strip().split("\r\n", 1)

		if not a[0]:
			#illeagal request headers
			return self.PARSE_ERROR

		#"GET / HTTP/1.1"
		self.command, self.path, _c_http_ver = a[0].split()

		if self.command == 'GET':
			self.cmd_get = 1
		elif self.command == 'HEAD':
			self.cmd_head = 1
		elif self.command == 'PUT':
			self.cmd_put = 1
		elif self.command == 'DELETE':
			self.cmd_delete = 1
		else:
			return self.PARSE_ERROR

		#all headers go to dict
		self.in_headers = dict((k, v) for k, v in (item.split(": ") for item in a[1].split("\r\n")))

		self.check_connection(_c_http_ver)

		if self.sock.fileno() not in self.server.x_reqs:
			return self.PARSE_OK
		else:
			if not _doing_pipelining:
				return self.PARSE_OK
			else:
				if not _last_pipelining:
					return self.PARSE_MORE
				else:
					return self.PARSE_OK

	def x_GET(self):
		if self.if_modified_since == 0 and self.xcache_key in self.server.xcache:
			self._c = self.server.xcache.get(self.xcache_key)
			ttl = self._c[0]
			if ttl >= time.time():
				#cache hit
				self.out_head_s, self.out_body_file, self.out_body_size, self.out_body_file_lmt, self.out_body_mmap, self.canbe_gzipped = self._c[1:]
				self.has_resp_body = True

				if self.canbe_gzipped:
					for x in self.in_headers.get("Accept-Encoding", "null").replace(' ','').split(','):
						if x == "gzip":
							self.gzip_transfer = True
							break
					if self.gzip_transfer:
						if  self.xcache_key in self.server.gzip_shelf:
							if self.server.gzip_shelf[self.xcache_key][4]==self.out_body_file_lmt:
								self.out_head_s=self.server.gzip_shelf[self.xcache_key][1]
								self.out_body_file=self.server.gzip_shelf[self.xcache_key][2]
								self.out_body_size=self.server.gzip_shelf[self.xcache_key][3]
								self.xcache_hit = True
								return
							else:
								self.xcache_hit = False
						else:
							self.xcache_hit = False
					else:
						self.xcache_hit = True
						return
				else:
					self.xcache_hit = True
					return
			else:
				#cache item expired
				if isinstance(self._c[2], file) and not self._c[2].closed:  #close the file opened, if not closed
					self._c[2].close()

				if self._c[5]:  #close the mmap maped, if exists
					self._c[5].close()

				self._c = None
				self.server.xcache.pop(self.xcache_key)

				self.xcache_hit = False

		#cache miss or if_modified_since request

		"""
		if self.vhost_mode:
			path = ''.join([self.homedir, '/', self.hostname, self.path])
		else:
			path = ''.join([self.homedir, self.path])
		"""

		path = ''.join([self.homedir, self.path])

		if os.path.isdir(path):
			if not path.endswith('/'):
				self.set_resp_code(301)
				self.set_out_header("Location", ''.join([path, "/"]))
				return

		for index in self.index_files:
			index = os.path.join(path, index)
			if os.path.exists(index):
				path = index
				break

		try:
			f = open(path, 'rb')
			self.out_body_file = f
		except IOError as e:
			if e.errno == errno.EISDIR:
				self.set_resp_code(403)
			else:
				self.set_resp_code(404)
			return

		try:
			fs = os.fstat(f.fileno())
			
			#Last Modified time
			self.out_body_file_lmt = fs.st_mtime
			lmt = self.date_time_string(fs.st_mtime)
			lms = self.in_headers.get("If-Modified-Since")
			if lmt == lms:
				self.set_resp_code(304)
				return
			else:
				self.set_out_header("Last-Modified", lmt)
			
			self.out_body_size = fs[6]
			self.set_out_header("Content-Length", str(fs[6]))
		except:
			self.set_resp_code(404)
			f.close()
			return

		try:
			a = path.rsplit('.')
			content_type = self.mimetype.get(a[1])
			if content_type:
				self.set_out_header("Content-Type", content_type)
				if self.gzip_on and self.r_http_ver == self.HTTP11:
					if a[1] in self.gzip_types and self.out_body_size > self.gzip_size and self.out_body_size <= self.gzip_max_size:
						self.canbe_gzipped = True
			else:
				self.set_out_header("Content-Type", "application/octet-stream")
		except:
			self.set_out_header("Content-Type", "application/octet-stream")

		self.set_resp_code(200)

	def send_out_all_headers(self, extra=None):
		if extra:
			if self.keep_connection == 1 and self.r_http_ver == self.HTTP11:
				self.sock.send(''.join([self.out_head_s, "Connection: keep-alive\n\n", extra]))
			else:
				self.sock.send(''.join([self.out_head_s, "Connection: close\n\n", extra]))
		else:
			if self.keep_connection == 1 and self.r_http_ver == self.HTTP11:
				self.sock.send(''.join([self.out_head_s, "Connection: keep-alive\n\n"]))
			else:
				self.sock.send(''.join([self.out_head_s, "Connection: close\n\n"]))

	def x_response(self):  #xxx
		if self.resume_transfer == 0:
			sent = _sent = 0
		elif self.resume_transfer == 1:
			self.xcache_hit = self.has_resp_body = True
			self.command = 'GET'
			self.cmd_get = 1
			_sent = 0
			self.transfer_completed = 0

			_s_sock_fileno = self.sock.fileno()
			_rs = self.server.resume.get(_s_sock_fileno)
			if _rs:
				self.out_body_file, self.out_body_size, sent, self.keep_connection, self.gzip_transfer, self.xcache_key = _rs
				if self.gzip_transfer:
					_org_file = self.server.xcache[self.xcache_key][2]
					_org_size = self.server.xcache[self.xcache_key][3]
					self.out_head_s = self.server.gzip_shelf[self.xcache_key][1]
					self.out_body_file = self.server.gzip_shelf[self.xcache_key][2]
					self.out_body_size = self.server.gzip_shelf[self.xcache_key][3]
					_file_lmt = self.server.gzip_shelf[self.xcache_key][4]
					_gzip_pos = self.server.gzip_shelf[self.xcache_key][5]
					self.gzip_finished = self.server.gzip_shelf[self.xcache_key][6]
			else:
				#no such resume, must be first trans
				self.server.epoll.modify(_s_sock_fileno, select.EPOLLIN)
				self.resume_transfer = sent = 0
				self.out_head_s = self.server.xcache[self.xcache_key][1]
				self.out_body_file = self.server.xcache[self.xcache_key][2]
				self.out_body_size = self.server.xcache[self.xcache_key][3]

		#At this point, begin transfer response, first to roll out headers
		if not self.xcache_hit:
			_t = time.time()
			if len(self.out_headers) > 0:
				self.out_head_s = ''.join([self.resp_line, "\nServer: ", self.server_version, "\nDate: ", self.date_time_string(_t), '\n', '\n'.join(['%s: %s' % (k, v) for k, v in self.out_headers.items()]), '\n'])
			else:
				self.out_head_s = ''.join([self.resp_line, "\nServer: ", self.server_version, "\nDate: ", self.date_time_string(_t), '\n'])

			if self.resp_code == self.HTTP_OK and self.out_body_size > 0:
				#Only 200 and body_size > 0 response will be cached, [ttl, out_head_s, f, fsize, f_lmt, mmap], and file smaller than 1KB will be mmaped
				if self.out_body_size < 1000 and not self.canbe_gzipped:
					self.out_body_mmap = mmap.mmap(self.out_body_file.fileno(), 0, prot=mmap.PROT_READ)
				else:
					if self.canbe_gzipped:
						for x in self.in_headers.get("Accept-Encoding", "null").replace(' ','').split(','):
							if x == "gzip":
								self.gzip_transfer = True
								break

						if self.gzip_transfer:
							#generate gzip cache item
							try:
								#gzip it
								_gzf = zlib.compressobj(6,
											zlib.DEFLATED,
											zlib.MAX_WBITS | 16,
											zlib.DEF_MEM_LEVEL,
											0)
								self.out_body_file.seek(0)
								if self.out_body_size > self.send_buf_size/2:
									self.gzip_chunked = True
									_ss = _gzf.compress(self.out_body_file.read(self.send_buf_size/2))
									_ss = ''.join([_ss, _gzf.flush(zlib.Z_SYNC_FLUSH)])
								else:
									_ss = _gzf.compress(self.out_body_file.read(self.out_body_size))
									_ss = ''.join([_ss, _gzf.flush(zlib.Z_FINISH)])


								_out_headers = copy.copy(self.out_headers)
								_out_headers["Content-Encoding"] = "gzip"
								if self.gzip_chunked:
									_out_headers["Transfer-Encoding"] = "chunked"
									try:
										del _out_headers["Content-Length"]
									except:
										pass
								else:
									_out_headers["Content-Length"] = len(_ss)
								_out_head_s = ''.join([self.resp_line, "\nServer: ", self.server_version, "\nDate: ", self.date_time_string(_t), '\n', '\n'.join(['%s: %s' % (k, v) for k, v in _out_headers.items()]), '\n'])
								#keep the mem cache of gzip_shelf limitted
								while len(self.server.gzip_shelf.cache) > 1000:
									self.server.gzip_shelf.cache.popitem()

								#keep the disk cache of gzip_shelf limitted
								if len(self.server.gzip_shelf) > self.x_shelf_size:
									self.server.gzip_shelf.popitem()

								if self.gzip_chunked:
									#[file size original, headers, content, body_size, file modified time, current gzip position, finished]
									_sss = ''.join([hex(len(_ss))[2:], '\r\n', _ss, '\r\n'])
									self.server.gzip_shelf[self.xcache_key] = [self.out_body_size, _out_head_s, _sss, len(_sss), self.out_body_file_lmt, self.send_buf_size/2, False]
									self.server._gzs[self.xcache_key] = _gzf
								else:
									self.server.gzip_shelf[self.xcache_key] = [self.out_body_size, _out_head_s, _ss, len(_ss), self.out_body_file_lmt, self.out_body_size, True]

								if hasattr(self.server.gzip_shelf.dict, 'sync'):
									self.server.gzip_shelf.dict.sync()
							except:
								pass  #zzz

				if len(self.server.xcache) > self.xcache_size:
					self.server.xcache.popitem()

				#put xcache item, every item take about 8+300+8+8+8+8+1=340 bytes
				#3 items per 1KB mem, 3k items per 1MB mem, 3M items per 1GB mem
				self.server.xcache[self.xcache_key] = [self.xcache_ttl + _t, self.out_head_s, self.out_body_file, self.out_body_size, self.out_body_file_lmt, self.out_body_mmap, self.canbe_gzipped]

				if self.gzip_transfer:
					_org_file = self.server.xcache[self.xcache_key][2]
					_org_size = self.server.xcache[self.xcache_key][3]
					self.out_head_s = self.server.gzip_shelf[self.xcache_key][1]
					self.out_body_file = self.server.gzip_shelf[self.xcache_key][2]
					self.out_body_size = self.server.gzip_shelf[self.xcache_key][3]
					_file_lmt = self.server.gzip_shelf[self.xcache_key][4]
					_gzip_pos = self.server.gzip_shelf[self.xcache_key][5]
					self.gzip_finished = self.server.gzip_shelf[self.xcache_key][6]

			elif self.resp_code >= self.HTTP_BAD_REQUEST:
				self.out_head_s = ''.join([self.out_head_s, "Content-Length: ", str(len(self.resp_msg) + 4), '\n'])

		#send headers & body
		if self.has_resp_body and self.out_body_file and self.cmd_get == 1:
			if self.out_body_mmap:
				self.send_out_all_headers(extra=self.out_body_mmap[:])
			elif isinstance(self.out_body_file, str) and self.out_body_size < 1000 and self.gzip_finished:
				self.send_out_all_headers(extra=self.out_body_file)
			else:
				#Try send as much as data once in a TCP packet
				#Because 2(1 header + 1 body) packets turn down performance up to 50% than 1(header + body) packet 
				if self.resume_transfer == 0:
					self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
					self.send_out_all_headers()

				if self.out_body_size - sent >= self.send_buf_size:
					if self._native_epoll:
						_send_buf = self.send_buf_size
					else:
						_send_buf = self.out_body_size
				else:
					_send_buf = self.out_body_size - sent

				try:
					if isinstance(self.out_body_file, str):
						#_sent = self.sock.send(self.out_body_file[sent:_send_buf+sent])
						_sent = self.sock.send(self.out_body_file[sent:_send_buf+sent])
					else:
						_sent = sendfile.sendfile(self.sock.fileno(), self.out_body_file.fileno(),
													sent, _send_buf)
					sent += _sent
					if self.resume_transfer == 0:
						#after transfer snd_buf data, requeue event, let other event to be handled
						if sent < self.out_body_size or not self.gzip_finished:
							self.server.resume[self.sock.fileno()] = [self.out_body_file,
									self.out_body_size, sent, self.keep_connection,
									self.gzip_transfer, self.xcache_key]
							self.server.epoll.modify(self.sock.fileno(), select.EPOLLOUT)
							self.transfer_completed = 0
					else:
						if self.out_body_size == sent and self.gzip_finished:
							del self.server.resume[self.sock.fileno()]
							#self.server.epoll.modify(self.sock.fileno(), select.EPOLLIN|select.EPOLLET)
							self.server.epoll.modify(self.sock.fileno(), select.EPOLLIN)
							self.transfer_completed = 1
						else:
							self.server.resume[self.sock.fileno()] = [self.out_body_file,
									self.out_body_size, sent, self.keep_connection,
									self.gzip_transfer, self.xcache_key]
				except OSError as e:
					if e[0] == errno.EAGAIN:
					#send buffer full?just wait to resume transfer
					#and gevent mode can't reach here, beacause gevent_sendfile intercepted the exception
						 self.server.resume[self.sock.fileno()] = [self.out_body_file,
                                                                        self.out_body_size, sent, self.keep_connection,
									self.gzip_transfer, self.xcache_key]

				if not self.gzip_finished:
					#continue gen gzip chunked encoding file data, zzz
					_gzf = self.server._gzs.get(self.xcache_key)

					if not _gzf:
						#this wrong, may cause error, just in case
						_gzf = zlib.compressobj(6,
								zlib.DEFLATED,
								zlib.MAX_WBITS | 16,
								zlib.DEF_MEM_LEVEL,
								0)

					if _org_size > _gzip_pos + self.send_buf_size/2:
						_z_buf_size = self.send_buf_size/2
						_flush_mode = zlib.Z_SYNC_FLUSH
					else:
						_z_buf_size = _org_size - _gzip_pos
						self.gzip_finished = True
						_flush_mode = zlib.Z_FINISH

					_org_file.seek(_gzip_pos)
					_ss = _gzf.compress(_org_file.read(_z_buf_size))
					_ss = ''.join([_ss, _gzf.flush(_flush_mode)])

					_sss = ''.join([self.out_body_file, hex(len(_ss))[2:], '\r\n', _ss, '\r\n'])

					if self.gzip_finished:
						_sss = ''.join([_sss, '0\r\n\r\n'])
						self.server._gzs.pop(self.xcache_key)

					self.server.gzip_shelf[self.xcache_key] = [_org_size, self.out_head_s, _sss, len(_sss), _file_lmt, _gzip_pos + _z_buf_size, self.gzip_finished]

					if hasattr(self.server.gzip_shelf.dict, 'sync'):
						self.server.gzip_shelf.dict.sync()

				#Now, transfer complete, resume nature behavior of TCP/IP stack, as turned before
				if self.keep_connection == 1 and self.resume_transfer == 0:
				    	#no need to set TCP_CORK when keep_connection=0
				    	#it will be cleared when socket closing and data will be flushed
					self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)
		elif self.resp_code == self.HTTP_NOT_MODIFIED:
			self.send_out_all_headers()
		elif self.resp_code == self.HTTP_OK and self.cmd_head == 1:
			self.send_out_all_headers()
		elif self.resp_code >= self.HTTP_BAD_REQUEST:
			self.send_out_all_headers(extra = "%d %s" % (self.resp_code, self.resp_msg))

	def clean(self):
		self.in_headers.clear()
		self.out_headers.clear()

		if self.keep_connection == 0 and self.transfer_completed == 1:
			self.sock.close()

def gevent_sendfile(out_fd, in_fd, offset, count):
    #This function is borrowed from gevent's example code, thanks!
    sent = 0
    while sent < count:
        try:
            _sent = o_sendfile(out_fd, in_fd, offset + sent, count - sent)
            sent += _sent
        except OSError as ex:
            if ex.args[0] == errno.EAGAIN:
                gevent.socket.wait_write(out_fd)
            else:
                raise
    return sent

class _xDNSHandler:
	PARSE_OK = 0
	PARSE_ERROR = -1
	
	xR_OK = 0
	xR_ERR_PARSE = -1
	xR_ERR_HANDLE = -2
	
	xResult = 0
	
	sock = None
	data = None
	addr = None
	
	rra = {}
	rrn = {}
	ttl = {}
	stat = {}

	_rra = None
	_ttl = None
	q = None
	query_name = None
	answer = None
	
	xcache_ttl = 10
	probe_interval = 10  #icmp probe interval in seconds, see if the ip alive

	def __init__(self, conn, client_address, server, config_section='3nsd'):
		self.server = server
		#if server.workers > 1:
		#	self._wlock = multiprocessing.Lock()
		#else:
		#	self._wlock = Semaphore()
		self.config = ConfigParser.ConfigParser()
		if not self.config.read('3xsd.conf'):
			self.config.read('/etc/3xsd.conf')

		for name, value in self.config.items(config_section):
			v = value.split(',', 1)
			if len(v) > 1:
				# [ttl, ip, ...]
				self.ttl[name] = int(v[0])
				self.rra[name] = self.ip_list(name, v[1], config_section)
				self.rrn[name] = 0

	def ip_list(self, name, ipstr, config_section):
		#ip,ip,ip... can be the following format:   lll
		#10.23.4.11  -  single ip, 10.23.4.101-200  -  multi ip
		a = ipstr.split(',')
		iplist = []
		t = time.time()
		for item in a:
			if item.find('-') < 0:
				iplist.append(item)
				self.stat[item] = [True, self.ttl[name], t]  #[stat, ttl, last-check-time]
			else:
				_ip = item.split('.')

				if config_section == '3nsd':
					_port = None
					_last = _ip[3].split('-')
				elif config_section == '3zsd' or config_section == '3fsd':
					#10.23.4.101-200:8000
					__l = _ip[3].split(':')
					_port = __l[1]
					_last = __l[0].split('-')

				for i in range(int(_last[0]), int(_last[1])+1):
					ip = '.'.join([_ip[0], _ip[1], _ip[2], str(i)])
					if _port:
						ip = ''.join([ip, ':', _port])
					iplist.append(ip)
					self.stat[ip] = [True, self.ttl[name], t]
		if len(iplist) > 0:
			return iplist
		else:
			self.stat.clear()
			return None

	def init_dns_handler(self):
		self.data = None
		self.addr = None
		self._rra = None
		self._ttl = None
		self.q = None
		self.query_name = None
		self.answer = None

	def __call__(self, events):
		#found that one event can contains multi dns query packages, so use while loop instead of for
		while 1:
			try:
				self.data, self.addr = self.sock.recvfrom(1024)
				#print "worker", self.server._worker_id, "get:", self.addr
				if self.x_parse_query() == self.PARSE_OK:
					self.x_gen_answer()
					self.x_send_out()
				else:
					self.xResult = self.xR_ERR_PARSE
			except socket.error as e:
				if e.errno == errno.EAGAIN:
					self.init_dns_handler()
					break
				else:
					raise

	def shift(self, alist, n):
		_n = n % len(alist)
		return alist[_n:] + alist[:_n]

	def x_parse_query(self):
		self.q = dpkt.dns.DNS(self.data)

		#we accept just A type query
		if self.q.qd[0].cls != dpkt.dns.DNS_IN or self.q.qd[0].type != dpkt.dns.DNS_A:
			return self.PARSE_ERROR
		
		self.query_name =  self.q.qd[0].name
		self._rra = self.rra.get(self.query_name)
		self._ttl = self.ttl.get(self.query_name)
		
		if self._rra is not None and self._ttl is not None:
		#ok, rr & ttl config found
			return self.PARSE_OK
		else:
		#not my serving domain name
			return self.PARSE_ERROR

	def x_gen_answer(self):
		if self.query_name in self.server.xcache:
			_c = self.server.xcache.get(self.query_name)
			if _c[0] > time.time():
				#cache item not expired, load it and rewrite the id field of answer to match queryer's 
				if self.q.id < 255:
					self.answer = ''.join(['\x00', chr(self.q.id), _c[1][2:]])
				else:
					self.answer = ''.join([chr(self.q.id/256), chr(self.q.id % 256), _c[1][2:]])
				return
			else:
				#expired, clear it
				self.server.xcache.pop(self.query_name)

		#cache not hit, go on handling: first to turn query into answer
		self.q.op = dpkt.dns.DNS_RA
		self.q.rcode = dpkt.dns.DNS_RCODE_NOERR
		self.q.qr = dpkt.dns.DNS_R
		
		_alive = 0
		_query_name = self.query_name

		#for round robbin, shift ip list every time
		self._rra = self.shift(self.rra.get(_query_name), self.rrn.get(_query_name))
		self.rrn[_query_name] = (self.rrn[_query_name] + 1) % len(self.rra.get(_query_name))

		#gen rr records
		while _alive == 0:
			for _ip_s in self._rra:
				#append rr record with ip not down
				__stat = self.stat.get(_ip_s)
				_stat = __stat[0]
				if not _stat:
					continue
				else:
					_alive += 1
				arr = dpkt.dns.DNS.RR()
				arr.cls = dpkt.dns.DNS_IN
				arr.type = dpkt.dns.DNS_A
				arr.name = self.query_name
				arr.ip = socket.inet_aton(_ip_s)
				arr.ttl = self._ttl
				self.q.an.append(arr)

			if _alive == 0:
				#all ip go down, failover to backup config
				_query_name = ''.join(['_', _query_name])
				if self.rra.get(_query_name) is None:
					break  #backup go down too, just break and return empty answer
				self._rra = self.shift(self.rra.get(_query_name), self.rrn.get(_query_name))
                		self.rrn[_query_name] += 1
				self._ttl = self.ttl.get(_query_name)

		self.answer = str(self.q)
	
		#cache it, when expired at one ttl
		self.server.xcache[self.query_name] = [self.xcache_ttl + time.time(), self.answer]

	def x_send_out(self):
	  	#self._wlock.acquire()
		try:
			#send out answer, seems sending without mutex lock is ok
			self.sock.sendto(self.answer, self.addr)
		except:
			raise
		#finally:
			#self._wlock.release()

	def probe_ips(self):
		gs = []
		while 1:
			gevent.sleep(self.probe_interval)
			if len(self.stat) > 0:
				if len(gs) > 0:
					del gs[:]
				for ip in self.stat.keys():
					if time.time() > self.stat[ip][2] + self.stat[ip][1]:  #last-check + ttl
						gs.append(gevent.spawn(self.icmp_ping, ip))  #do works concurrently
				gevent.joinall(gs)

	def icmp_ping(self, ip):
		#be sure to be a gevent.socket, for concurrent reason
		sock = gevent.socket.socket(gevent.socket.AF_INET, gevent.socket.SOCK_RAW, 1) #raw socket requiries root privilege
		if StrictVersion(gevent.__version__) >= StrictVersion('1.0'):
			sock.connect((ip, None))
		else:
			sock.connect((ip, 1))
		sock.settimeout(1)
		rcvd = 0
		for i in xrange(2): #send icmp ping tests
			icmp = dpkt.icmp.ICMP(type=8, data=dpkt.icmp.ICMP.Echo(id=random.randint(0, 0xffff),
						seq=i, data='3nsd probe'))
			try:
				sock.send(str(icmp))
				r = sock.recv(1024)
				rip = dpkt.ip.IP(r)
				if gevent.socket.inet_ntoa(rip.src) == ip:
					ricmp = dpkt.icmp.ICMP(str(rip.data))
					if ricmp.type == 0:
						rcvd += 1
			except gevent.socket.timeout:
				pass
			if rcvd > 0:
				break
		sock.close()
		self.stat[ip][2] = time.time()
		if rcvd == 0: #ip down
			self.stat[ip][0] = False
			return False
		else: #ip alive
			self.stat[ip][0] = True
			return True


class _xZHandler(_xHandler, _xDNSHandler):

	Z_RR = 0  #round robin
	Z_IP = 1  #ip hash

	None2 = [None, None]

	_f = None #socket fileno

	z_cache_size = 1000  	#limit the xcache size in mem, about 30MB size with average file size 30KB
	z_idle_ttl = 20  	#backend connections have a idle timeout of 20 seconds

	z_xcache_shelf = False  #persistent storage of xcache
	z_shelf_size = 1000000  #1 million files on-disk cache, about 30GB disk size with average file size 30KB

	def __init__(self, conn, client_address, server, native_epoll=True,
					gevent_stream=False, recv_buf_size=16384, send_buf_size=65536, z_mode=0):
		_xHandler.__init__(self, conn, client_address, server, native_epoll, gevent_stream, recv_buf_size, send_buf_size)

		if z_mode >= 0:
			_xDNSHandler.__init__(self, conn, client_address, server, config_section='3zsd')
			self.z_mode = z_mode
		else:
			self.z_mode = 0

		if Xcache_shelf == 1:
			self.z_xcache_shelf = True
			self.server.xcache_shelf = shelve.open('xcache.shelf', writeback=True)

	def init_handler(self, conn, client_address, rw_mode=0):
		_xHandler.init_handler(self, conn, client_address, rw_mode)

		self.z_hostname = self.z_backends = self.z_host_addr = self.z_host_sock = None
		self.z_header_length = self.z_finished = self.z_body_size = self.chuncked_encoding = self.transfer_completed = 0
		self.response_headers_parsed = False

		if conn:
			if self._f in self.server.xcache_stat:
				self.xcache_hit = True
				self.xcache_key = self.server.xcache_stat[self._f]

			if rw_mode > 0:
				_n = conn.fileno()
				if _n in self.server.k_conns:
					self.r_http_ver, self.keep_connection = self.server.k_conns[_n]

				self.path = self.server.c_path.get(_n)
				self.z_hostname = self.server.zhosts.get(_n)
				self._r = self.server.z_reqs.get(_n)

				if rw_mode > 1:
					self.z_host_sock = self.server.zconns.get(self._f)
                        		self.z_host_addr = self.server.zaddrs.get(self._f)
		else:
			return

	def z_check_xcache(self, _f):
                if self.if_modified_since == 0:

			_key_found = False
			_in_shelf = False

			self.accept_encoding = self.in_headers.get("Accept-Encoding")
			if self.accept_encoding:
				for x in self.accept_encoding.replace(' ','').split(','):
					_key = ''.join([x, ':', self.xcache_key])
					if _key in self.server.xcache:
						_key_found = True
						self.xcache_key = _key
						break
					elif self.z_xcache_shelf and _key in self.server.xcache_shelf:
						_key_found = True
						_in_shelf = True
						self.xcache_key = _key
						break
			if not _key_found:
				if self.xcache_key in self.server.xcache:
					 _key_found = True
				elif self.z_xcache_shelf and self.xcache_key in self.server.xcache_shelf:
					 _key_found = True
					 _in_shelf = True

			if _key_found:
				if not _in_shelf:
                        		self._c = self.server.xcache.get(self.xcache_key)
				else:
                        		self._c = self.server.xcache[self.xcache_key] = self.server.xcache_shelf.get(self.xcache_key)
                        	ttl = self._c[0]
                        	if ttl >= time.time():
                                	#cache hit
                                	self.out_head_s, self.out_body_file, self.out_body_size, self.out_body_file_lmt, self.out_body_mmap = self._c[1:]
                                	self.has_resp_body = True
                                	self.xcache_hit = True
					self.server.xcache_stat[_f] = self.xcache_key
                                	return
                        	else:
                                	#cache item expired
                                	self._c = None
					if not _in_shelf:
                                		self.server.xcache.pop(self.xcache_key)
					else:
						try:
                                			del self.server.xcache_shelf[self.xcache_key]
						except:
							#may be problem in concurrent mode
							pass

					if _f in self.server.xcache_stat:
                                		self.server.xcache_stat.pop(_f)

			self.xcache_hit =False
		else:
			self.xcache_hit =False

	def __call__(self, fds):
		for f, rw_mode in fds:
			self._f = f
			_do_clean = True

			if rw_mode == 0:  #ccc
				#from client, resume_transfer = 0
				#print "0 c->s"   #client to server
				self.init_handler(self.server.conns[f], self.server.addrs[f], rw_mode)
                                parse_stat = self.x_parse()
                                if parse_stat == self.PARSE_OK:
					self.server.k_conns[f] = [self.r_http_ver, self.keep_connection]
					self.server.c_path[f] = self.path
					if self.cmd_get == 1 or self.cmd_head == 1:
						self.z_check_xcache(self._f)
						if self.xcache_hit:
							self.x_response()
						else:
							self.z_GET_init()
							_do_clean = False
					elif self.cmd_put == 1 or self.cmd_delete == 1:
						if hasattr(self, "z_PUT_init"):
							self.z_PUT_init()
							_do_clean = False
						else:
							self.xResult = self.xR_ERR_HANDLE
					else:
						self.xResult = self.xR_ERR_HANDLE
                                elif parse_stat == self.PARSE_AGAIN:
                                        self.xResult = self.xR_PARSE_AGAIN
                                        continue
                                else:
                                        self.xResult = self.xR_ERR_PARSE
					#client may closed connection, clean cb_conns
					self.server.cleanc(self._f)
			elif rw_mode == 1:
				#to client, resume_transfer = 1
				#print "1 s->c"  #server to client
				self.init_handler(self.server.conns[f], self.server.addrs[f], rw_mode)
				if self.xcache_hit:
					self.x_response()

					_cb = self.server.cb_conns.get(f)
					if _cb:
						_z_sock, _f = _cb
					else:
						 _z_sock, _f = self.None2

					if _z_sock:
						#print "xcache_hit clean cb_conns pair:", f, _f
						self.server.z_reqs_cnt[_f] -= 1
						if self.server.z_reqs_cnt[_f] == 0:
							#release z_conn & c_conn pair in cb_conns
							self.server.cb_conns[_f] =  None
							self.server.cb_conns[f] = None
			
							#add idle list
							if str(self.server.zaddrs[_f]) in self.server.zidles:
								if _f not in self.server.zidles[str(self.server.zaddrs[_f])]:
									self.server.zidles[str(self.server.zaddrs[_f])].insert(0, _f)
							else:
								self.server.zidles[str(self.server.zaddrs[_f])] = [_f]

							self.server.zconns_stat[_f] = [0, time.time()]  #conn idle

							#clean zcache
							self.server.zcache.pop(_f, None)
							self.server.zcache_stat.pop(_f, None)

				else:
					_do_clean = self.z_transfer_client(self._f)
			elif rw_mode == 2:
				#from backend, resume_transfer = 2
				#print "2 b->s"  #backend to server
				self.init_handler(self.server.zconns[f], self.server.zaddrs[f], rw_mode)
				parse_stat = self.z_transfer_backend(self._f)
				if parse_stat == self.PARSE_ERROR:
					self.server.cleanz(self._f)
				elif parse_stat == self.PARSE_AGAIN:
					self.z_hostname = self.server.zhosts[self._f]
					self.path = self.server.z_path[self._f]
					self._r = self.server.z_reqs[self._f]
					#print "z_conns", self.server.zconns
					#print "cb_conns", self.server.cb_conns
					#print "idle_zconn", self.server.zidles
					#print f
					_do_clean = False

					_cb = self.server.cb_conns.get(self._f)
					if _cb:
						_client_sock, _client_sock_fileno = _cb
					else:
						_client_sock, _client_sock_fileno = self.None2

					if not _client_sock:
						self.server.epoll.unregister(f)
						self.server.cleanz(self._f)
					else:
						self.server.cleanz(self._f)
						self.z_connect_backend(rw=2, client_sock=_client_sock)
						self.z_send_request_init()
			elif rw_mode == 3:
				#to backend, resume_transfer = 3
				#print "3 s->b"  #server to backend
				self.init_handler(self.server.zconns[f], self.server.zaddrs[f], rw_mode)
				self.z_GET_backend()
			else:
				self.xResult = self.xR_ERR_PARSE

			if _do_clean:
				self.transfer_completed = 1
				self.clean()

	def z_parse_address(self, addr):
		try:
			host, port = addr.split(':', 1)
			port = int(port)
		except:
			#return None
			return addr, 80
		return host, port

	def z_pick_a_backend(self):
		if self.z_mode == self.Z_RR:
			self.z_backends = self.shift(self.rra.get(self.z_hostname), self.rrn.get(self.z_hostname))
                	self.rrn[self.z_hostname] = (self.rrn[self.z_hostname] + 1) % len(self.rra.get(self.z_hostname))
			return self.z_backends[0]
		elif self.z_mode == self.Z_IP:
			pass

	def z_GET_backend(self):
		#resume send (large)request to backend
		self._r = self.server.z_reqs[self._f]
		self.z_send_request_resume(self._r, self._f)

	def z_connect_backend(self, rw=0, client_sock=None, addr=None, update_cb_conns=True):
		#print "connecting to backend" #bbb
		if addr:
			self.z_host_addr = addr
		else:
			self.z_host_addr = self.z_parse_address(self.z_pick_a_backend())

		self.z_host_sock = None

		while str(self.z_host_addr) in self.server.zidles and len(self.server.zidles[str(self.z_host_addr)]) > 0:
			#look for idle connection
			self.z_host_sock = self.server.zconns.get(self.server.zidles[str(self.z_host_addr)].pop(), None)
			_zsfn = self.z_host_sock.fileno()
			if _zsfn == -1 or time.time() - self.server.zconns_stat[_zsfn][1] > self.z_idle_ttl:
				self.z_host_sock = None
			else:
				break

		if not self.z_host_sock: 
			#the idle conn may be closed, make a new connection
			#self.z_host_sock = gevent.socket.create_connection(self.z_host_addr)
			self.z_host_sock = socket.socket()
			self.z_host_sock.settimeout(5)
			self.z_host_sock.connect(self.z_host_addr)
			self.z_host_sock.setblocking(0)

			self.z_host_sock.setsockopt(socket.SOL_SOCKET,socket.SO_KEEPALIVE,1)
			self.z_host_sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 20)
			self.z_host_sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 4)
			self.z_host_sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 5)

		#make client-backend socket pair, store them into cb_conns
		#identify backend socket self.z_host_sock with client socket self._f
		if update_cb_conns:
			if rw == 0:
				_c_sock_fileno = self._f
			elif rw == 2:
				_c_sock_fileno = client_sock.fileno()

			self.server.cb_conns[_c_sock_fileno] = [self.z_host_sock, self.z_host_sock.fileno()]

		self._f = self.z_host_sock.fileno()
		#now self._f is backend socket, identidy it with client socket

		if update_cb_conns:
			self.server.zconns_stat[self._f] = [1, -1]  #using backend connection

			if rw == 0:
				self.server.cb_conns[self._f] = [self.sock, self.sock.fileno()]
			elif rw == 2:
				self.server.cb_conns[self._f] = [client_sock, client_sock.fileno()]

		#print "z_connect_backend create cb_conns pair:", _c_sock_fileno, self._f
		#print "cb_conns:", self.server.cb_conns
		#print "get zconn:", self._f
		#print "self.sock:", self.sock

		self.server.zconns[self._f] = self.z_host_sock
		self.server.zaddrs[self._f] = self.z_host_addr
		self.server.zhosts[self._f] = self.z_hostname

	def z_send_request_init(self, no_recv=False):
		#init send request, _r for request headers, _f for socket fileno to backend
		self.server.z_reqs[self._f] = self._r
		#self.server.z_resp_header[self._f] = None
		self.server.z_path[self._f] = self.path

		if no_recv:
			self.z_host_sock.shutdown(socket.SHUT_RD)

		try: #sss
			sent = self.z_host_sock.send(self._r)
		except socket.error as e:
			return self.PARSE_ERROR

		if no_recv:
			self.z_host_sock.close()
			self.server.zconns.pop(self._f)
			self.server.zaddrs.pop(self._f)
			self.server.zhosts.pop(self._f)
		else:
			if self._f in self.server.z_reqs_cnt:
				self.server.z_reqs_cnt[self._f] += 1
			else:
				self.server.z_reqs_cnt[self._f] = 1

			if sent < len(self._r):
				self.server.z_reqs_stat[self._f] = sent
				try:
					self.server.epoll.register(self._f, select.EPOLLIN | select.EPOLLOUT)
				except IOError as e:
					self.server.epoll.modify(self._f, select.EPOLLIN | select.EPOLLOUT)
			else:
				if self._f in self.server.z_reqs_stat:
					self.server.z_reqs_stat.pop(self._f)
				try:
					self.server.epoll.register(self._f, select.EPOLLIN)
				except IOError as e:
					self.server.epoll.modify(self._f, select.EPOLLIN)

	def z_send_request_resume(self, _r, _f):
		#resume request sending
		if _f in self.server.z_reqs_stat:
			begin = self.server.z_reqs_stat[_f]
			sent = self.z_host_sock.send(_r[begin:])
			if begin + sent < len(_r):
				self.server.z_reqs_stat[_f] = begin + sent
			else:
				#all sent
				self.server.z_reqs_stat.pop(_f)
				self.server.epoll.modify(_f, select.EPOLLIN)

	def z_GET_init(self):
		#init connection to backend, send request, ggg
		_f = None
		try:
			self.z_hostname, _port = self.z_parse_address(self.in_headers.get("Host").lower())

			if self.z_hostname not in self.rra.keys():
				#not my serving hostname
				self.xResult = self.xR_ERR_HANDLE
				return

			_cb = self.server.cb_conns.get(self._f)
			if _cb:
				self.z_host_sock, _f = _cb
			else:
				self.z_host_sock, _f = self.None2

			if _f and self.server.zhosts.get(_f, None) == self.z_hostname:
				#print "z_GET_init remake cb_conns pair:", self._f, _f
				self._f = _f
				self.server.cb_conns[_f] = [self.sock, self.sock.fileno()]
			else:
				#print "z_GET_init new conn:", self._f, _f
				self.z_connect_backend()
				_f = self._f

			self.z_send_request_init()
		except:
			self.xResult = self.xR_ERR_HANDLE
			"""
			if _f:
				self.server.zaddrs.pop(_f)
				self.server.zconns[_f].close()
				self.server.zconns.pop(_f)
				self.server.z_reqs.pop(_f)
				self.server.c_path.pop(_f)
			"""

	def z_transfer_client(self, __f):
		#to client 222
		_cb = self.server.cb_conns.get(__f)
		if _cb:
			_c, _f = _cb
		else:
			_c, _f = self.None2

		if _f not in self.server.zcache_stat:
			return False

		blockno, begin = self.server.zcache_stat[_f][:2]
		if blockno == len(self.server.zcache[_f]):
			if self.server.zcache_stat[_f][4] == 1:
				#finished all sent
				self.server.epoll.modify(self.sock, select.EPOLLIN)
				if self.server.zcache_stat[_f][7] == self.HTTP_OK:
					if self.server.zcache_stat[_f][10] == 1 or self.server.zcache_stat[_f][5] > 0:
						#only 200 and chuncked or body_size > 0 response item moved to xcache
						#here may be wrong, should use self.cmd_get instead of self.server.zcache_stat[_f][5]
						self.zcache_to_xcache(_f)

				self.server.z_reqs_cnt[_f] -= 1

				if self.server.z_reqs_cnt[_f] == 0:
					#release z_conn & c_conn pair in cb_conns
					#print "z_transfer_client clean cb_conns pair:", __f, _f
					self.server.cb_conns[__f] =  None
					self.server.cb_conns[_f] = None
			
				_backend_sock = self.server.zconns.get(_f)
				if _backend_sock:
					if _backend_sock.fileno() == -1:
						#backend closed connection
						self.server.zconns.pop(_f)
						self.server.zconns_stat.pop(_f)
					else:
						#add idle list
						if str(self.server.zaddrs[_f]) in self.server.zidles:
							if _f not in self.server.zidles[str(self.server.zaddrs[_f])]:
								self.server.zidles[str(self.server.zaddrs[_f])].insert(0, _f)#add to idle list
						else:
							self.server.zidles[str(self.server.zaddrs[_f])] = [_f]

						self.server.zconns_stat[_f] = [0, time.time()]  #conn idle

				#clean zcache
				self.server.zcache.pop(_f)
				self.server.zcache_stat.pop(_f)
				#self.server.z_path.pop(_f)

				#finished
				return True

			else:
				#no more data yet or finished with no Content-Length? that's a problem.
				_backend_sock = self.server.zconns.get(_f)
				if _backend_sock:
					if _backend_sock.fileno() == -1:
						self.server.zcache_stat[_f][5] = self.server.zcache_stat[_f][2] - self.server.zcache_stat[_f][3]
						self.server.zcache_stat[_f][4] = 1
				return False

		if len(self.server.zcache[_f][blockno][begin:]) > self.send_buf_size:
			sent = self.sock.send(self.server.zcache[_f][blockno][begin:begin + self.send_buf_size])
			should_sent = self.send_buf_size
			self.server.zcache_stat[_f][1] += sent
		else:
			sent = self.sock.send(self.server.zcache[_f][blockno][begin:])
			should_sent = len(self.server.zcache[_f][blockno][begin:])
			self.server.zcache_stat[_f][0] += 1
			self.server.zcache_stat[_f][1] = 0

		return False

	def zcache_to_xcache(self, _f):
		#remember that only 200 and body_size > 0 response item will be moved to xcache

		_cc = self.server.z_resp_header[_f].get('Cache-Control')
		_exp = self.server.z_resp_header[_f].get('Expires')

		_ttl = 0
		_t = time.time()  #now

		if _cc:
			if "private" in _cc or "no-cache" in _cc:
				_ttl = -1
			elif "max-age=" in _cc:
				_age_s = ''
				_index = _cc.find('max-age=') + len('max-age=')
				while _cc[_index] in ['1','2','3','4','5','6','7','8','9','0']:
					_age_s = ''.join([_age_s, _cc[_index]])
					_index += 1
					if _index > len(_cc) - 1:
						break

				_ttl = _t + int(_age_s)
			else:
				if _exp:
					#Expires: Tue, 20 Oct 2015 04:27:25 GMT
					_ttl = calendar.timegm(time.strptime(_exp, '%a, %d %b %Y %H:%M:%S GMT'))
				else:
					_ttl = self.xcache_ttl + _t
		else:
			if _exp:
				_ttl = calendar.timegm(time.strptime(_exp, '%a, %d %b %Y %H:%M:%S GMT'))
			else:
				_ttl = self.xcache_ttl + _t

		if _ttl > _t:
			if len(self.server.xcache) > self.z_cache_size:
				self.server.xcache.popitem()

                	if self.r_http_ver == self.HTTP11:
                        	_resp_line = "HTTP/1.1 200 OK"
                	else:
                        	_resp_line = "HTTP/1.0 200 OK"

			try:
				self.server.z_resp_header[_f].pop("Connection")
			except:
				pass

			self.out_head_s = ''.join([_resp_line, '\n', '\n'.join(['%s: %s' % (k, v) for k, v in self.server.z_resp_header[_f].items()]), '\n'])

			_resp = ''.join(self.server.zcache[_f])
			self.out_body_file = _resp[self.server.zcache_stat[_f][3]:]
			self.out_body_size = len(self.out_body_file)

			_xcache_key = b''
			_content_encoding = self.server.z_resp_header[_f].get('Content-Encoding')

			if _content_encoding:
				_xcache_key = ''.join([_content_encoding, ':', self.server.zcache_stat[_f][9],self.server.zcache_stat[_f][8]])
			else:
				_xcache_key = ''.join([self.server.zcache_stat[_f][9],self.server.zcache_stat[_f][8]])

			self.server.xcache[_xcache_key] = [_ttl, self.out_head_s, self.out_body_file, self.out_body_size, self.out_body_file_lmt, self.out_body_mmap]

			if self.z_xcache_shelf:
				try:
					if len(self.server.xcache_shelf) > self.z_shelf_size:
						if hasattr(self.server.xcache_shelf.dict, 'first'):
							#dbhash format
							k, v = self.server.xcache_shelf.dict.first()
							del self.server.xcache_shelf[k]
						if hasattr(self.server.xcache_shelf.dict, 'firstkey'):
							#gdbm format
							del self.server.xcache_shelf[self.server.xcache_shelf.dict.firstkey()]
				except:
					pass

				try:
					while len(self.server.xcache_shelf.cache) > self.z_cache_size:
						self.server.xcache_shelf.cache.popitem()

					self.server.xcache_shelf[_xcache_key] = self.server.xcache[_xcache_key]
					if hasattr(self.server.xcache_shelf.dict, 'sync'):
						#self.server.xcache.dict is an anydbm object, mostly gdbm
						self.server.xcache_shelf.dict.sync()
				except:
					#may be problem in concurrent mode
					pass

	def z_transfer_backend(self, _f):
		#from backend ttt
		try:
			_b = self.z_host_sock.recv(self.recv_buf_size)
			if not _b:
				#peer closed connection?
				#print "no content recieved, trying reconnect"
				return self.PARSE_AGAIN
		except socket.error as e:
			if e.errno == errno.EAGAIN:
				#no more request data, see if the whole request headers should be recieved
				return self.PARSE_AGAIN
			else:
				#peer closed connection?
				#return self.PARSE_AGAIN
				return self.PARSE_ERROR

		#self.server.zcache_stat[_f] sample:
		#[2, 0, 25230, 217, 0, 25013, 1, 200, '/py/vms_rrd/vms-ping_day.png', 'vm0']
		if _f in self.server.zcache and self.server.zcache_stat[_f][4] == 0 and self.z_finished == 0:
			#continue recv
			"""
				self.server.zcache_stat[_f][2] total_size_recv
				self.server.zcache_stat[_f][3] header_length
				self.server.zcache_stat[_f][4] finished
				self.server.zcache_stat[_f][5] body_size
				self.server.zcache_stat[_f][6] keep_connection
				self.server.zcache_stat[_f][7] http_status_code
				self.server.zcache_stat[_f][8] path
				self.server.zcache_stat[_f][9] hostname
				self.server.zcache_stat[_f][10] chuncked encoding
			"""
			self.server.zcache[_f].append(_b)
			self.server.zcache_stat[_f][2] += len(_b) 

			if not self.response_headers_parsed:
				if self.EOL2 in _b or self.EOL1 in _b:
					#rebuild the response headers and check them
					self.parse_backend_response_headers(''.join(self.server.zcache[_f]), _f)
					self.server.zcache_stat[_f][3] = self.z_header_length
					self.server.zcache_stat[_f][6] = self.keep_connection
					self.server.zcache_stat[_f][7] = self.resp_code

			if self.server.zcache_stat[_f][5] > 0:
				if self.server.zcache_stat[_f][2] == self.server.zcache_stat[_f][5] + self.server.zcache_stat[_f][3]:
					#finished content-length
					self.server.zcache_stat[_f][4] = 1
					self.z_finished = 1
			elif self.server.zcache_stat[_f][5] == 0 and "0\r\n\r\n" in _b:
				#finished chunked encoding
				self.server.zcache_stat[_f][4] = 1
				self.z_finished = 1

		elif self.z_finished == 0:
			#first recv
			_path = self.server.z_path[_f]
			_z_hostname = self.server.zhosts[_f]
			self.server.zcache[_f] = [_b]

			#zcache_stat format: [block num, size sent, total_size_recv, header_length, finished, body_size, keep_connection,resp_code,path,hostname,chuncked_encoding]
			if self.EOL2 in _b or self.EOL1 in _b:
				self.parse_backend_response_headers(_b, _f)
				self.server.zcache_stat[_f] = [0, 0, len(_b), self.z_header_length, self.z_finished, self.z_body_size, self.keep_connection, self.resp_code, _path, _z_hostname, self.chuncked_encoding]
			else:
				self.server.zcache_stat[_f] = [0, 0, len(_b), -1, 0, -1, -1, 0,  _path, _z_hostname, self.chuncked_encoding]

			_cb = self.server.cb_conns.get(_f)
			if _cb:
				_c_sock, _c_sock_no = _cb
			else:
				_c_sock, _c_sock_no = self.None2

			if _c_sock: 
				if _c_sock_no in self.server.xcache_stat:
					#clear xcache_stat to avoid cache hit before
					self.server.xcache_stat.pop(_c_sock_no, None)
				self.server.epoll.modify(_c_sock, select.EPOLLIN | select.EPOLLOUT)
			#else:
				#at this point, the last request of client should completed and cb_conns cleaned
                                #may be safe to ignore it, but if this request is different from the last? ....
				#print "cb_conns:", self.server.cb_conns
				#print "f:", _f, "zcache_stat:", self.server.zcache_stat, "z_reqs_cnt:", self.server.z_reqs_cnt

		if self.z_finished == 1:
			self.server.epoll.unregister(_f)

			return self.PARSE_OK
		else:
			return self.PARSE_MORE

	def parse_backend_response_headers(self, _b, _f):
		#cut headers out ppp
		b = _b.split(self.EOL2, 1)
		sp = len(self.EOL2)

		if not b[0]:
			b = _b.split(self.EOL1, 1)
			sp = len(self.EOL1)
			if not b[0]:
				#illeagal response headers
				return self.PARSE_ERROR

		self.z_header_length = len(b[0]) + sp

		a = b[0].strip().split("\r\n", 1)
		if not a[0]:
			return self.PARSE_ERROR

		#"HTTP/1.1 200 OK"
		_c_http_ver, _resp_code_str, self.resp_msg = a[0].split(None, 2)

		self.resp_code = int(_resp_code_str)
		if self.resp_code == self.HTTP_OK:
			self.has_resp_body = True

		self.server.z_resp_header[_f] = dict((k, v) for k, v in (item.split(": ") for item in a[1].split("\r\n")))
		self.in_headers = dict((k, v) for k, v in (item.split(": ") for item in a[1].split("\r\n")))

		try:
			self.z_finished = 0
			cl = self.in_headers.get("Content-Length")
			if cl:
				self.z_body_size = int(cl)
				if len(b[1]) == self.z_body_size:
					self.z_finished = 1
			else:
				c1 = self.in_headers.get("Transfer-Encoding")
				if c1:
					if c1.lower()== "chunked":
						self.chuncked_encoding = 1
						self.z_body_size = 0
						if "0\r\n\r\n" in b[1]:
							self.z_finished = 1
					else:
						self.z_body_size = -1
				else:
					if self.z_host_sock.fileno() == -1:
						#backend closed connection, transfer finished
						self.z_body_size = len(_b) - self.z_header_length
						self.z_finished = 1
					elif self.resp_code > self.HTTP_OK:
						self.z_body_size = 0
						self.z_finished = 1
					else:
						self.z_body_size = 0
		except:
			self.z_body_size = -1
			self.z_finished = 0

		self.check_connection(_c_http_ver, check_ims=False, gen_xcache_key=False)
		self.response_headers_parsed = True
		self.server.k_conns[_f] = [self.r_http_ver, self.keep_connection]

		#mangle the http status line and "Connection:" header to fit client side
		#_b = self.server.zcache[_f][0]  #headers in block 0

		#print "_f:", _f, "cb_conns:", self.server.cb_conns, "k_conns", self.server.k_conns
		try:
			__rc_http_ver, _c_keep_connection = self.server.k_conns[self.server.cb_conns[_f][1]]
			if __rc_http_ver == 1:
				_rc_http_ver = "HTTP/1.1"
			else:
				_rc_http_ver = "HTTP/1.0"
		except:
			_rc_http_ver = "HTTP/1.0"
			_c_keep_connection = 0

		if _c_http_ver != _rc_http_ver:
			if "HTTP/1.1" in self.server.zcache[_f][0]:
				self.server.zcache[_f][0] = self.server.zcache[_f][0].replace("HTTP/1.1", "HTTP/1.0", 1)
			elif "HTTP/1.0" in self.server.zcache[_f][0]:
				self.server.zcache[_f][0] = self.server.zcache[_f][0].replace("HTTP/1.0", "HTTP/1.1", 1)

		if _c_keep_connection != self.keep_connection:
			if "Connection: keep-alive" in self.server.zcache[_f][0]:
				self.server.zcache[_f][0] = self.server.zcache[_f][0].replace("Connection: keep-alive", "Connection: close", 1)
				self.z_header_length -= 5
			elif "Connection: close" in self.server.zcache[_f][0]:
				self.server.zcache[_f][0] = self.server.zcache[_f][0].replace("Connection: close", "Connection: keep-alive", 1)
				self.z_header_length += 5

	def out_conns_stats(self):
			print "--------------------------------------------------------------------------"
			print "zconns:", self.server.zconns
			print "zconns_stat:", self.server.zconns_stat
			print "zidles:", self.server.zidles
			print "cb_conns:", self.server.cb_conns
			print "--------------------------------------------------------------------------"

	def check_zconns(self):
		while 1:
			gevent.sleep(10)
			for f in self.server.zconns.keys():
				if f in self.server.zconns_stat:
					_t = self.server.zconns_stat[f][1]
					if time.time() - _t > self.z_idle_ttl:
						#idle time out, clean it
						if self.server.zidles:
							for _host in self.server.zidles.keys():
								if self.server.zidles[_host]:
									try:
										self.server.zidles[_host].remove(f)
									except:
										pass
						_sock = self.server.zconns[f]
						self.server.cleanz(f)
						_sock.close()

class _xDFSHandler(_xZHandler):

	DFS_PROXY_MODE = 0
	DFS_DIRECT_MODE = 1

	d_mode = 0

	dfs_config = None

	dfs_stage = 0
	dfs_redundancy = 1
	dfs_region = 4096
	dfs_prefix = '_3fs'
	dfs_prefix_s = '/_3fs_'

	dfs_pool = {}
	dfs_pool_count = {}

	dfs_writer = []

	file_stage = 0
	file_path = b''
	file_md5 = b''

	peer_ip_s = b''

	def __init__(self, conn, client_address, server, native_epoll=True,
					gevent_stream=False, recv_buf_size=16384, send_buf_size=65536, d_mode=0):
		_xZHandler.__init__(self, conn, client_address, server, native_epoll, gevent_stream,
										recv_buf_size, send_buf_size, -1)
		self.init_dfs_config()
		self.d_mode = d_mode

	def init_dfs_config(self):
		try:
                	self.dfs_config = ConfigParser.ConfigParser()
                	if not self.dfs_config.read('3xsd.conf'):
				self.dfs_config.read('/etc/3xsd.conf')

			for name, value in self.dfs_config.items('3fsd'):
				if name == 'stage':
					self.dfs_stage = int(value)
				elif name == 'redundancy':
					self.dfs_redundancy = int(value)
				elif name == 'region':
					self.dfs_region = int(value)
					self.dfs_region_mask = 1
					while self.dfs_region/(16**self.dfs_region_mask) > 1:
						self.dfs_region_mask += 1
				elif name == 'prefix':
					self.dfs_prefix = value
					self.dfs_prefix_s = ''.join(['/', self.dfs_prefix, '_'])
				elif name == 'write_permit':
					self.ttl['3fs_writer'] = 0
					self.dfs_writer = self.ip_list('3fs_writer', value, '3nsd')
				else:
					#must be a pool config of a domain_name
					#3xsd.net = 0,10.37.10.1-2:80,10.38.10.2:80;1,10.41.0.1-2:8000
                	               	self.ttl[name] = self.rrn[name] = 0
					self.rra[name] = []
					self.dfs_pool[name] = {}
					self.dfs_pool_count[name] = {}

					for item in value.split(';'):
						if item:
							_stage_s, _ip_s = item.split(',', 1)
							if _stage_s and _ip_s:
								#dfs_pool['3xsd.net'][0] = ['10.37.10.1:80', '10.37.10.2:80', '10.38.10.2:80']
								#dfs_pool['3xsd.net'][1] = ['10.41.0.1:8000', '10.41.0.2:8000']
								if self.dfs_pool[name].get(int(_stage_s)):
									self.dfs_pool[name][int(_stage_s)] += self.ip_list(name, _ip_s, '3fsd')
								else:
									self.dfs_pool[name][int(_stage_s)] = self.ip_list(name, _ip_s, '3fsd')

					for i in self.dfs_pool[name]:
						#to gen a fixed sorted server list, important for locating algorithm
						self.dfs_pool[name][i].sort()
						self.dfs_pool_count[name][i] = len(self.dfs_pool[name][i])

						#rra pool for RoundRobin
						self.rra[name] += self.dfs_pool[name][i]

					if self.rra.get(name):
						#make list distinct and sorted
						self.rra[name] = list(set(self.rra[name]))
						self.rra[name].sort()
						#print self.rra[name]
		except:
			raise

		#print "stage:", self.dfs_stage, ", redundancy:", self.dfs_redundancy, ", region:", self.dfs_region, ", prefix:", self.dfs_prefix, ", pool:", self.dfs_pool

	def init_handler(self, conn, client_address, rw_mode=0):
		_xZHandler.init_handler(self, conn, client_address, rw_mode)

		#do 3fsd specific initiation
		self.file_stage = 0
		self.file_path = self.file_md5 = b''

	def z_pick_a_backend(self, return_all=False):
		#self.z_hostname self.path self._r should be setup
		if self.dfs_prefix_s == self.path[:len(self.dfs_prefix_s)]:
			#it's a dfs access
			return self.dfs_locate_backend(self.z_hostname, return_all)
		else:
			return _xZHandler.z_pick_a_backend(self)

	def dfs_locate_backend(self, hostname, return_all=False):
		#3fs locating algorithm
		#url example: http://hostname/_3fs_0/path/to/file, 0 for stage
		#/path/to/file will be used to calculate out a standard md5 hex_string of 32 letters with lower case
		#/path/to/file -> b4a91649090a2784056565363583d067
		_fstage_s, _fpath = self.path[len(self.dfs_prefix_s):].split('/', 1)

		self.file_stage = int(_fstage_s)
		self.file_path = ''.join(['/', _fpath])

		md5 = hashlib.md5()
		md5.update(self.file_path)
		self.file_md5 = md5.hexdigest()

		i = 0
		_ret = []
		while self.dfs_redundancy - i > 0:
			_point = int(self.file_md5[self.dfs_region_mask*i:self.dfs_region_mask*(i+1)], base=16)
			_serno = __serno = int(_point / float(self.dfs_region / self.dfs_pool_count[hostname][self.dfs_stage]))
			while self.dfs_pool[hostname][self.dfs_stage][_serno] in _ret:
				#make sure the redundancy copys not in same server
				_serno = ( _serno + 1 ) % self.dfs_pool_count[hostname][self.dfs_stage]
				if _serno == __serno:
					break

			_ret.append(self.dfs_pool[hostname][self.dfs_stage][_serno])
			i += 1

		if return_all:
			return _ret
		else:
			return _ret[random.randint(0, self.dfs_redundancy - 1)]

	def z_GET_init(self):
		#init connection to backend, send request, ggg
		_f = None
		try:
			self.z_hostname, _port = self.z_parse_address(self.in_headers.get("Host").lower())

			if self.z_hostname not in self.rra.keys():
				#not my serving hostname
				self.xResult = self.xR_ERR_HANDLE
				return

			_cb = self.server.cb_conns.get(self._f)
			if _cb:
				self.z_host_sock, _f = _cb
			else:
				self.z_host_sock, _f = self.None2

			if _f and self.server.zhosts.get(_f, None) == self.z_hostname and self.dfs_prefix_s != self.path[:len(self.dfs_prefix_s)]:
				#print "z_GET_init remake cb_conns pair:", self._f, _f
				self._f = _f
				self.server.cb_conns[_f] = [self.sock, self.sock.fileno()]
			else:
				#print "z_GET_init new conn:", self._f, _f
				self.z_connect_backend()
				_f = self._f

			self.z_send_request_init()
		except:
			self.xResult = self.xR_ERR_HANDLE

	def z_PUT_init(self):
		try:
			self.z_hostname, _port = self.z_parse_address(self.in_headers.get("Host").lower())

			if self.z_hostname not in self.rra.keys():
				#not my serving hostname
				self.xResult = self.xR_ERR_HANDLE
				return

			if self.dfs_prefix_s == self.path[:len(self.dfs_prefix_s)]:
				#only 3fs access allow PUT/DELETE action in z_lbs & x_dfs mode
				try:
					self.peer_ip_s, _port_s = self.sock.getpeername()
				except:
					self.peer_ip_s = b''

				if self.peer_ip_s not in self.dfs_writer:
					self.xResult = self.xR_ERR_HANDLE
					return

				_backends = self.z_pick_a_backend(return_all=True)
				_b_index = 0
				for _b in _backends:
					self.z_host_addr = self.z_parse_address(_b)
					self.z_host_sock = None
					if _b_index == 0:
						self.z_connect_backend()
						self.z_send_request_init()
					else:
						self.z_connect_backend(addr=self.z_host_addr, update_cb_conn=False)
						self.z_send_request_init(no_recv=True)
					_b_index += 1
			else:
				self.xResult = self.xR_ERR_HANDLE

		except:
			self.xResult = self.xR_ERR_HANDLE
