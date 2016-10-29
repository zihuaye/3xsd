# _3xsd module
#
# 3xsd is a native epoll server serving TCP/UDP connections, a high performance static web server, a failover dns server,
# a http-based distributed file server, a load-balance proxy-cache server, and a 'warp drive' server.
#
# The xHandler to handle web requests, and the xDNSHandler to handle DNS query.
# The xZHandler to handle proxy(load-balance/cache)requests, and the xDFSHandler to handle DFS requests.
# The xWHandler to handle tunneling data.
#
# Author: Zihua Ye (zihua.ye@qq.com, zihua.ye@gmail.com)
#
# Copyright (c) 2014-2015, licensed under GPLv2.
#
# All 3 party modules copyright go to their authors(see their licenses).
#

__version__ = "0.0.26"

import os, sys, io, time, calendar, random, multiprocessing, threading
import shutil, mmap, sendfile, zlib, gzip, lzo, copy, setproctitle
import _socket as socket
import select, errno, gevent, dpkt, ConfigParser, hashlib, struct, shelve
import pytun, udt4, subprocess, fcntl, geoip2.database

from distutils.version import StrictVersion
from datetime import datetime
from collections import deque
from gevent.server import StreamServer
from gevent.coros import Semaphore
from udt4 import pyudt as udt

from Crypto.Cipher import AES
from Crypto.Cipher import Blowfish
from Crypto.Util import Counter
from Crypto import Random

Port = 8000	#Listening port number
Backlog = 1000	#Listening backlog
Conns = None	#gevent.pool type, connection limit
Workers = 0	#workers to fork, 0 for no worker
Homedir = None	#3xsd working home(document root)
Handler = None	#3xsd handler name, not an instance
Server = None	#3xsd server instance, init at startup
X_shelf = 0	#persistent storage of xcache(3zsd&3fsd)
Z_mode = 0	#0 - RR(default), 1 - IP Hash, 2 - URL Hash, for 3zsd
_Name = '3xsd'	#program name, changes at startup

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
		#obsolete
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
	gzip_shelf_lock = None
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
	xcache_shelf_lock = None

	#for 3wdd - warp drive
	zsess = {}    #session <-> (tun, udt socket)
	ztuns = {}    #tunnels & fd
	s_tuns = {}   #sessions connected with tuns
	s_udts = {}   #sessions connected with udts
	upolls = []   #the udt socket & tun epolls

	udt_send_buf = {}
	udt_thread_limit = 0
	udt_conns_cnt = {}
	udt_conns_cnt_lock = None
	udt_conn_port = None
	udt_conn_port_lock = None

	def __init__(self, server_address, RequestHandlerClass, backlog=1000, spawn=None,
							tcp=True, udt=False, recv_buf_size=16384, send_buf_size=65536):
		if tcp:
			self.recv_buf_size = recv_buf_size
			self.send_buf_size = send_buf_size
		elif udt:
			#this buffer size is about 100Mbps bandwidth between CN&US(Bandwidth*RTT/8)
			self.recv_buf_size = 2760000
			self.send_buf_size = 2760000

			#udt not work with reuse_port option
			self.reuse_port = False

			#self.udt_thread_limit = multiprocessing.cpu_count()
			self.udt_thread_limit = 1  #set thread_limit to 1 for the GIL

			self.udt_conns_cnt_lock = multiprocessing.Lock()
			for i in xrange(Workers + 1):
				self.udt_conns_cnt[i] = multiprocessing.Value('i', 0, lock=self.udt_conns_cnt_lock)

			self.udt_conn_port_lock = multiprocessing.Lock()
			self.udt_conn_port = multiprocessing.Value('i', Port, lock=self.udt_conn_port_lock)
		else:
			self.recv_buf_size = 65536
			self.send_buf_size = 65536
		self.tcp = tcp
		self.udt = udt
		StreamServer.__init__(self, server_address, RequestHandlerClass, backlog=backlog, spawn=spawn)
		self.handler = RequestHandlerClass(None, None, self)
		if hasattr(socket, "SO_REUSEPORT"):
			print("Good, this kernel has SO_REUSEPORT support")

	def master_works(self):
		if hasattr(socket, "SO_REUSEPORT") and self.reuse_port and not self.udt:
			#close master process's listening socket, because it never serving requests
			self.socket.close()

	def pre_start(self):
		self.init_socket(tcp=self.tcp)

	def set_socket_buf(self):
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.recv_buf_size)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.send_buf_size)

	def init_socket(self, tcp=True):
		if tcp:
			if self.server_mode == 'z_lbs' or self.server_mode == 'x_dfs':
				self.zlb_mode = True
			self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
		else:
			if not self.udt:
				#i_dns mode
				socket.socket = gevent.socket.socket
				self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

		if not self.udt:
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
			self.socket_fileno = self.socket.fileno()
			if tcp:
				self.socket.listen(self.backlog)
			else:
				self.handler.sock = self.socket
		else:
			if self._worker_id == 0 and self.workers > 0:
				#delay socket init for udt worker
				self.socket = None
				return

			udt4.startup()
			if self.handler.wdd_mode == 'server' or self.handler.wdd_mode == 'hybird':
				self.socket = udt.UdtSocket()
				#if self.reuse_addr == 1:
				#	self.socket.setsockopt(udt4.UDT_REUSEADDR, True) #default on
				#self.socket.setsockopt(udt4.UDT_MSS, 9000)    #default 1500
				self.socket.setsockopt(udt4.UDT_RCVBUF, self.recv_buf_size) #default 10MB
				self.socket.setsockopt(udt4.UDT_SNDBUF, self.send_buf_size) #default 10MB

				if self.workers > 0:
					_ip, _port = self.address
					_port = _port + self._worker_id - 1
					self.address = (_ip, _port)
				self.socket.bind(self.address)
				self.socket.listen(self.backlog)
			else:
				self.socket = None

	def cleanupall(self):
		if self.tcp:
			self.epoll.unregister(self.socket.fileno())
			self.epoll.close()
		self.conns.clear()
		self.addrs.clear()
		self.xcache.clear()
		self.x_reqs.clear()
	
	def cleanup(self, fd):
		try:
			self.epoll.unregister(fd)
			self.conns[fd].close()
		except:
			pass
		try:
			self.conns.pop(fd)
			self.addrs.pop(fd)
			self.x_reqs.pop(fd)
		except:
			pass
	
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
			self.cb_conns.pop(fd, None)
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

	def clean_zidles(self, fd):
		for _host, _idle_list in self.zidles.iteritems():
			if fd in _idle_list:
				self.zidles[_host].remove(fd)

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

	def o_udts(self):
		print "-------------------------------------"
		print "conns", self.conns
		print "zsess", self.zsess
		print "ztuns", self.ztuns
		print "s_tuns", self.s_tuns
		print "s_udts", self.s_udts
		print "upolls", self.upolls
		print "udt_send_buf", self.udt_send_buf

	def o_mem(self):
		#if os.path.exists('/tmp/3wdd_dumpit'):
		#	scanner.dump_all_objects('/tmp/3wdd_dump.txt')
		pass

	def check_3ws(self):
		while 1:
			try:
				if self.handler.gzip_on and self.gzip_shelf and self.gzip_shelf.cache:
					_over = len(self.gzip_shelf.cache) - 1000
					_delx = int(_over/8)
					if _over > 1000:
						self.gzip_shelf.cache.clear()
					elif _over > 0 and _delx > 9:
						while _delx > 0:
							self.gzip_shelf.cache.popitem()
							_delx -= 1

					if hasattr(self.gzip_shelf.dict, 'sync'):
						#self.gzip_shelf.dict is an anydbm object, mostly gdbm or bsddb
						with self.gzip_shelf_lock:
							self.gzip_shelf.dict.sync()
				#print self.xcache
			except:
				pass
			time.sleep(15)

	def get_tcp_stat(self, sock):
		_fmt = "B"*7+"I"*21
		_x = struct.unpack(_fmt, sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 92))
		return _x[0]


	def check_lbs(self):
		while 1:
			try:
				#maintain mem caches & shelfies
				if self.handler.z_xcache_shelf and self.xcache_shelf and self.xcache_shelf.cache:
					_over = len(self.xcache_shelf.cache) - self.handler.z_cache_size
					_delx = int(_over/8)
					if _over > self.handler.z_cache_size:
						self.xcache_shelf.cache.clear()
					elif _over > 0 and _delx > 9:
						while _delx > 0:
							self.xcache_shelf.cache.popitem()
							_delx -= 1

					if hasattr(self.xcache_shelf.dict, 'sync'):
						#self.xcache_shelf.dict is an anydbm object, mostly gdbm or bsddb
						with self.xcache_shelf_lock:
							self.xcache_shelf.dict.sync()

				#maintain backend conns
				#print "------------------------------------------"
				#print "conns:", self.conns
				#print "zconns:", self.zconns
				#print "cb_conns:", self.cb_conns
				#print "zidles:", self.zidles
				_keys = self.zconns.keys()
				if _keys:
					for _f in _keys:
						if self.zconns[_f].fileno() == -1 or self.get_tcp_stat(self.zconns[_f]) != 1:
							#connection not in ESTABLISHED stat, being closed
							self.clean_zidles(_f)
							self.cleanz(_f)

				_keys = self.conns.keys()
				if _keys:
					for _f in _keys:
						if self.conns[_f].fileno() == -1:
							#client connection being closed
							self.conns.pop(_f)
							self.cb_conns.pop(_f)
			except:
				pass
			time.sleep(15)

	def handle_event(self, events):
		for f, ev in events:
			if f == self.socket_fileno:
			    #new connection..
				try:
				#multi workers to accept same connection, only one can get it
					conn, addr = self.socket.accept()
				except:
					continue
				c = conn.fileno()
				#conn.setblocking(0)
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
		elif self.udt and self.workers > 0:
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
			gevent.spawn(self.handler.probe_ips)	#a separate greenlet to perform ip stat checking
			while 1:
				gevent.sleep(1e-20)		#a smallest float, works both at v0.13.8 and v1.0.x
				_events = self.epoll.poll(10)	#long-timeout helping cpu% lower
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
			t = threading.Timer(15, self.check_lbs)
			t.start()

			while 1:
				self.handle_event(self.epoll.poll())
		finally:
			self.cleanupall()
			self.socket.close()		

	def serve_wds(self): #www
		try:
			s = 0
			while s < self.udt_thread_limit:
				self.upolls.append(None)
				s += 1

			if self.handler.wdd_mode == 'client' or self.handler.wdd_mode == 'hybird':
				if 1:
					_idx = -1
					for _session in self.handler.wdd_dial:
						_idx += 1
						if self.workers > 1 and (_idx % self.workers) + 1 != self._worker_id:
							continue
						if _session in self.handler.client_session:
							t = threading.Thread(target=self.handler.connect_udt_server, args=(_session,))
							t.daemon = True
							t.start()

			t = threading.Thread(target=self.check_3wd, args=())
			t.daemon = True
			t.start()

			if self.handler.wdd_mode == 'server' or self.handler.wdd_mode == 'hybird':
				self.if_reinit_socket()
				while 1:
					#accept incoming udt connection
					_conn, _addr = self.socket.accept()
					#launch setting up udt_tunnel
					t = threading.Thread(target=self.handler.setup_udt_connection, args=(_conn,_addr,))
					t.start()

			if self.handler.wdd_mode == 'client':
				while 1:
					time.sleep(1000)
		except:
			raise
		finally:
			if self.socket:
				self.socket.close()		

	def serve_3ws(self):
		self.if_reinit_socket()
		self.epoll = select.epoll()
		self.epoll.register(self.socket.fileno(), select.EPOLLIN)

		try:
			t = threading.Timer(15, self.check_3ws)
			t.start()

			while 1:
				self.handle_event(self.epoll.poll())
		finally:
			self.cleanupall()
			self.socket.close()		

	def handle_event_udt_tun(self, index):
		try:
			while 1:
				self.handler.handle_udt_tun_events(self.upolls[index].wait(True, True, -1, True))
		except:
			if self.upolls[index]:
				self.upolls[index] = None
			raise

	def forward_tun_udt(self, _tun, _usock, _encrypt_mode, _compress, _session):  #uuu

		_zip = lambda s : eval(_compress).compress(s) if _compress and len(s) < _tun.mtu - 100 else s
		_repack = lambda s : ''.join([struct.pack('!H', len(s)), s])
		_forward_it=lambda s : _usock.send(_repack(self.handler.encrypt_package(_zip(s), _encrypt_mode, _session))) if _encrypt_mode else _usock.send(_repack(_zip(s)))

		try:
			while 1:
				r = [_tun]; w = []; x = []; _b = None
				r, w, x = select.select(r, w, x, 6.0)
				if r:
					_forward_it(_tun.read(_tun.mtu))
				else:
					if _tun.fileno() == -1:
						#tunnel down
						print "Thread forward_tun_udt of tunnel:", _session, "exit.."
						break
		except:
			print "Thread forward_tun_udt of tunnel", _session, "exit.."
			raise

	def forward_udt_tun(self, _tun, _usock, _encrypt_mode, _compress, _session):  #ttt

		_magic = {'zlib':(''.join([chr(0x78), chr(0x9c)]), 2), 'lzo':(''.join([chr(0xf0), chr(0x0), chr(0x0)]), 3)}
		_unzip = lambda s : eval(_compress).decompress(s) if _compress and _magic[_compress][0] in s[:_magic[_compress][1]] else s
		_forward_it = lambda s : _tun.write(_unzip(self.handler.decrypt_package(s, _encrypt_mode, _session))) if _encrypt_mode else _tun.write(_unzip(s))

		try:
			while 1:
				_forward_it(_usock.recv(struct.unpack('!H', _usock.recv(2))[0]))
		except IOError as e:
			if e.errno == errno.EINVAL:
				#illegal data, maybe tunnel peer shutdown suddenly
				_usock.close()
			print "Thread forward_udt_tun of tunnel", _session, "exit.."
			raise
		except:
			print "Thread forward_udt_tun of tunnel", _session, "exit.."
			raise

	def forward_udt_relay(self, _usock, _session):
		_repack = lambda s : ''.join([struct.pack('!H', len(s)), s])

		try:
			_from, _to = self.handler.udt_relay[_session]
			_relay_session = None
			_first = True
			while not _relay_session:
				if _session == _from:
					self.handler.udt_relay_thread_stat[_from] = True
					_relay_session = self.zsess.get(_to, None)
				else:
					self.handler.udt_relay_thread_stat[_to] = True
					_relay_session = self.zsess.get(_from, None)

				if _first:
					_first = False
				else:
					time.sleep(5)
			else:
				_to_usock = _relay_session[1]
				while 1:
					_to_usock.send(_repack(_usock.recv(struct.unpack('!H', _usock.recv(2))[0])))
		except:
			if _session == _from:
				self.handler.udt_relay_thread_stat[_from] = False
			else:
				self.handler.udt_relay_thread_stat[_to] = False

			print "Thread forward_udt_relay of tunnel", _session, "exit.."
			raise

	def check_3wd(self): #333
		try:
			while 1:
				time.sleep(20)

				_tun, _usock, _addr = [None, None, None]
				if (self.handler.wdd_mode == 'client' or self.handler.wdd_mode == 'hybird'):
					_idx = -1
					for _session in self.handler.wdd_dial:
						_idx += 1
						if self.workers > 1 and (_idx % self.workers) + 1 != self._worker_id:
							continue
						if _session in self.handler.client_session:
							_redial = False
							_tun, _usock, _addr = self.zsess.get(_session, (None, None, None))
							if _usock:
								#{INIT = 1, OPENED, LISTENING, CONNECTING, CONNECTED, BROKEN, CLOSING, CLOSED, NONEXIST}
								if _usock.getsockstate() > 5:	#connection gone
									self.handler.destroy_tunnel(_session)
									_redial = True
								else:
									self.handler.tun_rtt[_session]= _usock.perfmon().msRTT
							else:
								#must connect failed before
								_redial = True

							if _redial:
								t = threading.Thread(target=self.handler.connect_udt_server, args=(_session,))
								t.daemon = True
								t.start()

				if self.handler.wdd_mode == 'server' or self.handler.wdd_mode == 'hybird':
					for _session in self.handler.connected.keys():
						if _session not in self.handler.wdd_dial or _session not in self.handler.client_session:
							#server connection
							_tun, _usock, _addr = self.zsess.get(_session, (None, None, None))
							if _usock:
								if _usock.getsockstate() > 5:	#connection gone
									self.handler.destroy_tunnel(_session)
								else:
									self.handler.tun_rtt[_session]= _usock.perfmon().msRTT

							if os.path.exists('/tmp/usock_stat'):
								udt4.dump_perfmon(_usock.perfmon())

					if self.workers > 1:
						self.wdd_idle_worker(9000)

				for _session in self.handler.udt_relay:
					if _session in self.handler.udt_relay_thread_stat and not self.handler.udt_relay_thread_stat[_session]:
						#relaunch the udt relay thread, due to one side may be downed before
						_tun, _usock, _addr = self.zsess.get(_session, (None, None, None))
						if _usock:
							print "Re-launching relay tunnel", _session
							if self.handler.io_mode == self.handler.IO_NONBLOCK:
								_n = _usock.UDTSOCKET.UDTSOCKET % self.udt_thread_limit
								if self.upolls[_n] is None:
									self.upolls[_n] = udt.Epoll()
									self.upolls[_n].add_usock(_usock, udt4.UDT_EPOLL_IN)
									t = threading.Thread(target=self.handle_event_udt_tun, args=(_n,))
									t.daemon = True
									t.start()
								else:
									self.upolls[_n].add_usock(_usock, udt4.UDT_EPOLL_IN)
								self.handler.udt_relay_thread_stat[_session] = True
							else:
								t = threading.Thread(target=self.forward_udt_relay,args=(_usock,_session,))
								t.daemon = True
								t.start()

				if self.handler.routing_metric:
					with open(os.devnull, 'w') as devnull:
						for _route in self.handler.route_metric:
							if len(self.handler.route_metric[_route]) > 1:
								_target_session = None
								_target_session_rtt = -1
								_fixed_metric = 0  #0 for dynamic, >0 for fixed
								for _session in self.handler.route_metric[_route]:
									if _route in self.handler.route_metric_fixed:
										if _session in self.handler.route_metric_fixed[_route]:
											_fixed_metric = self.handler.route_metric_fixed[_route][_session]
										else:
											_fixed_metric = 0
									if _session in self.handler.tun_rtt:
										_rtt_old=self.handler.route_metric[_route][_session]
										_rtt = self.handler.route_metric[_route][_session] = int(self.handler.tun_rtt[_session] * 10)
										if _rtt == 0:
											_rtt = self.handler.route_metric[_route][_session] = 1
										if _target_session_rtt == -1:
											if _fixed_metric > 0:
												_target_session_rtt = _fixed_metric
											else:
												_target_session_rtt = _rtt
											_target_session = _session
										else:
											if _fixed_metric > 0:
												if _target_session_rtt > _fixed_metric:
													_target_session_rtt = _fixed_metric
													_target_session = _session
											elif _target_session_rtt > _rtt:
												_target_session_rtt = _rtt
												_target_session = _session

										subprocess.call(['ip', 'route', 'del', _route, 'metric', str(_rtt_old), 'dev', ''.join([_session, '.', str(self._worker_id)])], stderr=devnull)
										subprocess.call(['ip', 'route', 'add', _route, 'metric', str(_rtt),     'dev', ''.join([_session, '.', str(self._worker_id)])], stderr=devnull)

								if _target_session:
									#change the default outgoing path(dev) for a route
									subprocess.call(['ip', 'route', 'replace', _route, 'metric', str('0'), 'dev', ''.join([_target_session, '.', str(self._worker_id)])], stderr=devnull)

									_rtch_script = self.handler.rtch_script.get(_target_session, None)
									if _rtch_script:
										subprocess.call([_rtch_script, ''.join([_target_session, '.', str(self._worker_id)])], stderr=devnull)

							else:
								#only one path, no need to change
								continue

				del _tun
				del _usock
				del _addr

				self.o_mem()

		except:
			raise

	def wdd_idle_worker(self, port): #iii
		conns = -1
		worker_id = -1

		#assume that more than 1 worker
		for _worker_id, _conns in self.udt_conns_cnt.items():
			if _worker_id == 0: continue
			if conns == -1:
				conns = _conns.value
				worker_id = _worker_id
			else:
				if conns > _conns.value:
					#locate the least connection worker
					conns = _conns.value
					worker_id = _worker_id


		if self.udt_conns_cnt[port - Port + 1].value > conns:
			#orig worker has more conns than the least one, redirect to new worker
			#print "to new server", conns, worker_id
			self.udt_conn_port.value = worker_id + Port -1
		else:
			#no need to redirect
			#print "keep server", conns, worker_id
			self.udt_conn_port.value = port

		return self.udt_conn_port.value

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
	PARSE_MORE2 = 3

	EOL1 = b'\n\n'
	EOL2 = b'\n\r\n'
	
	xR_OK = 0
	xR_PARSE_AGAIN = 1
	xR_ERR_PARSE = -1
	xR_ERR_HANDLE = -2
	xR_ERR_403 = -3
	xR_ERR_404 = -4
	xR_ERR_5xx = -5
	
	xResult = 0

	weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
	
	monthname = [None, 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
			'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

	index_files = ["index.html", "index.htm"]

	gzip_types = []
	gzip_types_default = ["html", "htm", "js", "css", "txt", "xml"]

	mimetype = {'html': 'text/html', 'htm': 'text/html', 'txt': 'text/plain',
			'css': 'text/css', 'xml': 'text/xml', 'js': 'application/x-javascript',
			'png': 'image/png', 'jpg': 'image/jpeg', 'gif': 'image/gif', 'bin': 'application/octet-stream'}

	web_config = None
	web_config_parsed = False
	
	_r = b''  #request headers
	
	xcache_ttl = 5		#normally, 5-10 seconds internal cache time of items
	xcache_size = 1000000	#1 million items, about 1/3GB(333MB) mem used
	x_shelf_size = 1000000	#1 million items in disk, about 30GB disk size with average item size 30KB

	gzip_on = False
	gzip_size = 1000	 #default >1KB file size can be gzipped
	gzip_max_size = 10000000 #default <=10MB file size can be gzipped

	multip = {'k':1000, 'K':1000, 'm':1000000, 'M':1000000, 'g':1000000000, 'G':1000000000}

	writers = []

	multis = {'s':1, 'm':60, 'h':3600, 'd':86400, 'w':604800, 'y':31536000}

	expire_types = {}

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
							if not self.server.gzip_shelf:
								self.server.gzip_shelf = shelve.open('shelf.gzip', writeback=True)
							if not self.server.gzip_shelf_lock:
								self.server.gzip_shelf_lock = multiprocessing.Lock()
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
						self.gzip_types = copy.copy(self.gzip_types_default)
						for item in value.split(','):
							if item:
								if item not in self.gzip_types:
									if item[0] == '-':
										self.gzip_types.remove(item[1:])
									else:
										self.gzip_types.append(item)
					elif name == 'writers':
						self.writers = []
						if value:
							a = value.split(',')
							for item in a:
								if item.find('-') < 0:
									self.writers.append(item)
								else:
									_ip = item.split('.')

									_last = _ip[3].split('-')

									for i in range(int(_last[0]), int(_last[1])+1):
										ip = '.'.join([_ip[0], _ip[1], _ip[2], str(i)])
										self.writers.append(ip)
					elif name == 'xcache_ttl':
						self.xcache_ttl = int(value)
					elif name == 'server_pipelining':
						if value.lower() == "on":
							self.server_pipelining = True
					elif name == 'expire_types':
						self.expire_types = {}
						for item in value.split(','):
							if item:
								k, v = item.split(':', 1)
								if k and v:
									if v[-1] in ['s','m','h','d','w','y']:
										_multis = self.multis[v[-1]]
										self.expire_types[k] = int(v[:-1])*_multis
									else:
										self.expire_types[k] = int(v)
			except:
				raise

			web_config_parsed = True

	def init_handler(self, conn, client_address, rw_mode=0):
		self.addr = client_address
		self.sock = conn
		if self.sock:
			self.sock_fileno = conn.fileno()
		
		self.out_body_file = self.out_body_mmap = self._c = self.accept_encoding = None
		self.out_body_size = self.out_body_file_lmt = self.cmd_get = self.cmd_head =  self.cmd_put =  self.cmd_delete = self.if_modified_since = self.keep_connection = self.xResult = 0
		self.has_resp_body = self.xcache_hit = False
		self.canbe_gzipped = self.gzip_transfer = self.gzip_chunked = False
		self.gzip_finished = self.next_request = True
		#self.vhost_mode = False
		self.transfer_completed = 1

		self.command = self.path = self.resp_line = self.resp_msg = self.out_head_s = self._r = self._rb = self.hostname = self.xcache_key = b''
		self.c_http_ver = self.s_http_ver = self.r_http_ver = 1
		
		self.resp_code = self.HTTP_OK
		self.resume_transfer = rw_mode

	def __call__(self, fds):
		#should be called by native epoll server, can handle multi requests at one handler call, like: do 10 read events at a time with 10 connections
		for f, rw_mode in fds:  #ccc
			if rw_mode == 0:
				self.init_handler(self.server.conns[f], self.server.addrs[f], rw_mode)
				parse_stat = self.x_parse()
				if parse_stat == self.PARSE_OK or parse_stat == self.PARSE_MORE2:
					if self.cmd_get == 1 or self.cmd_head == 1:
						self.x_GET()
						self.x_response()
					elif self.cmd_put == 1 or self.cmd_delete == 1:
						self.x_PUT()
						self.x_response()
					else:
						self.xResult = self.xR_ERR_HANDLE
				elif self.server_pipelining and parse_stat == self.PARSE_MORE:
					if self.cmd_get == 1 or self.cmd_head == 1:
						self.x_GET()
						self.x_response()
					elif self.cmd_put == 1 or self.cmd_delete == 1:
						self.x_PUT()
						self.x_response()
					else:
						self.xResult = self.xR_ERR_HANDLE
					self.server.epoll.modify(f, select.EPOLLIN|select.EPOLLOUT)
				elif parse_stat == self.PARSE_AGAIN:
					self.xResult = self.xR_PARSE_AGAIN
					continue
				else:
					self.xResult = self.xR_ERR_PARSE
			elif rw_mode == 1:
				#continue sending a large file or pipeling
				if f in self.server.resume:
					#large file transfering
					self.init_handler(self.server.conns[f], self.server.addrs[f], rw_mode)
					self.x_response()
				elif self.server_pipelining and f in self.server.x_reqs:
					if self.server.x_reqs[f][1] == 1:
						#add pipelining request
						fds.append((f, 0))
					else:
						#maybe large PUT request recving
						try:
							self.server.epoll.modify(f, select.EPOLLIN)
						except:
							pass
					self.keep_connection = 1
				elif self.server_pipelining and f not in self.server.x_reqs:
					self.transfer_completed = 0  #not do clean()
					try:
						self.server.epoll.modify(f, select.EPOLLIN)
					except:
						pass
				else:
					self.xResult = self.xR_ERR_PARSE
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
			#self.r_http_ver = 1
		else:
			self.keep_connection = 0

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
		elif code == 201:
			self.resp_msg = "Created"
		elif code == 204:
			self.resp_msg = "No Content"
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
		elif code == 503:
			self.resp_msg = "Service Unavailable"
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
		#get the request headers, xxx
		_doing_pipelining = _last_pipelining = _header_parsed = False
		_eol_pos = -1
		_cl = 0

		_fn = self.sock_fileno
		if _fn not in self.server.x_reqs:
			r = self._r
			_xreqs_empty = _first_pipelining = True
		else:
			r = self._r = self.server.x_reqs[_fn][0]
			_xreqs_empty = _first_pipelining = False

			if self.EOL2 in r or self.EOL1 in r:
				if self.server.x_reqs[_fn][1] < 0:
					#request body not finished recv, PUT method
					_header_parsed = True
					_first_pipelining = True
					_cl = 0 - self.server.x_reqs[_fn][1]
					_eol_pos = self.server.x_reqs[_fn][2]
				elif self.server.x_reqs[_fn][1] == 0:
					#not pipelining requests, must done before
					self.server.x_reqs.pop(_fn)
					r = self._r = b''
				else:
					#self.server.x_reqs[_fn][1] == 1
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

				if not _header_parsed:
					_eol_pos = r.find(self.EOL2)

				if _eol_pos > -1 and not _header_parsed: 
					#headers mostly end with EOL2 "\n\r\n"
					if not self.server_pipelining:
						if not _xreqs_empty:
							#a big-headers request is all recieved
							self.server.x_reqs.pop(_fn, None)
					else:
						#for http pipelining
						if r.count(self.EOL2) > 1 or _eol_pos < len(r) - len(self.EOL2): 
							c = r.split(self.EOL2, 1)
							r = c[0]
							self.server.x_reqs[_fn] = [c[1], 1]
							_doing_pipelining = True
						else:
							if not _xreqs_empty:
								#doing the last pipelining, clear x_reqs
								self.server.x_reqs.pop(_fn, None)
							_last_pipelining = True
					break
				elif _eol_pos > -1 and _header_parsed:
					#recving request body
					self._rb = r[_eol_pos+len(self.EOL2):]
					if _cl > len(self._rb):
						#not finished recv request body
						self.server.x_reqs[_fn] = [r, 0 - _cl, _eol_pos]
						return self.PARSE_AGAIN
					elif _cl < len(self._rb):
						#full request body recv, there are other data, maybe pipelining requests
						self.server.x_reqs[_fn] = [self._rb[_cl:], 1]
						_doing_pipelining = True
						break
					else:
						#whole body recv done
						self.server.x_reqs.pop(_fn , None)
						#vars should been re-setup, though header parsed before
						break
				else:
					#not finished all headers, save recv data
					self.server.x_reqs[_fn] = [r, 0]
					return self.PARSE_AGAIN
				#self.sock.setblocking(0)
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

		#a = r.split("\r\n", 1)
		a = r[:_eol_pos].splitlines()

		#if not a[0]:
		if len(a) < 2:
			#illeagal request headers
			return self.PARSE_ERROR

		try:
			#"GET / HTTP/1.1"
			self.command, self.path, _c_http_ver = a[0].split()
		except:
			#illeagal command/path line
			return self.PARSE_ERROR

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

		"""
		#all headers go to dict
		if self.cmd_put == 0:
			self.in_headers = dict((k, v) for k, v in (item.split(": ") for item in a[1].strip().split("\r\n")))
		else:
			self.in_headers = dict((k, v) for k, v in (item.split(": ") for item in a[1].split(self.EOL2, 1)[0].split("\r\n")))
		"""

		for _line in a[1:]:
			_pos = _line.find(": ")
			if _pos > 0:
				self.in_headers[_line[:_pos]] = _line[_pos+2:]

		self.check_connection(_c_http_ver)

		if self.cmd_put == 1 and not _header_parsed:
			if _eol_pos  < len(r) - len(self.EOL2):
				self._rb = r[_eol_pos+len(self.EOL2):]	 #request body
			else:
				self._rb = b''

			_cl = int(self.in_headers.get("Content-Length", "0"))
			if _cl == 0:
				return self.PARSE_ERROR
			elif _cl > len(self._rb):
				#not finished recv request body
				self.server.x_reqs[_fn] = [r, 0 - _cl, _eol_pos]
				return self.PARSE_AGAIN
			elif _cl < len(self._rb):
				#full request body recv, there are other data, maybe pipelining requests
				self.server.x_reqs[_fn] = [self._rb[_cl:], 1]
				_doing_pipelining = True
			else:
				#full request body recv
				self.server.x_reqs.pop(_fn, None)

		if _fn not in self.server.x_reqs:
			#no more requests to process, last pipelining or non-pipelining
			return self.PARSE_OK
		else:
			if self.server.x_reqs[_fn][1] == 1:
				#doing pipelining, not last
				if _first_pipelining:
					#first piplining
					return self.PARSE_MORE
				else:
					#not first piplining
					return self.PARSE_MORE2

	def x_GET(self):
		if self.if_modified_since == 0 and self.xcache_key in self.server.xcache:
			self._c = self.server.xcache.get(self.xcache_key)
			ttl = self._c[0]
			if ttl >= time.time():
				#cache hit
				self.out_head_s, self.out_body_file, self.out_body_size, self.out_body_file_lmt, self.out_body_mmap, self.canbe_gzipped = self._c[1:]
				self.has_resp_body = True

				if self.r_http_ver == self.HTTP11:
					self.resp_line = 'HTTP/1.1 200 OK'
				else:
					self.resp_line = 'HTTP/1.0 200 OK'

				if self.canbe_gzipped and self.c_http_ver == self.HTTP11 and self.r_http_ver == self.HTTP11:
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
								self.gzip_chunked=self.server.gzip_shelf[self.xcache_key][7]
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

				if self._c[5]:	#close the mmap maped, if exists
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
				if self.gzip_on and a[1] in self.gzip_types and self.out_body_size > self.gzip_size and self.out_body_size <= self.gzip_max_size:
						self.canbe_gzipped = True
			else:
				self.set_out_header("Content-Type", "application/octet-stream")

			if a[1] in self.expire_types:
				self.set_out_header("Cache-Control", ''.join(["max-age=", str(self.expire_types[a[1]])]))
				self.set_out_header("Expires", self.date_time_string(time.time() + self.expire_types[a[1]]))
		except:
			self.set_out_header("Content-Type", "application/octet-stream")

		self.set_resp_code(200)

	def x_PUT(self):
		try:
			_peer_ip, _port = self.sock.getpeername()
		except:
			_peer_ip = b''

		if _peer_ip not in self.writers:
			self.xResult = self.xR_ERR_HANDLE
			return

		path = ''.join([self.homedir, self.path])

		if not os.path.isdir(path):
			if self.cmd_delete == 1:
				if os.path.exists(path):
					try:
						os.unlink(path)
						self.set_resp_code(204)
					except:
						self.set_resp_code(403)
				else:
					self.set_resp_code(204)

			elif self.cmd_put == 1:
				try:
					_dir = path.rsplit('/', 1)[0]
					if not os.path.exists(_dir):
						os.makedirs(_dir, 0755)

					f = open(path, 'wb')
					f.write(self._rb)
					f.close()
					self.set_resp_code(201)
				except IOError as e:
					self.set_resp_code(403)
		else:
			if self.cmd_delete == 1:
				try:
					os.rmdir(path)
					self.set_resp_code(204)
				except:
					self.set_resp_code(403)
			else:
				self.set_resp_code(403)

	def send_out_all_headers(self, extra=''):
		#if self.keep_connection == 1 and self.r_http_ver == self.HTTP11:
		if self.keep_connection == 1:
			self.sock.send(''.join([self.resp_line, "\n", self.out_head_s, "Connection: keep-alive\n\n", extra]))
			#writev(self.sock_fileno, [self.resp_line, "\n", self.out_head_s, "Connection: keep-alive\n\n", extra])
		else:
			self.sock.send(''.join([self.resp_line, "\n", self.out_head_s, "Connection: close\n\n", extra]))
			#writev(self.sock_fileno, [self.resp_line, "\n", self.out_head_s, "Connection: close\n\n", extra])

	def send_out_all_headers2(self, extra=None):
		if extra:
			#if self.keep_connection == 1 and self.r_http_ver == self.HTTP11:
			if self.keep_connection == 1:
				self.sock.send(''.join([self.resp_line, "\n", self.out_head_s, "Connection: keep-alive\n\n", extra]))
			else:
				self.sock.send(''.join([self.resp_line, "\n", self.out_head_s, "Connection: close\n\n", extra]))
		else:
			#if self.keep_connection == 1 and self.r_http_ver == self.HTTP11:
			if self.keep_connection == 1:
				self.sock.send(''.join([self.resp_line, "\n", self.out_head_s, "Connection: keep-alive\n\n"]))
			else:
				self.sock.send(''.join([self.resp_line, "\n", self.out_head_s, "Connection: close\n\n"]))

	def x_response(self):
		if self.resume_transfer == 0:
			sent = _sent = 0
		elif self.resume_transfer == 1:
			self.xcache_hit = self.has_resp_body = True
			self.command = 'GET'
			self.cmd_get = 1
			_sent = 0
			self.transfer_completed = 0

			_rs = self.server.resume.get(self.sock_fileno)
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
					self.gzip_chunked = self.server.gzip_shelf[self.xcache_key][7]
			else:
				#no such resume, must be first trans
				self.server.epoll.modify(self.sock_fileno, select.EPOLLIN)
				self.resume_transfer = sent = 0
				self.out_head_s = self.server.xcache[self.xcache_key][1]
				self.out_body_file = self.server.xcache[self.xcache_key][2]
				self.out_body_size = self.server.xcache[self.xcache_key][3]

		#At this point, begin transfer response, first to roll out headers
		if not self.xcache_hit:
			_t = time.time()
			if len(self.out_headers) > 0:
				self.out_head_s = ''.join(["Server: ", self.server_version, "\nDate: ", self.date_time_string(_t), '\n', '\n'.join(['%s: %s' % (k, v) for k, v in self.out_headers.items()]), '\n'])
			else:
				self.out_head_s = ''.join(["Server: ", self.server_version, "\nDate: ", self.date_time_string(_t), '\n'])

			if self.resp_code == self.HTTP_OK and self.out_body_size > 0:
				#Only 200 and body_size > 0 response will be cached, [ttl, out_head_s, f, fsize, f_lmt, mmap], and file smaller than 1KB will be mmaped
				if self.out_body_size < 1000 and not self.canbe_gzipped:
					self.out_body_mmap = mmap.mmap(self.out_body_file.fileno(), 0, prot=mmap.PROT_READ)
				else:
					if self.canbe_gzipped and self.c_http_ver == self.HTTP11 and self.r_http_ver == self.HTTP11:
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
								#moved to self.server.check_3ws()
								#keep the mem cache of gzip_shelf limitted
								#while len(self.server.gzip_shelf.cache) > 1000:
								#	self.server.gzip_shelf.cache.popitem()

								#keep the disk cache of gzip_shelf limitted
								if len(self.server.gzip_shelf) > self.x_shelf_size:
									with self.server.gzip_shelf_lock:
										self.server.gzip_shelf.popitem()

								if self.gzip_chunked:
									#[file size original, headers, content, body_size, file modified time, current gzip position, finished, chunked]
									_sss = ''.join([hex(len(_ss))[2:], '\r\n', _ss, '\r\n'])
									with self.server.gzip_shelf_lock:
										self.server.gzip_shelf[self.xcache_key] = [self.out_body_size, _out_head_s, _sss, len(_sss), self.out_body_file_lmt, self.send_buf_size/2, False, self.gzip_chunked]
									self.server._gzs[self.xcache_key] = _gzf
								else:
									with self.server.gzip_shelf_lock:
										self.server.gzip_shelf[self.xcache_key] = [self.out_body_size, _out_head_s, _ss, len(_ss), self.out_body_file_lmt, self.out_body_size, True, self.gzip_chunked]

								#moved to self.server.check_3ws()
								#if hasattr(self.server.gzip_shelf.dict, 'sync'):
								#	with self.server.gzip_shelf_lock:
								#		self.server.gzip_shelf.dict.sync()
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
					self.gzip_chunked = self.server.gzip_shelf[self.xcache_key][7]

			elif self.resp_code >= self.HTTP_BAD_REQUEST:
				self.out_head_s = ''.join([self.out_head_s, "Content-Length: ", str(len(self.resp_msg) + 4), '\n'])

		#send headers & body
		if self.has_resp_body and self.out_body_file and self.cmd_get == 1:
			if self.out_body_mmap:
				if self.server_pipelining:
					if self.sock_fileno in self.server.x_reqs:
						self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
					self.send_out_all_headers(extra=self.out_body_mmap[:])
					if self.sock_fileno not in self.server.x_reqs:
						self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)
				else:
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
						_sent = self.sock.send(self.out_body_file[sent:_send_buf+sent])
					else:
						_sent = sendfile.sendfile(self.sock_fileno, self.out_body_file.fileno(),
													sent, _send_buf)
					sent += _sent
					if self.resume_transfer == 0:
						#after transfer snd_buf data, requeue event, let other event to be handled
						if sent < self.out_body_size or not self.gzip_finished:
							self.server.resume[self.sock_fileno] = [self.out_body_file,
									self.out_body_size, sent, self.keep_connection,
									self.gzip_transfer, self.xcache_key]
							self.server.epoll.modify(self.sock_fileno, select.EPOLLOUT)
							self.transfer_completed = 0
					else:
						if self.out_body_size == sent and self.gzip_finished:
							self.server.resume.pop(self.sock_fileno)
							self.server.epoll.modify(self.sock_fileno, select.EPOLLIN)
							self.transfer_completed = 1
						else:
							self.server.resume[self.sock_fileno] = [self.out_body_file,
									self.out_body_size, sent, self.keep_connection,
									self.gzip_transfer, self.xcache_key]
				except OSError as e: #rrr
					if e.errno == errno.EAGAIN:
						#send buffer full?just wait to resume transfer
						#and gevent mode can't reach here, beacause gevent_sendfile intercepted the exception
						self.server.resume[self.sock_fileno] = [self.out_body_file,
									self.out_body_size, sent, self.keep_connection,
									self.gzip_transfer, self.xcache_key]
					elif e.errno == errno.EPIPE:
						#peer closed connection
						self.transfer_completed = 1
						self.server.resume.pop(self.sock_fileno)
						self.server.cleanup(self.sock_fileno);
					else:
						raise

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

					with self.server.gzip_shelf_lock:
						self.server.gzip_shelf[self.xcache_key] = [_org_size, self.out_head_s, _sss, len(_sss), _file_lmt, _gzip_pos + _z_buf_size, self.gzip_finished, self.gzip_chunked]
						#moved to self.server.check_3ws()
						#if hasattr(self.server.gzip_shelf.dict, 'sync'):
						#	self.server.gzip_shelf.dict.sync()

				#Now, transfer complete, resume nature behavior of TCP/IP stack, as turned before
				if self.keep_connection == 1 and self.resume_transfer == 0:
					#no need to set TCP_CORK when keep_connection=0
					#it will be cleared when socket closing and data will be flushed
					self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)
		elif self.resp_code == self.HTTP_NOT_MODIFIED:
			self.send_out_all_headers()
		elif self.resp_code == self.HTTP_OK and self.cmd_head == 1:
			self.send_out_all_headers()
		elif self.resp_code >= self.HTTP_BAD_REQUEST or self.resp_code == 201 or self.resp_code == 204:
			self.send_out_all_headers(extra = "%d %s" % (self.resp_code, self.resp_msg))

	def clean(self):
		if self.transfer_completed == 1:
			self.in_headers.clear()
			self.out_headers.clear()
			if self.keep_connection == 0:
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
	geo = {}

	_rra = None
	_ttl = None
	q = None
	query_name = None
	query_name_geo = None
	answer = None
	
	xcache_ttl = 10
	probe_interval = 10  #icmp probe interval in seconds, see if the ip alive

	LR_LEFT = 0
	LR_RIGHT = 1

	lr_peers = {}
	lr_ttl = 3600
	lr_left = ''
	lr_right = ''
	lr_range = ''
	lr_range_suffix = ''
	lr_prefix = 'lr'
	lr_resolve = False
	lr_left_measuring = False
	lr_right_measuring = False

	geoip_db = None

	def __init__(self, conn, client_address, server, config_section='3nsd'):
		self.server = server
		#if server.workers > 1:
		#	self._wlock = multiprocessing.Lock()
		#else:
		#	self._wlock = Semaphore()
		self.init_nsd_config(config_section=config_section)

	def init_nsd_config(self, config_section='3nsd'):

		#for reload config
		self.ttl = {}
		self.rra = {}
		self.rrn = {}
		self.geo = {}

		self.lr_peers = {}
		self.lr_ttl = 3600
		self.lr_left = ''
		self.lr_right = ''
		self.lr_range = ''
		self.lr_range_suffix = ''
		self.lr_prefix = 'lr'
		self.lr_resolve = False
		self.lr_left_measuring = False
		self.lr_right_measuring = False

		self.geoip_db = None

		self.config = ConfigParser.ConfigParser()
		if not self.config.read('3xsd.conf'):
			self.config.read('/etc/3xsd.conf')

		for name, value in self.config.items(config_section):
			if name == "left":
				if value:
					self.lr_left = value.lower().strip()
			elif name == "right":
				if value:
					self.lr_right = value.lower().strip()
			elif name == "range":
				if value:
					self.lr_range = value.lower().strip()
			elif name == "range_suffix":
				if value:
					self.lr_range_suffix = value.lower().strip()
			elif name == "lr_ttl":
				if value:
					self.lr_ttl = int(value)
			elif name == "lr_prefix":
				if value:
					self.lr_prefix = value.lower().strip()
			elif name == "geoip_db":
				if value:
					self.geoip_db = geoip2.database.Reader(value)
			else:
				v = value.split(',', 1)
				if len(v) > 1:
					# [ttl, ip, ...]
					if '@' in name:
						_name, _geo = name.lower().split('@')

						if _name not in self.geo:
							self.geo[_name] = {}

						for _cc in _geo.split('/'):
							if _cc:
								self.geo[_name][_cc] = name

					self.ttl[name] = int(v[0])
					self.rra[name] = self.ip_list(name, v[1], config_section)
					self.rrn[name] = 0

	def ip_list(self, name, ipstr, config_section):
		#ip,ip,ip... can be the following format:   #lll
		#10.23.4.11  -	single ip, 10.23.4.101-200  -  multi ip
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
		self.query_name_geo = None
		self.answer = None

	def __call__(self, events):
		#found that one event can contains multi dns query packages, so use while loop instead of for
		while 1:
			try:
				self.data, self.addr = self.sock.recvfrom(1024)
				if self.x_parse_query() == self.PARSE_OK:
					if not self.lr_resolve:
						self.x_gen_answer()
					else:
						self.x_lr_resolve()
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
		if len(alist) == 1:
			return alist
		else:
			_n = n % len(alist)
			return alist[_n:] + alist[:_n]

	def x_check_range_resolve(self):
		#check if it's a left-right range resolving name
		self.lr_resolve = self.lr_left_measuring = self.lr_right_measuring = False

		if self.lr_range_suffix in self.query_name[0-len(self.lr_range_suffix):] and self._rra and self._rra[0] == '0.0.0.0':
			self.lr_resolve = True

		if self.lr_left  in self.query_name[0-len(self.lr_left):] and self.lr_prefix in self.query_name[:len(self.lr_prefix)]:
			self.lr_resolve = True
			self.lr_left_measuring = True

		if self.lr_right in self.query_name[0-len(self.lr_right):] and self.lr_prefix in self.query_name[:len(self.lr_prefix)]:
			self.lr_resolve = True
			self.lr_right_measuring = True

		if self.lr_resolve:
			_peer, _ = self.addr
			if _peer not in self.lr_peers:
				self.lr_peers[_peer] = [int(time.time()) + self.lr_ttl, 0, 0]  #[ttl, left-rtt,right-rtt], also as [ttl, a,b]

		return self.lr_resolve

	def x_check_peer_geo(self): #cpcp
		if self.geoip_db:
			try:
				_rs = self.geoip_db.country(self.addr[0])
				_cc = None

				#the country code(_cc), first match continent code, then country's iso_code
				if hasattr(_rs.continent, "code"):
					_cc = _rs.continent.code.lower()
					if _cc in self.geo[self.query_name]:
						self.query_name_geo = self.geo[self.query_name][_cc]
				if hasattr(_rs.country, "iso_code"):
					_cc = _rs.country.iso_code.lower()
					if _cc in self.geo[self.query_name]:
						self.query_name_geo = self.geo[self.query_name][_cc]

				#city has not iso_code, so what's next?
				#if hasattr(_rs.city, "iso_code"):
				#	print "peer city code:", _rs.city.iso_code
				#elif hasattr(_rs.city, "name"):
				#	print "peer city name:", _rs.city.name

				print "peer:", self.addr[0], "geo:", self.query_name_geo, "cc:", _cc
			except:
				pass

	def x_parse_query(self):  #pqpq
		self.q = dpkt.dns.DNS(self.data)

		#we accept just A type query
		if self.q.qd[0].cls != dpkt.dns.DNS_IN or self.q.qd[0].type != dpkt.dns.DNS_A:
			return self.PARSE_ERROR
		
		self.query_name =  self.query_name_geo = self.q.qd[0].name

		if self.query_name in self.geo:
			self.x_check_peer_geo()
			self._rra = self.rra.get(self.query_name_geo)
			self._ttl = self.ttl.get(self.query_name_geo)
		else:
			self._rra = self.rra.get(self.query_name)
			self._ttl = self.ttl.get(self.query_name)
		
		if self.x_check_range_resolve():
			#It's a left-right range resolve
			return self.PARSE_OK
		elif self._rra is not None and self._ttl is not None:
			#ok, rr & ttl config found
			return self.PARSE_OK
		else:
			#not my serving domain name
			return self.PARSE_ERROR

	def x_lr_resolve(self): #lrlr
		_peer = self.addr[0]
		_ttl, a, b = self.lr_peers[_peer]
		_t = time.time()  #_t: current time

		#print "---------------------"
		#print _peer, self.lr_peers[_peer]

		if _t <= _ttl:
			#cache of a,b not expired
			if a > 0 and b > 0:
				self.x_lr_range(a, b, ch=True) #ch = cache hit
				return
		else:
			#cache of a,b expired
			_ttl = int(_t) + self.lr_ttl
			a = b = 0
			self.lr_peers[_peer] = [_ttl, a, b]

		if self.lr_left_measuring:
			#doing left measure
			_speer0, _sts0, _sts1, _sts2 = self.query_name.split('.')[0].split('-')[1:]
			_ts0, _ts1, _ts2 = (int(_sts0), int(_sts1), int(_sts2))

			_peer0 = socket.inet_ntoa(struct.pack('!I', int(_speer0)))
			if _peer0 not in self.lr_peers:
				self.lr_peers[_peer0] = [int(_t) + self.lr_ttl, 0, 0]

			if _ts2 > 0:
				b = self.lr_peers[_peer][2] = self.lr_peers[_peer0][2] = _ts2
			if a == 0:
				if _ts1 == 0:
					self.x_lr_cname(self.LR_LEFT, _ts0, int((_t - _ts0) * 1000), _ts2)
					return
				else:
					a = self.lr_peers[_peer][1] = self.lr_peers[_peer0][1] = int((_t - _ts0) * 1000) - _ts1 if _ts1>300000 else _ts1
			if b == 0:
				self.x_lr_cname(self.LR_RIGHT, _ts0, a, 0)
			elif a > 0 and b > 0:
				if a < _ts1:
					#for debug purpose
					self.x_lr_cname(self.LR_LEFT, _ts0, a, b)
				else:
					self.x_lr_range(a, b)
		elif self.lr_right_measuring:
			#doing right measure
			_speer0, _sts0, _sts1, _sts2 = self.query_name.split('.')[0].split('-')[1:]
			_ts0, _ts1, _ts2 = (int(_sts0), int(_sts1), int(_sts2))

			_peer0 = socket.inet_ntoa(struct.pack('!I', int(_speer0)))
			if _peer0 not in self.lr_peers:
				self.lr_peers[_peer0] = [int(_t) + self.lr_ttl, 0, 0]

			if _ts1 > 0:
				a = self.lr_peers[_peer][1] = self.lr_peers[_peer0][1] = _ts1
			if b == 0:
				if _ts2 == 0:
					self.x_lr_cname(self.LR_RIGHT, _ts0, _ts1, int((_t - _ts0) * 1000))
					return
				else:
					b = self.lr_peers[_peer][2] = self.lr_peers[_peer0][2] = int((_t - _ts0) * 1000) - _ts2 if _ts2>300000 else _ts2
			if a == 0:
				self.x_lr_cname(self.LR_LEFT, _ts0, 0, b)
			elif a > 0 and b > 0:
				if b < _ts2:
					#for debug purpose
					self.x_lr_cname(self.LR_RIGHT, _ts0, a, b)
				else:
					self.x_lr_range(a, b)
		else:
			#doing initial query
			#_ts0: base time stamp, in secs
			_ts0 = int(_t - 300)
			#_ts: offset time stamp from base time, in msecs
			_ts = int((_t - _ts0) * 1000)

			if self.lr_range == 'left':
				#left
				if a == 0 and b == 0:
					if _ts0 % 2:
						self.x_lr_cname(self.LR_LEFT, _ts0, _ts)
					else:
						self.x_lr_cname(self.LR_RIGHT, _ts0, 0, 0)
				elif a == 0: #b > 0
					self.x_lr_cname(self.LR_LEFT, _ts0, _ts, b)
				elif b == 0: #a > 0
					self.x_lr_cname(self.LR_RIGHT, _ts0, a, 0)
				else: #a > 0, b > 0
					self.x_lr_range(a, b, ch=True)
			else:
				#right
				if a == 0 and b == 0:
					if _ts0 % 2:
						self.x_lr_cname(self.LR_RIGHT, _ts0, 0, _ts)
					else:
						self.x_lr_cname(self.LR_LEFT, _ts0, 0, 0)
				elif b == 0: #a > 0
					self.x_lr_cname(self.LR_RIGHT, _ts0, a, _ts)
				elif a == 0: #b > 0
					self.x_lr_cname(self.LR_LEFT, _ts0, 0, b)
				else: #a > 0, b > 0
					self.x_lr_range(a, b, ch=True)

	def x_lr_range(self, a, b, ch=False): #lrlr
		if self.lr_left_measuring:
			_cname = self.query_name[self.query_name.find('.')+1:]
			if a > b:
				_cname = _cname.replace(self.lr_left, self.lr_right)
		elif self.lr_right_measuring:
			_cname = self.query_name[self.query_name.find('.')+1:]
			if a < b:
				_cname = _cname.replace(self.lr_right, self.lr_left)
		else:
			if self.lr_range == 'left':
				_cname = self.query_name.replace(self.lr_range_suffix, self.lr_left)
			elif self.lr_range == 'right':
				_cname = self.query_name.replace(self.lr_range_suffix, self.lr_right)

		#gen cname answer
		self.q.op = dpkt.dns.DNS_RA
		self.q.rcode = dpkt.dns.DNS_RCODE_NOERR
		self.q.qr = dpkt.dns.DNS_R

		arr = dpkt.dns.DNS.RR()
		arr.cls = dpkt.dns.DNS_IN
		arr.type = dpkt.dns.DNS_CNAME
		arr.name = self.query_name
		arr.cname = _cname
		arr.ttl = 0 if not ch else self.ttl.get(_cname, 0)
		self.q.an.append(arr)

		#I haven't understand what the Authority Record is going on..
		if self.q.ar:	del self.q.ar[:]

		self.answer = str(self.q)

	def x_lr_cname(self, _range, ts0, ts1, ts2=0): #lrlr
		#query_name: ga.i.3xsd.net
		#cname: ts0-ts1-ts2.ga.l.3xsd.net
		#ts0: base time, in secs
		#ts1: a measure time start point if ts2 = 0, or rtt of a if ts2 > 0, in msecs from base time
		#ts2: b measure time start point if ts3 = 0, or rtt of b if ts3 > 0, in msecs from base time

		if self.lr_right_measuring or self.lr_left_measuring:
			_query_name = self.query_name[self.query_name.find('.')+1:]
		else:
			_query_name = self.query_name

		if _range == self.LR_LEFT:
			if self.lr_right_measuring:
				_query_name = _query_name.replace(self.lr_right, self.lr_left)
			else:
				_query_name = _query_name.replace(self.lr_range_suffix, self.lr_left)
		elif _range == self.LR_RIGHT:
			if self.lr_left_measuring:
				_query_name = _query_name.replace(self.lr_left, self.lr_right)
			else:
				_query_name = _query_name.replace(self.lr_range_suffix, self.lr_right)

		#[prefix, peer_ip, ts0, ts1, ts2]
		_cname = ''.join([self.lr_prefix, '-', str(struct.unpack('!I', socket.inet_aton(self.addr[0]))[0]), '-', str(ts0), '-', str(ts1), '-', str(ts2), '.', _query_name])

		#gen cname answer
		self.q.op = dpkt.dns.DNS_RA
		self.q.rcode = dpkt.dns.DNS_RCODE_NOERR
		self.q.qr = dpkt.dns.DNS_R

		arr = dpkt.dns.DNS.RR()
		arr.cls = dpkt.dns.DNS_IN
		arr.type = dpkt.dns.DNS_CNAME
		arr.name = self.query_name
		arr.cname = _cname
		arr.ttl = 0
		self.q.an.append(arr)

		#I haven't understand what the Authority Record is going on..
		if self.q.ar:	del self.q.ar[:]

		self.answer = str(self.q)

	def x_gen_answer(self): #gaga
		if self.query_name_geo in self.server.xcache:
			_c = self.server.xcache.get(self.query_name_geo)
			if _c[0] > time.time():
				#cache item not expired, load it and rewrite the id field of answer to match queryer's 
				if self.q.id < 255:
					self.answer = ''.join(['\x00', chr(self.q.id), _c[1][2:]])
				else:
					self.answer = ''.join([chr(self.q.id/256), chr(self.q.id % 256), _c[1][2:]])
				return
			else:
				#expired, clear it
				self.server.xcache.pop(self.query_name_geo)

		#cache not hit, go on handling: first to turn query into answer
		self.q.op = dpkt.dns.DNS_RA
		self.q.rcode = dpkt.dns.DNS_RCODE_NOERR
		self.q.qr = dpkt.dns.DNS_R
		
		_alive = 0
		#if not a geo resolving, self.query_name_geo is just equal to self.query_name, set by x_parse_query()
		_query_name = self.query_name_geo

		#for round robbin, shift ip list every time
		self._rra = self.shift(self.rra.get(_query_name), self.rrn.get(_query_name))
		self.rrn[_query_name] = (self.rrn[_query_name] + 1) % len(self.rra.get(_query_name))

		#gen rr records for A resolve
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

		#I haven't understand what the Authority Record is going on..
		if self.q.ar:	del self.q.ar[:]

		self.answer = str(self.q)
	
		#cache it, when expired at one ttl
		self.server.xcache[self.query_name_geo] = [self.xcache_ttl + time.time(), self.answer]

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
					if ip == '0.0.0.0': continue
					if time.time() > self.stat[ip][2] + self.stat[ip][1]:  #last-check + ttl
						gs.append(gevent.spawn(self.icmp_ping, ip))  #do works concurrently
				gevent.joinall(gs)
			#print "lr_peers:", self.lr_peers
			#print "-------------------"
			#print "self.rra: ", self.rra
			#print "self.geo: ", self.geo

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
	Z_URL = 2 #url hash

	None2 = [None, None]

	_f = None #socket fileno

	z_cache_size = 1000	#limit the xcache size in mem, about 30MB size with average file size 30KB
	z_idle_ttl = 20		#backend connections have a idle timeout of 20 seconds

	z_xcache_shelf = False	#persistent storage of xcache
	z_shelf_size = 1000000	#1 million files on-disk cache, about 30GB disk size with average file size 30KB

	transparent_proxy = False
	tp_host = {}

	def __init__(self, conn, client_address, server, native_epoll=True,
					gevent_stream=False, recv_buf_size=16384, send_buf_size=65536, z_mode=0):
		_xHandler.__init__(self, conn, client_address, server, native_epoll, gevent_stream, recv_buf_size, send_buf_size)
		self.server_pipelining = False

		if Z_mode > z_mode:
			z_mode = Z_mode

		if z_mode >= 0:
			_xDNSHandler.__init__(self, conn, client_address, server, config_section='3zsd')
			if '*' in self.rra.keys():
				self.transparent_proxy = True
			self.z_mode = z_mode
		else:
			self.z_mode = 0

		if X_shelf == 1:
			self.z_xcache_shelf = True
			self.server.xcache_shelf = shelve.open('shelf.xcache', writeback=True)
			self.server.xcache_shelf_lock = multiprocessing.Lock()

	def init_zsd_config(self, config_section='3zsd'):
		_xHandler.init_config(self)
		_xDNSHandler.init_nsd_config(self, config_section=config_section)
		if '*' in self.rra.keys():
			self.transparent_proxy = True

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
		if self.if_modified_since == 0 and self.in_headers.get("Cache-Control", "null").find("no-cache") < 0:

			_key_found = _in_shelf = False

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
					if self.r_http_ver == self.HTTP11:
						self.resp_line = 'HTTP/1.1 200 OK'
					else:
						self.resp_line = 'HTTP/1.0 200 OK'
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
							if self.xResult < 0:
								#something wrong
								if self.xResult == self.xR_ERR_5xx:
									#backend error
									self.x_response()
							else:
								_do_clean = False
					elif self.cmd_put == 1 or self.cmd_delete == 1:
						if hasattr(self, "z_PUT_init"):
							self.z_PUT_init()
							if self.xResult < 0:
								if self.xResult == self.xR_ERR_5xx:
									self.x_response()
							else:
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
									self.server.zidles[str(self.server.zaddrs[_f])].appendleft(_f)
							else:
								self.server.zidles[str(self.server.zaddrs[_f])] = deque([_f])

							self.server.zconns_stat[_f] = [0, time.time()]	#conn idle

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
				_do_clean = self.z_GET_backend()
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

	def z_resolve_request_host(self, _hostname):
		#self.tp_host[_hostname] = {'ip', 'expire-time'}
		#transparent hosts's hostname-ip resolve results
		_t = time.time()
		_ttl = self.ttl.get('*')

		if _hostname in self.tp_host:
			if _t > self.tp_host[_hostname]['expire-time']:
				#renew ip
				self.tp_host[_hostname]['ip'] = socket.gethostbyname(_hostname)
				self.tp_host[_hostname]['expire-time'] = _ttl + _t
		else:
			#new resolve
			self.tp_host[_hostname]= {'ip':socket.gethostbyname(_hostname), 'expire-time':_ttl+_t}

		return ''.join([self.tp_host[_hostname]['ip'], ':80'])

	def z_pick_a_backend(self):
		if self.transparent_proxy and self.z_hostname not in self.rra.keys():
			z_hostname = '*'
			if self.rra['*'][0] == '0.0.0.0:0': 
				#a special backend 0.0.0.0:0 is defined,
				#means that zsd should resolve the request hostname to ip and direct to it
				return self.z_resolve_request_host(self.z_hostname)
		else:
			z_hostname = self.z_hostname

		if self.z_mode == self.Z_RR:
			self.z_backends = self.shift(self.rra.get(z_hostname), self.rrn.get(z_hostname))
			self.rrn[z_hostname] = (self.rrn[z_hostname] + 1) % len(self.rra.get(z_hostname))
			return self.z_backends[0]
		elif self.z_mode == self.Z_IP:
			try:
				_ip_peer, _port_peer = self.sock.getpeername()
				_ip_forward = self.in_headers.get('X-Forwarded-For')
				if _ip_forward:
					#get the real peer ip, for what via cache servers
					_ip_peer = _ip_forward.split(',', 1)[0].strip()
			except:
				_ip_peer = b''

			_ips = self.rra.get(z_hostname)
			_ips_n = len(_ips)
			if _ip_peer:
				#ip hash, with c block num
				_ip = _ip_peer.split('.')
				_idx = __idx = (int(_ip[0])*65536 + int(_ip[1])*256 + int(_ip[2]))%_ips_n
				_try = 1

				while self.stat[_ips[_idx]][0] == False:
					#server marked down
					_t = time.time()
					if _t - self.stat[_ips[_idx]][2] > self.stat[_ips[_idx]][1]:
						#expires ttl, will be rechecked
						self.stat[_ips[_idx]][0] = True
						self.stat[_ips[_idx]][2] = _t
						break

					_idx = random.randint(0, _ips_n - 1)
					if _idx == __idx:
						_idx = (_idx + 1) % _ips_n

					_try += 1
					if _try >= _ips_n:
						break
				return _ips[_idx]
			else:
				return _ips[random.randint(0, _ips_n - 1)]
		elif self.z_mode == self.Z_URL:
			#url hash, with path md5's first 6 hex digist
			md5 = hashlib.md5()
			md5.update(self.path)
			_path_md5 = md5.hexdigest()

			_ips = self.rra.get(z_hostname)
			_ips_n = len(_ips)
			_idx = __idx = (int(_path_md5[0:1], base=16)*65536 + int(_path_md5[2:3], base=16)*256 + int(_path_md5[4:5], base=16))%_ips_n
			_try = 1

			while self.stat[_ips[_idx]][0] == False:
				#server marked down
				_t = time.time()
				if _t - self.stat[_ips[_idx]][2] > self.stat[_ips[_idx]][1]:
					#expires ttl, will be rechecked
					self.stat[_ips[_idx]][0] = True
					self.stat[_ips[_idx]][2] = _t
					break

				_idx = random.randint(0, _ips_n - 1)
				if _idx == __idx:
					_idx = (_idx + 1) % _ips_n

				_try += 1
				if _try >= _ips_n:
					break
			return _ips[_idx]

	def z_GET_backend(self):
		#resume send (large)request to backend
		self._r = self.server.z_reqs[self._f]
		return self.z_send_request_resume(self._r, self._f)

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
			try:
				self.z_host_sock.connect(self.z_host_addr)
			except socket.error as e: #ppp
				_addr_s = ''.join([self.z_host_addr[0],':',str(self.z_host_addr[1])])
				self.stat[_addr_s][0] = False
				self.stat[_addr_s][2] = time.time()
				if e.errno == errno.ECONNRESET:
					self.set_resp_code(502)
				elif e.errno == errno.ETIMEDOUT:
					self.set_resp_code(504)
				elif e.errno == errno.ECONNREFUSED:
					self.set_resp_code(503)
				else:
					self.set_resp_code(500)
				self.xResult = self.xR_ERR_5xx
				return

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

		#if no_recv:
		#	self.z_host_sock.shutdown(socket.SHUT_RD)

		try:
			if len(self._r) > self.send_buf_size/2:
				_buf_size = self.send_buf_size/2
				_once = False
			else:
				_buf_size = len(self._r)
				_once = True

			sent = self.z_host_sock.send(self._r[:_buf_size])

			if sent < _buf_size:
				_once = False

			if not _once:
				self.server.z_reqs_stat[self._f] = [sent, no_recv]
				try:
					self.server.epoll.register(self._f, select.EPOLLIN | select.EPOLLOUT)
				except IOError as e:
					self.server.epoll.modify(self._f, select.EPOLLIN | select.EPOLLOUT)
				except:
					raise
			else:
				if self._f in self.server.z_reqs_stat:
					self.server.z_reqs_stat.pop(self._f)
				try:
					self.server.epoll.register(self._f, select.EPOLLIN)
				except IOError as e:
					self.server.epoll.modify(self._f, select.EPOLLIN)
				except:
					raise

		except socket.error as e:
			return self.PARSE_ERROR
		except:
			raise

		if no_recv:
			if _once:
				try:
					self.server.epoll.unregister(self._f)
				except:
					raise
				self.z_host_sock.close()
				self.server.zconns.pop(self._f)
				self.server.zaddrs.pop(self._f)
				self.server.zhosts.pop(self._f)
		else:
			if self._f in self.server.z_reqs_cnt:
				self.server.z_reqs_cnt[self._f] += 1
			else:
				self.server.z_reqs_cnt[self._f] = 1

	def z_send_request_resume(self, _r, _f):
		#resume request sending
		if _f in self.server.z_reqs_stat:
			begin, no_recv = self.server.z_reqs_stat[_f]

			if len(_r[begin:]) > self.send_buf_size/2:
				_buf_size = self.send_buf_size/2
			else:
				_buf_size = len(_r[begin:])

			sent = self.z_host_sock.send(_r[begin:begin+_buf_size])

			if begin + sent < len(_r):
				self.server.z_reqs_stat[_f] = [begin + sent, no_recv]
			else:
				#all sent
				self.server.z_reqs_stat.pop(_f)

				if not no_recv:
					self.server.epoll.modify(_f, select.EPOLLIN)
				else:
					try:
						self.server.epoll.unregister(self._f)
					except:
						pass
					self.z_host_sock.close()
					self.server.zconns.pop(self._f)
					self.server.zaddrs.pop(self._f)
					self.server.zhosts.pop(self._f)

		return False

	def z_GET_init(self):
		#init connection to backend, send request, ggg
		_f = None
		try:
			self.z_hostname, _port = self.z_parse_address(self.in_headers.get("Host").lower())

			if self.z_hostname not in self.rra.keys() and not self.transparent_proxy:
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

			if self.xResult == self.xR_ERR_5xx:
				return

			self.z_send_request_init()
		except:
			self.xResult = self.xR_ERR_HANDLE

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
								#add to idle list
								self.server.zidles[str(self.server.zaddrs[_f])].appendleft(_f)
						else:
							self.server.zidles[str(self.server.zaddrs[_f])] = deque([_f])

						self.server.zconns_stat[_f] = [0, time.time()]	#conn idle

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
			if sent < should_sent:
				self.server.zcache_stat[_f][1] += sent
			else:
				self.server.zcache_stat[_f][0] += 1
				self.server.zcache_stat[_f][1] = 0

		#print "sent block:", blockno, sent, len(self.server.zcache[_f][blockno]), self.send_buf_size
		#print "zcache_stat:", self.server.zcache_stat[_f]

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

			try:
				self.server.z_resp_header[_f].pop("Connection")
			except:
				pass

			self.out_head_s = ''.join(['\n'.join(['%s: %s' % (k, v) for k, v in self.server.z_resp_header[_f].items()]), '\n'])

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
							with self.server.xcache_shelf_lock:
								del self.server.xcache_shelf[k]
						if hasattr(self.server.xcache_shelf.dict, 'firstkey'):
							#gdbm format
							with self.server.xcache_shelf_lock:
								del self.server.xcache_shelf[self.server.xcache_shelf.dict.firstkey()]
				except:
					pass

				try:
					#moved to self.server.check_lbs
					#while len(self.server.xcache_shelf.cache) > self.z_cache_size:
					#	self.server.xcache_shelf.cache.popitem()

					with self.server.xcache_shelf_lock:
						self.server.xcache_shelf[_xcache_key] = self.server.xcache[_xcache_key]
					#moved to self.server.check_lbs
					#if hasattr(self.server.xcache_shelf.dict, 'sync'):
					#	#self.server.xcache.dict is an anydbm object, mostly gdbm
					#	self.server.xcache_shelf.dict.sync()
				except:
					#may be problem in concurrent mode
					pass

	def z_transfer_backend(self, _f):
		#from backend
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

					if self.response_headers_parsed:
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

			if self.response_headers_parsed:
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
		#cut headers out
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
			#for reload config
			self.dfs_pool = {}
			self.dfs_pool_count = {}
			self.dfs_writer = []

			self.ttl = {}
			self.rra = {}
			self.rrn = {}

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

			if self.xResult == self.xR_ERR_5xx:
				return

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
						self.z_connect_backend(addr=self.z_host_addr)
						self.z_send_request_init()
					else:
						self.z_connect_backend(addr=self.z_host_addr, update_cb_conns=False)
						self.z_send_request_init(no_recv=True)
					_b_index += 1
			else:
				self.xResult = self.xR_ERR_HANDLE

		except:
			self.xResult = self.xR_ERR_HANDLE
			raise

class _xWHandler:

	server = None

	wdd_mode = 'server'
	wdd_dial = []

	encrypt = False
	encrypt_mode = None
	sess_encrypt_mode = {}
	aes = {}

	compress_tunnel = {}

	session = {}
	client_session = {}
	connected = {}

	tun_local_ip = {}
	tun_peer_ip = {}
	tun_mtu = {}
	tun_txqueue = {}
	token = {}
	e_token = {}	

	peer_ip = {}
	peer_port = {}

	tun_route = {}
	tun_rtt = {}
	route_metric = {}
	route_metric_fixed = {}
	routing_metric = False
	ifup_script = {}
	ifdown_script = {}
	rtch_script = {}

	udt_relay = {}
	udt_relay_thread_stat = {}

	IO_BLOCK = 0
	IO_NONBLOCK = 1

	io_mode = 0

	def __init__(self, conn, client_address, server, recv_buf_size=2760000, send_buf_size=2760000):
		self.server = server

		self.recv_buf_size = recv_buf_size
		self.send_buf_size = send_buf_size

		self.init_wdd_config()
		self.init_handler()

	def init_wdd_config(self):
		#for reload config
		self.wdd_mode = 'server'
		self.wdd_dial = []
		self.encrypt = False
		self.encrypt_mode = None
		self.sess_encrypt_mode = {}
		self.aes = {}
		self.session = {}
		self.client_session = {}
		self.connected = {}
		self.tun_local_ip = {}
		self.tun_peer_ip = {}
		self.tun_mtu = {}
		self.tun_txqueue = {}
		self.token = {}
		self.e_token = {}	
		self.peer_ip = {}
		self.peer_port = {}
		self.tun_route = {}
		self.tun_rtt = {}
		self.route_metric = {}
		self.route_metric_fixed = {}
		self.udt_relay = {}
		self.udt_relay_thread_stat = {}
		self.compress_tunnel = {}
		self.io_mode = 0
		self.routing_metric = False
		self.ifup_script = {}
		self.ifdown_script = {}
		self.rtch_script = {}

		self.config = ConfigParser.ConfigParser()
		if not self.config.read('3xsd.conf'):
			self.config.read('/etc/3xsd.conf')

		#
		#example: an udt tunnel session called: peer1
		#local ip: 10.19.27.1  peer ip: 10.19.27.2
		#mtu: 1500  txqueue: 1000  connect token(password): ddw3~)
		#
		#peer1 = 10.19.27.1:10.19.27.2:1500:1000:ddw3~)

		for name, value in self.config.items('3wdd'):
			if name == 'mode':
				self.wdd_mode = value.lower()
			elif name == 'dial':
				self.wdd_dial = value.split(',')
			elif name == 'encrypt':
				self.encrypt = True
				_value = value.lower()
				if _value == 'on' or  _value == 'aes-128-ecb':
					self.encrypt_mode = AES.MODE_ECB
				elif _value == 'aes-128-cbc':
					self.encrypt_mode = AES.MODE_CBC
				elif _value == 'aes-128-cfb':
					self.encrypt_mode = AES.MODE_CFB
				elif _value == 'aes-128-ctr':
					self.encrypt_mode = AES.MODE_CTR
				elif _value == 'blowfish-cbc':
					self.encrypt_mode = Blowfish.MODE_CBC + 100 #diff from aes
				elif _value == 'blowfish-cfb':
					self.encrypt_mode = Blowfish.MODE_CFB + 100
				elif _value == 'blowfish-ctr':
					self.encrypt_mode = Blowfish.MODE_CTR + 100
				else:
					self.encrypt = False
			elif name == 'io_mode':
				_value = value.lower()
				if _value == 'block' or _value == 'default':
					self.io_mode = self.IO_BLOCK
				elif _value == 'non_block':
					self.io_mode = self.IO_NONBLOCK
				else:
					self.io_mode = self.IO_BLOCK
			elif name == 'relay':
				for _from_to in value.split(','):
					_from, _to = _from_to.split(':')
					if _from and _to:
						self.udt_relay[_from] = (_from, _to)
						self.udt_relay[_to] = (_from, _to)
						self.udt_relay_thread_stat[_from] = False
						self.udt_relay_thread_stat[_to] = False
			elif name == 'routing_metric':
				_value = value.lower()
				if _value == 'on':
					self.routing_metric = True
			else:
				v = value.split(':')
				if len(v) >= 5:
					self.session[name] = True
					self.tun_local_ip[name] = v[0]
					self.tun_peer_ip[name] = v[1]
					self.tun_mtu[name] = int(v[2]) if v[2] else 0
					self.tun_txqueue[name] = int(v[3]) if v[3] else 0
					self.token[name] = v[4]
					self.e_token[name] = self.encrypt_token(name, v[4])
					if self.encrypt:
						if self.encrypt_mode == AES.MODE_CBC or self.encrypt_mode == AES.MODE_CFB:
							#aes-128-cbc, aes-128-cfb
							pass
						else:
							#aes-128-ecb as default
							if name not in self.aes:
								self.aes[name] = AES.new(self.e_token[name], AES.MODE_ECB)
				if len(v) > 5:
					if v[5]:
						_em_zip = v[5].lower().split(',')
						_em = _em_zip[0]

						if _em == 'aes-128-cbc':
							self.sess_encrypt_mode[name] = AES.MODE_CBC
						elif _em == 'aes-128-cfb':
							self.sess_encrypt_mode[name] = AES.MODE_CFB
						elif _em == 'aes-128-ctr':
							self.sess_encrypt_mode[name] = AES.MODE_CTR
						elif _em == 'on' or _em == 'aes-128-ecb':
							self.sess_encrypt_mode[name] = AES.MODE_ECB
							if name not in self.aes:
								self.aes[name] = AES.new(self.e_token[name], AES.MODE_ECB)
						elif _em == 'blowfish-cbc':
							self.sess_encrypt_mode[name] = Blowfish.MODE_CBC + 100
						elif _em == 'blowfish-cfb':
							self.sess_encrypt_mode[name] = Blowfish.MODE_CFB + 100
						elif _em == 'blowfish-ctr':
							self.sess_encrypt_mode[name] = Blowfish.MODE_CTR + 100

						if len(_em_zip) > 1:
							if _em_zip[1] == 'zlib' or _em_zip[1] == 'compress':
								self.compress_tunnel[name] = 'zlib'
							elif _em_zip[1] == 'lzo':
								self.compress_tunnel[name] = 'lzo'
				if len(v) > 7:
					if v[6]:
						self.peer_ip[name] = v[6]
						self.client_session[name] = True
						if v[7]:
							self.peer_port[name] = int(v[7])
						else:
							self.peer_port[name] = 9000
				if len(v) > 8 or len(v) == 7:
					self.tun_route[name] = []
					for route in v[len(v) - 1].split(','):
						if route:
							if "ifup=" in route:
								#not a 0.0.0.0/0 route, must be a ifup/ifdown script
								self.ifup_script[name] = route[5:]
								continue
							if "ifdown=" in route:
								self.ifdown_script[name] = route[7:]
								continue
							if "rtch=" in route:
								self.rtch_script[name] = route[5:]
								continue

							if route.count('/') == 2:
								_net, _mask, _s_metric = route.split('/')
								route = ''.join([_net, '/', _mask])
								_metric = int(_s_metric)
								if route in self.route_metric_fixed:
									self.route_metric_fixed[route][name] = _metric
								else:
									self.route_metric_fixed[route] = {name: _metric}

							self.tun_route[name].append(route)
							if route in self.route_metric:
								self.route_metric[route][name] = len(self.route_metric[route]) + 1
							else:
								self.route_metric[route] = {name: 1}

	def encrypt_token(self, session, token):
		md5 = hashlib.md5()
		md5.update(''.join([session, '#', token]))
		return md5.hexdigest()

	def init_handler(self):
		pass

	def connect_udt_server(self, target, new_port=None):  #ctud
		sock = udt.UdtSocket()
		sock.setsockopt(udt4.UDT_RCVBUF, self.server.recv_buf_size) #default 10MB
		sock.setsockopt(udt4.UDT_SNDBUF, self.server.send_buf_size) #default 10MB
		if new_port:
			_port = new_port
			_allow_redirect = 0
		else:
			_port = self.peer_port[target]
			if _port < 0:
				_port = abs(_port)
				_allow_redirect = 0
			else:
				_allow_redirect = 1

		_peer_ip = socket.gethostbyname(self.peer_ip[target])
		try:
			#print "connecting udt server", self.peer_ip[target], str(_port)
			sock.connect((_peer_ip, _port))
		except:
			sock.close()
			return
		_c_str = ''.join([target, ':', _peer_ip, ':', str(_port), ':', str(_allow_redirect)])
		sock.send(struct.pack('!i', len(_c_str)))
		sock.send(_c_str)
		try:
			_len = struct.unpack('!i', sock.recv(4))[0]
			_my_ip = sock.recv(_len)
		except:
			sock.close()
			return
		#the _s_token used to verify the two sides has 4 factors: session_name, passwd(token), server_ip, client_ip
		#this should be able to prevent from middleman attack & fake connect attempt
		_s_token = self.encrypt_token(self.e_token[target], ''.join([_peer_ip, '#', _my_ip]))
		sock.send(struct.pack('!i', len(_s_token)))
		sock.send(_s_token)
		try:
			_result = -1
			_result = struct.unpack('!i', sock.recv(4))[0]
		except:
			sock.close()
			return
		if _result == 0:
			self.setup_tunnel(target, sock, (_peer_ip, _port))
			self.connected[target] = True
			self.server.udt_conns_cnt[self.server._worker_id].value += 1
		elif _result == 1:
			_len = struct.unpack('!i', sock.recv(4))[0]
			_new_port_str = sock.recv(_len)
			sock.close()

			t = threading.Thread(target=self.connect_udt_server, args=(target,int(_new_port_str)))
			t.daemon = True
			t.start()

	def setup_tunnel(self, session_name, conn, addr): #stst
		try:
			if not conn or not addr or not session_name: return

			_do_relay = True if session_name in self.udt_relay else False

			_tun = None
			if not _do_relay:
				_tun_name = ''.join([session_name, '.', str(self.server._worker_id)])
				_tun = pytun.TunTapDevice(name=_tun_name, flags=pytun.IFF_TUN|pytun.IFF_NO_PI)

				_tun.addr = self.tun_local_ip[session_name]
				_tun.dstaddr = self.tun_peer_ip[session_name]

				_tun.netmask = '255.255.255.255'
				_tun.mtu = self.tun_mtu[session_name]

				with open(os.devnull, 'w') as devnull:
					subprocess.call(['ip', 'route', 'del', ''.join([_tun.dstaddr, '/', _tun.netmask])], stderr=devnull)

				self.server.ztuns[_tun.fileno()] = _tun
				self.server.s_tuns[_tun.fileno()] = session_name
				_tun.up()

				with open(os.devnull, 'w') as devnull:
					subprocess.call(['ip', 'link', 'set', _tun_name, 'txqueuelen', str(self.tun_txqueue[session_name])], stderr=devnull)

				if session_name in self.tun_route:
					if self.tun_route[session_name]:
						with open(os.devnull, 'w') as devnull:
							for route in self.tun_route[session_name]: #rtrt
								if route in self.route_metric:
									if self.route_metric[route][session_name] == -1:
										if len(self.route_metric[route]) > 1:
											j = 0
											for k in self.route_metric[route]:
												if k == session_name:
													break
												else:
													j += 1
											self.route_metric[route][session_name] = 327670 + j
											subprocess.call(['ip', 'route', 'add', route, 'metric', str(self.route_metric[route][session_name]), 'dev', _tun_name], stderr=devnull)
										else:
											self.route_metric[route][session_name] = 1
											subprocess.call(['ip', 'route', 'add', route, 'metric', '1', 'dev', _tun_name], stderr=devnull)
									else:
										subprocess.call(['ip', 'route', 'add', route, 'metric', str(self.route_metric[route][session_name]), 'dev', _tun_name], stderr=devnull)
								else:
									subprocess.call(['ip', 'route', 'del', route], stderr=devnull)
									subprocess.call(['ip', 'route', 'add', route, 'dev', _tun_name], stderr=devnull)

				_ifup_script = self.ifup_script.get(session_name, None)
				if _ifup_script:
					with open(os.devnull, 'w') as devnull:
						subprocess.call([_ifup_script, _tun_name], stderr=devnull)

			self.server.s_udts[conn.UDTSOCKET.UDTSOCKET] = session_name
			self.server.zsess[session_name] = (_tun, conn , addr)
			self.tun_rtt[session_name] = -1

			if self.encrypt or session_name in self.sess_encrypt_mode:
				if session_name in self.sess_encrypt_mode:
					_encrypt_mode = self.sess_encrypt_mode[session_name]
				else:
					_encrypt_mode = self.encrypt_mode
			else:
				_encrypt_mode = None

			if session_name in self.compress_tunnel:
				_compress = self.compress_tunnel[session_name]
			else:
				_compress = None

			if self.io_mode == self.IO_NONBLOCK:
				#io_mode == IO_NONBLOCK, single thread epoll to handle udt&tun events
				if _tun:
					flag = fcntl.fcntl(_tun.fileno(), fcntl.F_GETFL)
    					fcntl.fcntl(_tun.fileno(), fcntl.F_SETFL, flag | os.O_NONBLOCK)

				conn.setblocking(False)

				_n = conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit
				if self.server.upolls[_n] is None:
					self.server.upolls[_n] = udt.Epoll()
					self.server.upolls[_n].add_usock(conn, udt4.UDT_EPOLL_IN)
					self.udt_relay_thread_stat[session_name] = True
					if not _do_relay:
						self.server.upolls[_n].add_ssock(_tun, udt4.UDT_EPOLL_IN)
					t = threading.Thread(target=self.server.handle_event_udt_tun, args=(_n,))
					t.daemon = True
					t.start()
				else:
					self.server.upolls[_n].add_usock(conn, udt4.UDT_EPOLL_IN)
					self.udt_relay_thread_stat[session_name] = True
					if not _do_relay:
						self.server.upolls[_n].add_ssock(_tun, udt4.UDT_EPOLL_IN)
			else:
				#io_mode == IO_BLOCK (default), 2 threads bi-direction forwarding packages
				if not _do_relay:
					t = threading.Thread(target=self.server.forward_tun_udt,args=(_tun,conn,_encrypt_mode,_compress,session_name,))
					t.daemon = True
					t.start()
					t = threading.Thread(target=self.server.forward_udt_tun,args=(_tun,conn,_encrypt_mode,_compress,session_name,))
					t.daemon = True
					t.start()
				else:
					t = threading.Thread(target=self.server.forward_udt_relay,args=(conn,session_name,))
					t.daemon = True
					t.start()


			if _do_relay:
				print "UDT relay tunnel", session_name, "launched, no tun device, io_mode:", self.io_mode
			else:
				print "UDT tunnel", session_name, "launched, local", _tun.addr, "peer", _tun.dstaddr, "mtu", _tun.mtu, "encryption:", _encrypt_mode, "compress:", _compress, "io_mode:", self.io_mode

		except:
			if conn:
				try:
					if self.io_mode == self.IO_NONBLOCK and self.server.upolls[conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit]:
						self.server.upolls[conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit].remove_usock(conn)
				except:
					pass
				try:
					conn.close()
					del conn
				except:
					pass
			if _tun:
				try:
					if self.io_mode == self.IO_NONBLOCK and self.server.upolls[conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit]:
						self.server.upolls[conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit].remove_ssock(_tun)
				except:
					pass
				try:
					_tun.down()
					_tun.close()
					del _tun
				except:
					pass
			raise
				
	def destroy_tunnel(self, session_name):   #ddd
		_tun, _conn, _addr = self.server.zsess.pop(session_name, (None, None, None))
		self.connected.pop(session_name, None)

		self.tun_rtt.pop(session_name, None)
		if session_name in self.tun_route:
			for _route in self.tun_route[session_name]:
				self.route_metric[_route][session_name] = -1

		if _tun:
			print "Destroying", ''.join([session_name, '.', str(self.server._worker_id)]), _tun, _conn, _addr
		else:
			print "Destroying", ''.join([session_name, '.', str(self.server._worker_id)]), _conn, _addr

		if _conn and _addr:
			if _tun:
				self.server.ztuns.pop(_tun.fileno(), None)
				self.server.s_tuns.pop(_tun.fileno(), None)
			self.server.s_udts.pop(_conn.UDTSOCKET.UDTSOCKET, None)
			self.server.udt_conns_cnt[self.server._worker_id].value -= 1

			if session_name in self.server.udt_send_buf:
				self.server.udt_send_buf.pop(session_name)

			if self.io_mode == self.IO_NONBLOCK:
				try:
					_n = _conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit
					if self.server.upolls[_n]:
						self.server.upolls[_n].remove_usock(_conn)
						if _tun:
							self.server.upolls[_n].remove_ssock(_tun)
				except:
					pass

			try:
				#revoke mem, does it work?
				if _tun:
					_tun.down()
					_tun.close()
					del _tun

				_conn.close()
				del _conn
				del _addr
			except:
				pass

			if session_name in self.udt_relay_thread_stat:
				self.udt_relay_thread_stat[session_name] = False

			_ifdown_script = self.ifdown_script.get(session_name, None)
			if _ifdown_script:
				with open(os.devnull, 'w') as devnull:
					subprocess.call([_ifdown_script, ''.join([session_name, '.', str(self.server._worker_id)])], stderr=devnull)

	def setup_udt_connection(self, conn, addr):  #stud
		try:
			if not conn or not addr:
				return

			_len = 0
			_len = struct.unpack('!i', conn.recv(4))[0]
			if _len > 0:
				_c_str = conn.recv(_len)
				_session_name, _my_ip, _my_port_str, _allow_redirect_str = _c_str.split(':',3)
				_peer_ip, _peer_port = addr
				
				if _session_name not in self.session:
					#no such session config
					conn.close()
				else:
					conn.send(struct.pack('!i', len(_peer_ip)))
					conn.send(_peer_ip)

					_len = struct.unpack('!i', conn.recv(4))[0]
					if _len > 0:
						_s_token = conn.recv(_len)

						if _s_token == self.encrypt_token(self.e_token[_session_name], ''.join([_my_ip, '#', _peer_ip])):
							#pass, check idle worker
							if _allow_redirect_str == '1':
								#this value changed at every connection time
								#_idle_port = self.server.wdd_idle_worker(int(_my_port_str))

								#this value fixed for about 20 secs
								_idle_port = self.server.udt_conn_port.value
							else:
								_idle_port = int(_my_port_str)

							if _idle_port == int(_my_port_str):
								#tell client, setup the tunnel, put conn in epoll
								conn.send(struct.pack('!i', 0))
								if _session_name in self.connected:
									#only one tunnel per session
									self.destroy_tunnel(_session_name)
								self.setup_tunnel(_session_name, conn, addr)
								self.connected[_session_name] = True
								self.server.udt_conns_cnt[self.server._worker_id].value += 1
							else:
								#send redirect msg
								conn.send(struct.pack('!i', 1))
								conn.send(struct.pack('!i', len(str(_idle_port))))
								conn.send(str(_idle_port))
								conn.close()
						else:
							#sorry
							conn.close()
		except:
			if conn:
				try:
					conn.close()
					del conn
					del addr
				except:
					pass
			raise

	def decrypt_package(self, _buf, _encrypt_mode, _session):
		unpad = lambda s : s[0:-ord(s[-1])]
		if _encrypt_mode == Blowfish.MODE_CBC + 100:
			_blf = Blowfish.new(self.e_token[_session], _encrypt_mode - 100, _buf[:Blowfish.block_size])
			return unpad(_blf.decrypt(_buf[Blowfish.block_size:]))
		elif _encrypt_mode == Blowfish.MODE_CFB + 100:
			_blf = Blowfish.new(self.e_token[_session], _encrypt_mode - 100, _buf[:Blowfish.block_size])
			return _blf.decrypt(_buf[Blowfish.block_size:])
		elif _encrypt_mode == Blowfish.MODE_CTR + 100:
			_blf = Blowfish.new(self.e_token[_session], _encrypt_mode - 100, counter=Counter.new(64))
			return _blf.decrypt(_buf)
		elif _encrypt_mode == AES.MODE_CBC:
			_aes = AES.new(self.e_token[_session], _encrypt_mode, _buf[:AES.block_size])
			return unpad(_aes.decrypt(_buf[AES.block_size:]))
		elif _encrypt_mode == AES.MODE_CFB:
			_aes = AES.new(self.e_token[_session], _encrypt_mode, _buf[:AES.block_size])
			return _aes.decrypt(_buf[AES.block_size:])
		elif _encrypt_mode == AES.MODE_CTR:
			_aes = AES.new(self.e_token[_session], _encrypt_mode, counter=Counter.new(128))
			return _aes.decrypt(_buf)
		else:
			#AES.MODE_ECB
			return unpad(self.aes[_session].decrypt(_buf))


	def encrypt_package(self, _buf, _encrypt_mode, _session):
		if _encrypt_mode == Blowfish.MODE_CBC + 100:
			BS = Blowfish.block_size
			pad = lambda s: ''.join([s, (BS - len(s) % BS) * chr(BS - len(s) % BS)])
			_iv = Random.new().read(BS)
			_blf = Blowfish.new(self.e_token[_session], _encrypt_mode - 100, _iv)
			return ''.join([_iv, _blf.encrypt(pad(_buf))])
		elif _encrypt_mode == Blowfish.MODE_CFB + 100:  #CFB OFB CTR: padding is not required
			_iv = Random.new().read(Blowfish.block_size)
			_blf = Blowfish.new(self.e_token[_session], _encrypt_mode - 100, _iv)
			return ''.join([_iv, _blf.encrypt(_buf)])
		elif _encrypt_mode == Blowfish.MODE_CTR + 100:
			_blf = Blowfish.new(self.e_token[_session], _encrypt_mode - 100, counter=Counter.new(64))
			return _blf.encrypt(_buf)
		elif _encrypt_mode == AES.MODE_CBC:
			BS = AES.block_size
			pad = lambda s: ''.join([s, (BS - len(s) % BS) * chr(BS - len(s) % BS)])
			_iv = Random.new().read(BS)
			_aes = AES.new(self.e_token[_session], _encrypt_mode, _iv)
			return ''.join([_iv, _aes.encrypt(pad(_buf))])
		elif _encrypt_mode == AES.MODE_CFB:  #CFB OFB CTR: padding is not required
			_iv = Random.new().read(AES.block_size)
			_aes = AES.new(self.e_token[_session], _encrypt_mode, _iv)
			return ''.join([_iv, _aes.encrypt(_buf)])
		elif _encrypt_mode == AES.MODE_CTR:
			_aes = AES.new(self.e_token[_session], _encrypt_mode, counter=Counter.new(128))
			return _aes.encrypt(_buf)
		else:
			#AES.MODE_ECB
			BS = AES.block_size
			pad = lambda s: ''.join([s, (BS - len(s) % BS) * chr(BS - len(s) % BS)])
			return self.aes[_session].encrypt(pad(_buf))


	def handle_udt_tun_events(self, sets): #ooo
		for u in sets[0]:
			_un = u.UDTSOCKET.UDTSOCKET
			if _un in self.server.s_udts:
				_session = self.server.s_udts[_un]
			else:
				continue

			_encrypt_mode = self.sess_encrypt_mode[_session] if _session in self.sess_encrypt_mode else self.encrypt_mode
			_compress = self.compress_tunnel[_session] if _session in self.compress_tunnel else None
			_magic = {'zlib':(''.join([chr(0x78), chr(0x9c)]), 2), 'lzo':(''.join([chr(0xf0), chr(0x0), chr(0x0)]), 3)}
			_unzip = lambda s : eval(_compress).decompress(s) if _compress and _magic[_compress][0] in s[:_magic[_compress][1]] else s
			_forward2_tun = lambda s : self.server.zsess[_session][0].write(_unzip(self.decrypt_package(s, _encrypt_mode, _session))) if _encrypt_mode else self.server.zsess[_session][0].write(_unzip(s))
			_repack = lambda s : ''.join([struct.pack('!H', len(s)), s])

			try:
				#for i in xrange(10):
				if _session not in self.udt_relay:
					_forward2_tun(u.recv(struct.unpack('!H', u.recv(2))[0]))
				else:
					_from, _to = self.udt_relay[_session]
					if _session == _from:
						_to_s = _to
					else:
						_to_s = _from
					_, _to_usock, _ = self.server.zsess.get(_to_s, (None, None, None))
					if _to_usock:
						#print "relaying tunnel", _session, "to", self.udt_relay[_session]
						_buf = u.recv(struct.unpack('!H', u.recv(2))[0])
						_to_usock.send(_repack(_buf))
					else:
						#relay two sides not full connected yet
						continue
			except udt4.UDTException as e:
				if e[0] == udt4.EASYNCRCV:
					#recv buffer empty, no more data to read
					#print "recv", i, "packages from udt and write in", _t2 - _t1, "secs"
					continue
				elif e[0] == udt4.EASYNCSND:
					#send buffer full, just for relaying case
					if _to_s in self.server.udt_send_buf:
						self.server.udt_send_buf[_to_s].append(_buf)
					else:
						self.server.udt_send_buf[_to_s] = deque([_buf])
					_ux = _conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit
					self.server.upolls[_ux].remove_usock(u)
					self.server.upolls[_ux].add_usock(u, udt4.UDT_EPOLL_IN|udt4.UDT_EPOLL_OUT)
				if u.getsockstate() > 5:
					self.server.upolls[_un % self.server.udt_thread_limit].remove_usock(u)
					self.udt_relay_thread_stat[_session] = False
			except IOError as e:
				if e.errno == errno.EINVAL:
					#illegal data, maybe tunnel peer shutdown suddenly
					continue
		for u in sets[1]:
			_un = u.UDTSOCKET.UDTSOCKET
			if _un in self.server.s_udts:
				_session = self.server.s_udts[_un]
			else:
				continue

			if _session in self.server.udt_send_buf:
				try:
					u.send(self.server.udt_send_buf[_session][0])
					self.server.udt_send_buf[_session].popleft()
					if len(self.server.udt_send_buf[_session]) == 0:
						self.server.udt_send_buf.pop(_session, None)
				except: 
					if u.getsockstate() > 5:
						self.server.udt_send_buf.pop(_session, None)
						self.server.upolls[_un % self.server.udt_thread_limit].remove_usock(u)
			else:
				_ux = u.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit
				self.server.upolls[_ux].remove_usock(u)
				self.server.upolls[_ux].add_usock(u, udt4.UDT_EPOLL_IN)

		for _tun in sets[2]:
			if _tun.fileno() == -1: continue

			_session = self.server.s_tuns[_tun.fileno()]
			_, _conn = self.server.zsess[_session][:2]

			_encrypt_mode = self.sess_encrypt_mode[_session] if _session in self.sess_encrypt_mode else self.encrypt_mode
			_compress = self.compress_tunnel[_session] if _session in self.compress_tunnel else None
			_zip = lambda s : eval(_compress).compress(s) if _compress and len(s) < _tun.mtu - 100 else s
			_encrypt=lambda s : self.encrypt_package(_zip(s), _encrypt_mode, _session) if _encrypt_mode else _zip(s)
			_repack = lambda s : ''.join([struct.pack('!H', len(s)), s])
			
			try:
				#for i in xrange(10):
				_buf = _repack(_encrypt(_tun.read(_tun.mtu)))
				_conn.send(_buf)

			except IOError as e:
				#no more tun data to read
				#print "read", i+1, "packages from tun, and sent in", _t2 - _t1, "secs"
				continue
			except udt4.UDTException as e:
				if e[0] == udt4.EASYNCSND:
					#send buffer full
					if _session in self.server.udt_send_buf:
						self.server.udt_send_buf[_session].append(_buf)
					else:
						self.server.udt_send_buf[_session] = deque([_buf])
					_ux = _conn.UDTSOCKET.UDTSOCKET % self.server.udt_thread_limit
					self.server.upolls[_ux].remove_usock(_conn)
					self.server.upolls[_ux].add_usock(_conn, udt4.UDT_EPOLL_IN|udt4.UDT_EPOLL_OUT)
