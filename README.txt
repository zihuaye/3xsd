3xsd is a native epoll server serving TCP/UDP connections, a high performance static web server, a failover dns server, a http-based distributed file server(implementing), and a load-balance proxy-cache server.

Features in detail:

3wsd - web server supporting: static files, using mmap & sendfile to send files with in-mem xcache, 
       transparent gzip file transfer with fixed length(small file) & chunked(large file), 
       persistent storage of gzip files
       
3nsd - dns server supporting: only A record resolution, domainname failover(refer to conf file),
       ip icmp probe & hide when fail, round robbin ip resolving
       
3zsd - proxy server supporting: load balance backend servers, in-mem file caching & 
       persistent cache file storage

3fsd - distribute web file system supporting: mass unlimitted file storage, easy to expand, 
       O(1) location algorithm, non-centralized, can work with stand web server(WebDAV) in proxy mode

More to find in .conf file.

Performance:

  Small file under 1KB single process test(full in-mem), with nginx configuring accept_mutex off, 80% performance.
  Multi process test, with reuse_port enabling kernel, 95% performance of nginx.
  The test is not quite strict， but I just wan to say it's fast enough.
  
OS requirement: CentOS 6.x with python 2.6/2.7, Debian 6/7.

Doing this before running the program(minimal requirement):

  yum install python-gevent pysendfile python-setproctitle python-psutil python-pip(optional if install dpkt)

Dpkt module is also needed when running 3nsd DNS server, pip install it.

Or, of cause you can let pip do it all for you:
  pip install 3xsd

In a debian, you can use apt-get to install python-pip(pip) or python-setuptools(easy_install), then to install the packages following.

Python Packages(Modules) version reference:
  gevent==0.13.8(1.0.1, 1.1)
  greenlet==0.4.2
  pysendfile==2.0.1
  setproctitle==1.0.1
  psutil==0.6.1
  dpkt==1.6

To install a module of specific version(like gevent 0.13.8), you can:
  pip install gevent==0.13.8

This will install the latest version of gevent(pypy will need it):
  pip install git+git://github.com/surfly/gevent.git#egg=gevent
