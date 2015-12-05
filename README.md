##3xsd
3xsd is a native epoll server serving TCP/UDP connections, a high performance static web server, a
failover dns server, a http-based distributed file server, and a load-balance proxy-cache server.

##Features in detail:

###3wsd - web server
       supporting: static files, event driven(epoll), using mmap & sendfile to send files,
       in-mem xcache, transparent gzip content transfer with fixed length(small file) & 
       chunked(large file), persistent storage of gzip files,
       partial support of WebDAV(PUT/DELETE), pipelining support
       
###3nsd - dns server
       supporting: only A record resolution, domainname failover(refer to conf file),
       ip icmp probe & hide when fail, round robbin ip resolving
       
###3zsd - proxy server
       supporting: load balance backend servers, in-mem file caching & 
       persistent cache file storage

###3fsd - distribute web file system
       supporting: mass unlimitted file storage, easy to expand,
       O(1) location algorithm, non-centralized, can work with standard web server(WebDAV)
       in proxy mode, file redundancy, file persistent caching

More to find in .conf file.

##Performance:

###3wsd:
  Small file under 1KB single process test(full in-mem), contrast with nginx configuring
  accept_mutex off, 80% performance.
  Multi processes test, with reuse_port enabling kernel, 95% performance of nginx(and beyond,
  may be 105% or more, based on process number, I tested 2-4).
  The tests above is not quite strict， but I just want to say that it's fast enough.

  And with pipelining enabled, 3wsd will perform better with 3-4 requests/send(5%-10%
  performance increase), 2 requests/send have the same speed with non-piplining.

###3zsd:
  About 80% performance of 3wsd.
  
###3nsd:
  Fast enough...about 2800-3000 queries/s per processes, with 1GHz bcm2709 4-cores ARMv7
  cpu testing, better when multi-processes with reuse_port enabling kernel.
  
###3fsd:
  Same with 3zsd.
  
###More performance:
There are at lease two ways to increase the performance of 3xsd:

       1.Install Cython, and rename _3xsd.py to _3xsd.pyx, run it. 
       Cython will compile _3xsd.py lib into a _3xsd.so file, using static type
       declarations. This can gain about 5%-6% performance increasement.
       2.Use PyPy.This can gain about 10%-15% performance increasement(or more).

#OS requirement & install: 

CentOS 6/7 with python 2.6/2.7, Debian 6/7.

Doing this before running the program(minimal requirement):

       yum install python-gevent pysendfile python-setproctitle python-psutil python-pip
       
       (python-pip is optional if install dpkt)
  
Dpkt module is also needed when running 3nsd DNS server, pip install it.

Or, of cause you can let pip do it all for you:

       pip install 3xsd

In a debian, you can use apt-get to install python-pip(pip) or python-setuptools(easy_install),
then to install the packages following.

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
