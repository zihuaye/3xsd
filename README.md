##3xsd
3xsd is a native epoll server serving TCP/UDP connections, a high performance static web server, a
failover dns server, a http-based distributed file server, a load-balance proxy-cache server, and
a 'warp drive' server. Written in python, take the full power of multi-cores.

##Features in detail:

###3wsd - web server
       supporting: static files, event driven(epoll), using mmap & sendfile to send files,
       in-mem xcache, transparent gzip content transfer with fixed length(small file) & 
       chunked(large file), persistent storage of gzip files,
       partial support of WebDAV(PUT/DELETE), pipelining support
       
###3nsd - dns server
       supporting: only A record resolution, domainname failover(refer to conf file),
       ip icmp probe & hide when fail, round robbin ip resolving
       global DNS Left-Right Range Resolve(LRRR)(experimental)
       
###3zsd - proxy server
       supporting: load balance backend servers, in-mem file caching & 
       persistent cache file storage

###3fsd - distribute web file system
       supporting: mass unlimitted file storage, easy to expand,
       O(1) location algorithm, non-centralized, can work with standard web server(WebDAV)
       in proxy mode, file redundancy, file persistent caching

###3wdd - 'warp drive' server
       supporting: data tunneling over UDT and tun,
       better congestion control than TCP/UDP over wan link,
       better thoughput(above 80%) over wan link, refer to this report:
       http://www.c-s-a.org.cn/ch/reader/create_pdf.aspx?file_no=20091035
       tunnel ip/mtu/txqueuelen/route define, auto create/recreate/destroy
       encrypt packages through AES-128-ECB/CBC/CFB/CTR and Blowfish-CBC/CFB/CTR
       tunnel on-the-fly compress with zlib/lzo, tunnel data relaying
       route metric, routing data through different path, depending on tunnel rtt(choose the best one)

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
  
###3wdd:
  Early testing indicated that:
  UDT tunnel(no encrypt) performing 50%-60% speed of direct TCP connection with ZetaTCP,
  and package lost rate remaining below 0.6%, while direct connection has 1.4%-3%.
  (Test CN-US WAN link with 150ms-280ms latency, through the always-jammed CUCN submarine cable)
  However, UDT tunnel beats normal TCP connection without ZetaTCP, with 50% - 4 times
  (commonly 1-2 times) outperforming.(v)(Test link like above)
  
  Update:
  And an encrypted UDT tunnel with AES-CBC/CFB will has 50% performance decrease(because of the 
  iv doubled transfer data).
  Now with a Blowfish-CTR method, tunnel data transfer performance is closed to raw non-encrypt 
  tunnel. I believe that with a intel AES-NI supported CPU(like XEON E3-1240/1270), AES-128-CTR
  can also do it.

###More performance:
There are at lease two ways to increase the performance of 3xsd:

       1.Install Cython, and rename _3xsd.py to _3xsd.pyx, run it. 
       Cython will compile _3xsd.py lib into a _3xsd.so file, using static type
       declarations. This can gain about 5%-6% performance increasement.
       2.Use PyPy.This can gain about 10%-15% performance increasement(or more).

#OS requirement & install: 

CentOS 6/7 with python 2.6/2.7, Debian 6/7. Python 2.7 recommended.

Doing this before running the program(minimal requirement):

       yum install python-gevent pysendfile python-setproctitle python-psutil python-pip
       
       (python-pip is optional if install dpkt)
  
Dpkt module is also needed when running 3nsd DNS server, pip install it.

If you want to use 3wdd, python-pytun, pyudt4, pycrypto, python-lzo are also needed.

       yum install python-crypto2.6 python-lzo

will quickly install pycrypto(probably do some 'linking' works) and lzo. The other two depended on pip install.

Probably you need this easy-install.pth file in python's site-packages dir:

       import sys; sys.__plen = len(sys.path)
       ./pycrypto-2.6.1-py2.6-linux-x86_64.egg
       ./pyudt4-0.6.0-py2.6-linux-x86_64.egg
       import sys; new=sys.path[sys.__plen:]; del sys.path[sys.__plen:]; p=getattr(sys,'__egginsert',0); sys.path[p:p]=new;          sys.__egginsert = p+len(new)

I provide a pre-compiled package [pyudt_tun-centos6-x86_64.tar.gz](https://github.com/zihuaye/3xsd/blob/master/pyudt_tun-centos6-x86_64.tar.gz) to simplify
the installation procedure of pyudt4 & python-pytun.

Be aware of pyudt4 having some bugs, you'd better download it's source code of epoll-fixes branch and 
apply the patch I offered. See changelog.txt v0.0.20  2016.03.07 fixed section for detail.
(Already included in [pyudt_tun-centos6-x86_64.tar.gz](https://github.com/zihuaye/3xsd/blob/master/pyudt_tun-centos6-x86_64.tar.gz))

Or, of cause you can let pip do it all for you(not including patching pyudt4):

       pip install 3xsd

In a debian, you can use apt-get to install python-pip(pip) or python-setuptools(easy_install),
then to install the packages following.

Python Packages(Modules) version reference:

       gevent==0.13.8(1.0.1, 1.1)
       greenlet==0.4.2
       pysendfile==2.0.1
       setproctitle==1.0.1
       psutil==0.6.1
       dpkt==1.6(1.8.6)
       python-pytun==2.2.1
       pyudt4==0.6.0(epoll-fixes branch)
       pycrypto==2.6.1
       python-lzo==1.8

System libs version reference:

       libevent-1.4.13-4(not actually used, just needed for gevent to function)
       udt-4.11-6
       lzo-2.03-3.1

To install a module of specific version(like gevent 0.13.8), you can:

       pip install gevent==0.13.8

This will install the latest version of gevent(pypy will need it):

       pip install git+git://github.com/surfly/gevent.git#egg=gevent

