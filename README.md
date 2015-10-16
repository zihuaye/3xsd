3xsd is a native epoll server serving TCP/UDP connections, a high performance static web server, a failover dns server, a http-based distributed file server(implementing), and a load-balance proxy-cache server.

OS requirement: CentOS 6.x with python 2.6/2.7, Debian 6/7.

Doing this before running the program(minimal requirement):

  yum install python-gevent pysendfile python-setproctitle python-psutil python-pip(optional if install dpkt)

dpkt module is also needed when running 3nsd DNS server, pip install it.

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
