from setuptools import setup

setup(
	name='3xsd',
	version='0.0.22',

	description='3xsd is a native epoll server serving TCP/UDP connections, a high performance static web server, a failover dns server, a http-based distributed file server, a load-balance proxy-cache server, and a warp-drive server. Written in python, take the full power of multi-cores.',

	url='https://github.com/zihuaye/3xsd',

	author='Zihua Ye',
	author_email='zihua.ye@gmail.com zihua.ye@qq.com',

	license='GPLv2',

	classifiers=[
        'Topic :: Internet :: WWW/HTTP :: HTTP Servers',
	'Topic :: Internet :: Name Service (DNS)',
	'Topic :: Internet :: Proxy Servers',
	'Topic :: System :: Filesystems',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
	],

	py_modules=['_3xsd'],

	scripts=['3xsd'],

	install_requires=['gevent', 'setproctitle', 'psutil', 'dpkt', 'pysendfile', 'python-pytun', 'pyudt4', 'pycrypto', 'python-lzo']
)
