#! /usr/bin/env python

#    cfv - Command-line File Verify
#    Copyright (C) 2000-2009  Matthew Mueller <donut AT dakotacom DOT net>
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

import getopt, re, os, sys, errno, time, copy, struct, codecs
from stat import *

cftypes={}
_cf_fn_exts, _cf_fn_matches, _cf_fn_searches = [], [], []
_cf_matchers = []

LISTOK=512
LISTBAD=1024
LISTNOTFOUND=2048
LISTUNVERIFIED=4096
LISTARGS={'ok':LISTOK, 'bad':LISTBAD, 'notfound':LISTNOTFOUND, 'unverified':LISTUNVERIFIED}

class Data:
	def __init__(self, **kw):
		self.__dict__.update(kw)

def chomp(line):
	if line[-2:] == '\r\n': return line[:-2]
	elif line[-1:] in '\r\n': return line[:-1]
	return line

def chompnulls(line):
	p = line.find('\0')
	if p < 0: return line
	else:     return line[:p]

def lchoplen(line, max):
	if len(line)>max:
		return '...'+line[-(max-3):]
	return line

try:
	''.lstrip('a') #str.lstrip(arg) only in python>=2.2
	def lstrip(s,c):
		return s.lstrip(c)
except TypeError:
	def lstrip(s, c):
		for i in range(0,len(s)):
			if s[i]!=c:
				break
		return s[i:]

try:
	sorted #sorted(seq) only in python>=2.4
except NameError:
	def sorted(seq):
		l = list(seq)
		l.sort()
		return l

_realpath = getattr(os.path, 'realpath', os.path.abspath) #realpath is only in Python>=2.2
_path_key_cache = {}
def get_path_key(path):
	dk = _path_key_cache.get(path)
	if dk is not None:
		return dk
	st = os.stat(path or os.curdir)
	if st[ST_INO]:
		dk = (st[ST_DEV],  st[ST_INO])
	else:
		dk = _realpath(os.path.join(curdir, path))
	_path_key_cache[path] = dk
	return dk
	
curdir=os.getcwd()
reldir=['']
prevdir=[]
def chdir(d):
	global curdir,_path_key_cache
	os.chdir(d)
	prevdir.append((curdir,_path_key_cache))
	reldir.append(os.path.join(reldir[-1], d))
	curdir=os.getcwd()
	_path_key_cache = {}
def cdup():
	global curdir,_path_key_cache
	reldir.pop()
	curdir,_path_key_cache=prevdir.pop()
	os.chdir(curdir)


def getscrwidth():
	w = -1
	try:
		from fcntl import ioctl
		try:
			from termios import TIOCGWINSZ
		except ImportError:
			from TERMIOS import TIOCGWINSZ
		tty = sys.stdin.isatty() and sys.stdin or sys.stdout.isatty() and sys.stdout or sys.stderr.isatty() and sys.stderr or None
		if tty:
			h,w = struct.unpack('h h', ioctl(tty.fileno(), TIOCGWINSZ, '\0'*struct.calcsize('h h')))
	except ImportError:
		pass
	if w>0:
		return w
	c = os.environ.get('COLUMNS',80)
	try:
		return int(c)
	except ValueError:
		return 80
scrwidth = getscrwidth()

stdout = sys.stdout
stderr = sys.stderr
_stdout_special = 0
stdinfo = sys.stdout
stdprogress = None
def set_stdout_special():
	"""If stdout is being used for special purposes, redirect informational messages to stderr."""
	global stdinfo, _stdout_special
	_stdout_special = 1
	stdinfo = stderr

class CodecWriter:
	"""Similar to codecs.StreamWriter, but str objects are passed through directly to the output stream.
	This is necessary as some codecs barf on trying to encode even ascii strings.
	Also, filenames read from disk will not be unicode, but rather strings encoded in the filesystemencoding, which will make any StreamWriter barf.
	But printing them directly should (hopefully) work fine.
	"""
	#Another approach could be to convert str objects to unicode and then pass them to encode, but that would be extra work.
	def __init__(self, encoding, stream, errors='strict'):
		self.stream = stream
		self.errors = errors
		self.encode = codecs.lookup(encoding)[0] #codecs.getencoder(encoding) #codecs.getencoder is only in python2.2+, but lookup(...)[0] is equivalent
	def write(self, object):
		if type(object)==type(''):
			self.stream.write(object)
		else:
			data, consumed = self.encode(object, self.errors)
			self.stream.write(data)
	def writelines(self, list):
		self.write(''.join(list))
	def __getattr__(self, name, getattr=getattr):
		""" Inherit all other methods from the underlying stream.
		"""
		return getattr(self.stream, name)

codec_error_handler = 'backslashreplace'
try:
	codecs.lookup_error(codec_error_handler) #backslashreplace is only in python2.3+
except:
	codec_error_handler = 'replace'

def setup_output():
	global stdinfo,stdprogress,stdout,stderr
	stdout = CodecWriter(getattr(sys.stdout,'encoding',None) or 'ascii', sys.stdout, errors=codec_error_handler)
	stderr = CodecWriter(getattr(sys.stderr,'encoding',None) or getattr(sys.stdout,'encoding',None) or 'ascii', sys.stderr, errors=codec_error_handler)
	stdinfo = _stdout_special and stderr or stdout
	# if one of stdinfo (usually stdout) or stderr is a tty, use it.  Otherwise use stdinfo.
	stdprogress = stdinfo.isatty() and stdinfo or stderr.isatty() and stderr or stdinfo
	doprogress = not config.verbose==-2 and (
			config.progress=='y' or (
				config.progress=='a' and stdprogress.isatty()
			)
		)
	if not doprogress:
		stdprogress = None

def pverbose(s,nl='\n'):
	if config.verbose>0:
		stdinfo.write(s+nl)
def pinfo(s,nl='\n'):
	if config.verbose>=0 or config.verbose==-3:
		stdinfo.write(s+nl)
def perror(s,nl='\n'):
	if config.verbose>=-1:
		stdout.flush() # avoid inconsistent screen state if stdout has unflushed data
		stderr.write(s+nl)

def plistf(filename):
	stdout.write(perhaps_showpath(filename)+config.listsep)

class INF:
	"""object that is always larger than what it is compared to"""
	def __cmp__(self, other):
		return 1
	def __mul__(self, other):
		return self
	def __div__(self, other):
		return self
	def __rdiv__(self, other):
		return 0
INF=INF()

class ProgressMeter:
	spinnerchars=r'\|/-'
	def __init__(self, steps=20):
		self.wantsteps=steps
		self.needrefresh=1
		self.filename=None

	def init(self, name, size=None, cursize=0):
		self.steps = self.wantsteps
		self.filename = name
		if size is None:
			if name != '' and os.path.isfile(name):
				size = os.path.getsize(name)
		self.name = lchoplen(perhaps_showpath(name), scrwidth - self.steps - 4)
		if not size: #if stdin or device file, we don't know the size, so just use a spinner.  If the file is actually zero bytes, it doesn't matter either way.
			self.stepsize = INF
			self.steps = 1
		elif size<=self.steps:
			self.stepsize = 1
		else:
			self.stepsize = size/self.steps
		self.nextstep = self.stepsize
		self.spinneridx = 0
		self.needrefresh = 1
		self.update(cursize)
	
	def update(self, cursize):
		if self.needrefresh:
			donesteps = cursize/self.stepsize
			stepsleft = self.steps - donesteps
			self.nextstep = self.stepsize*(donesteps+1)
			stdprogress.write('%s : %s'%(self.name,'#'*donesteps+'.'*stepsleft)+'\b'*stepsleft); stdprogress.flush()
			self.needrefresh = 0
		elif self.nextstep < cursize:
			updsteps = (cursize-self.nextstep)/self.stepsize + 1
			self.nextstep = self.nextstep + self.stepsize*updsteps
			stdprogress.write('#'*updsteps); stdprogress.flush()
		else:
			stdprogress.write(self.spinnerchars[self.spinneridx]+'\b'); stdprogress.flush()
			self.spinneridx = (self.spinneridx+1)%len(self.spinnerchars)
	
	def cleanup(self):
		if not self.needrefresh:
			stdprogress.write('\r'+' '*(len(self.name)+3+self.steps+1)+'\r')
			self.needrefresh = 1

class TimedProgressMeter(ProgressMeter):
	def __init__(self, *args, **kw):
		ProgressMeter.__init__(self, *args, **kw)
		self.nexttime = 0
	
	def update(self, cursize):
		curtime = time.time()
		if curtime > self.nexttime:
			self.nexttime = curtime + 0.06
			ProgressMeter.update(self, cursize)


def enverrstr(e):
	return getattr(e,'strerror',None) or str(e)

class CFVException(Exception):
	pass

class CFVValueError(CFVException):#invalid argument in user input
	pass

class CFVNameError(CFVException):#invalid command in user input
	pass

class CFVSyntaxError(CFVException):#error in user input
	pass


class FileInfoCache:
	def __init__(self):
		self.data = {}
		self.stdin = {}
		self.testfiles = {}
	
	def set_testfiles(self, testfiles):
		for fn in testfiles:
			fn = os.path.join(reldir[-1], fn)
			if config.ignorecase:
				fn = fn.lower()
			self.testfiles[fn] = 1
			
	def should_test(self, fn):
		fn = os.path.join(reldir[-1], fn)
		if config.ignorecase:
			fn = fn.lower()
		return self.testfiles.get(fn,0)
	
	def set_verified(self, fn):
		self.getfinfo(fn)['_verified'] = 1
	
	def is_verified(self, fn):
		return self.getfinfo(fn).get('_verified',0)
	
	def set_flag(self, fn, flag):
		self.getfinfo(fn)[flag] = 1
	
	def has_flag(self, fn, flag):
		return self.getfinfo(fn).has_key(flag)
	
	def getpathcache(self, path):
		pathkey = get_path_key(path)
		pathcache = self.data.get(pathkey)
		if pathcache is None:
			self.data[pathkey] = pathcache = {}
		return pathcache
	
	def getfinfo(self, fn):
		if fn=='':
			return self.stdin
		else:
			fpath,ftail = os.path.split(fn)
			pathdata = self.getpathcache(fpath)
			finfo = pathdata.get(ftail)
			if finfo is None:
				pathdata[ftail] = finfo = {}
			return finfo
	
	def rename(self, oldfn, newfn):
		ofinfo = self.getfinfo(oldfn)
		nfinfo = self.getfinfo(newfn)
		nfinfo.clear()
		for k,v in ofinfo.items():
			if k[0]!="_": #don't preserve flags
				nfinfo[k]=v
		#nfinfo.update(ofinfo)
		ofinfo.clear()

		
def getfilesha1(file):
	finfo = cache.getfinfo(file)
	if not finfo.has_key('sha1'):
		finfo['sha1'],finfo['size'] = _getfilesha1(file)
	return finfo['sha1'],finfo['size']

def getfilemd5(file):
	finfo = cache.getfinfo(file)
	if not finfo.has_key('md5'):
		finfo['md5'],finfo['size'] = _getfilemd5(file)
	return finfo['md5'],finfo['size']

def getfilecrc(file):
	finfo = cache.getfinfo(file)
	if not finfo.has_key('crc'):
		finfo['crc'],finfo['size'] = _getfilecrc(file)
	return finfo['crc'],finfo['size']
	
def rename(oldfn, newfn):
	os.rename(oldfn, newfn)
	cache.rename(oldfn, newfn)

class Stats:
	def __init__(self):
		self.num=0
		self.ok=0
		self.badsize=0
		self.badcrc=0
		self.notfound=0
		self.ferror=0
		self.cferror=0
		self.bytesread=0L #use long int for size, to avoid possible overflow.
		self.unverified=0
		self.diffcase=0
		self.misnamed=0
		self.quoted=0
		self.textmode=0
		self.starttime=time.time()
		self.subcount=0

	def make_sub_stats(self):
		b = copy.copy(self)
		b.starttime = time.time()
		return b
	
	def sub_stats_end(self, end):
		for v in 'badcrc', 'badsize', 'bytesread', 'cferror', 'diffcase', 'misnamed', 'ferror', 'notfound', 'num', 'ok', 'quoted', 'unverified', 'textmode':
			setattr(self, v, getattr(end, v) - getattr(self, v))
		end.subcount = end.subcount + 1

	def _stats(self):
		pinfo('%i files'%self.num,'')
		pinfo(', %i OK' %self.ok,'')
		if self.badcrc:
			pinfo(', %i badcrc' %self.badcrc,'')
		if self.badsize:
			pinfo(', %i badsize' %self.badsize,'')
		if self.notfound:
			pinfo(', %i not found' %self.notfound,'')
		if self.ferror:
			pinfo(', %i file errors' %self.ferror,'')
		if self.unverified:
			pinfo(', %i unverified' %self.unverified,'')
		if self.cferror:
			pinfo(', %i chksum file errors' %self.cferror,'')
		if self.misnamed:
			pinfo(', %i misnamed' %self.misnamed,'')
		if self.diffcase:
			pinfo(', %i differing cases' %self.diffcase,'')
		if self.quoted:
			pinfo(', %i quoted filenames' %self.quoted,'')
		if self.textmode:
			pinfo(', %i tested in textmode' %self.textmode,'')

		elapsed=time.time()-self.starttime
		pinfo('.  %.3f seconds, '%(elapsed),'')
		if elapsed==0.0:
			pinfo('%.1fK'%(self.bytesread/1024.0),'')
		else:
			pinfo('%.1fK/s'%(self.bytesread/elapsed/1024.0),'')

		pinfo('\n','')

class Config:
	verbose=0 # -1=quiet  0=norm  1=noisy
	docrcchecks=1
	dirsort=1
	cmdlinesort=1
	cmdlineglob='a'
	recursive=0
	showunverified=0
	defaulttype='sfv'
	ignorecase=0
	unquote=0
	fixpaths=None
	strippaths=0
	showpaths=2
	showpathsabsolute=0
	gzip=0
	rename=0
	search=0
	renameformat='%(name)s.bad-%(count)i%(ext)s'
	renameformatnocount=0
	list=0
	listsep='\n'
	user_cf_fn_regexs=[]
	dereference=1
	progress='a'
	announceurl=None
	piece_size_pow2=18
	private_torrent=False
	def setdefault(self,cftype):
		if cftype in cftypes.keys():
			self.defaulttype=cftype
		else:
			raise CFVValueError, "invalid default type '%s'"%cftype
	def setintr(self,o,v,min,max):
		try:
			x=int(v)
			if x>max or x<min:
				raise CFVValueError, "out of range int '%s' for %s"%(v,o)
			self.__dict__[o]=x
		except ValueError:
			raise CFVValueError, "invalid int type '%s' for %s"%(v,o)
	def setbool(self,o,v):
		v = v.lower()
		if v in ('yes','on','true','1'):
			x=1
		elif v in ('no','off','false','0'):
			x=0
		else:
			raise CFVValueError, "invalid bool type '%s' for %s"%(v,o)
		self.__dict__[o]=x
	def setyesnoauto(self,o,v):
		if 'yes'.startswith(v.lower()):
			self.__dict__[o]='y'
		elif 'auto'.startswith(v.lower()):
			self.__dict__[o]='a'
		elif 'no'.startswith(v.lower()):
			self.__dict__[o]='n'
		else:
			raise CFVValueError, "invalid %s option '%s', must be 'no', 'auto', or 'yes'"%(o,v)
	def setstr(self,o,v):
		self.__dict__[o]=v
	def setx(self,o,v):
		if o=="default":
			self.setdefault(v)
		elif o in ("dirsort","cmdlinesort","ignorecase","rename","search","dereference","unquote","private_torrent"):
			self.setbool(o,v)
		elif o in ("cmdlineglob", "progress"):
			self.setyesnoauto(o,v)
		elif o=="verbose":
			try:
				self.setintr(o,v,-3,1)
			except CFVValueError:
				if   v=='v':  self.verbose=1
				elif v=='V':  self.verbose=0
				elif v=='VV': self.verbose=-1
				elif v=='q':  self.verbose=-2
				elif v=='Q':  self.verbose=-3
				else:
					raise CFVValueError, "invalid verbose option '%s', must be 'v', 'V', 'VV', 'q', 'Q' or -3 - 1"%v
		elif o in ("gzip",):
			self.setintr(o,v,-1,1)
		elif o in ("recursive","showunverified"):
			self.setintr(o,v,0,2)
		elif o=="showpaths":
			p=0
			a=0
			for v in v.split('-'):
				if not p:
					if 'none'.startswith(v.lower()) or v=='0':
						self.showpaths=0
						p=1; continue
					elif 'auto'.startswith(v.lower()) or v=='2':
						self.showpaths=2
						p=1; continue
					elif 'yes'.startswith(v.lower()) or v=='1':
						self.showpaths=1
						p=1; continue
				if not a:
					if 'absolute'.startswith(v.lower()):
						self.showpathsabsolute=1
						a=1; continue
					elif 'relative'.startswith(v.lower()):
						self.showpathsabsolute=0
						a=1; continue
				raise CFVValueError, "invalid showpaths option '%s', must be 'none', 'auto', 'yes', 'absolute', or 'relative'"%v
		elif o=="strippaths":
			if 'none'.startswith(v.lower()):
				self.strippaths='n'
			elif 'all'.startswith(v.lower()):
				self.strippaths='a'
			else:
				try:
					x=int(v)
					if x<0:
						raise ValueError
					self.strippaths=x
				except ValueError:
					raise CFVValueError, "invalid strippaths option '%s', must be 'none', 'all', or int >=0"%v
		elif o=="fixpaths":
			self.fixpaths = v and re.compile('['+re.escape(v)+']') or None
		elif o=="renameformat":
			testmap=make_rename_formatmap('1.2')
			testmapwc=make_rename_formatmap('1.2')
			testmapwc['count']=1
			format_test=v%testmapwc
			try:
				format_test=v%testmap
				self.renameformatnocount=1 #if we can get here, it doesn't use the count param
			except KeyError:
				self.renameformatnocount=0
			self.renameformat=v
		elif o=="filename_type":
			typename,match = v.split('=',1)
			if not cftypes.has_key(typename):
				raise CFVValueError, "filename_type: invalid type '%s'"%typename
			self.user_cf_fn_regexs.append((re.compile(match, re.I).search, cftypes[typename]))
		elif o=='announceurl':
			self.setstr(o,v)
		elif o=='piece_size_pow2':
			self.setintr(o,v,1,30)
		else:
			raise CFVNameError, "invalid option '%s'"%o
	def readconfig(self):
		filename=os.path.expanduser(os.path.join("~",".cfvrc"))
		if not os.path.isfile(filename):
			filename=os.path.expanduser(os.path.join("~","_cfvrc"))
		if os.path.isfile(filename):
			file=open(filename,"r")
			l=0
			while 1:
				l=l+1
				s=file.readline()
				if not s:
					break #end of file
				if s[0]=="#":
					continue #ignore lines starting with #
				s=chomp(s)
				if not s:
					continue #ignore blank lines
				x = s.split(' ',1)
				if len(x)!=2:
					raise CFVSyntaxError, "%s:%i: invalid line '%s'"%(filename,l,s)
				else:
					o,v = x
					try:
						self.setx(o,v)
					except CFVException, err:
						raise sys.exc_info()[0], "%s:%i: %s"%(filename,l,err), sys.exc_info()[2] #reuse the traceback of the original exception, but add file and line numbers to the error
	def __init__(self):
		self.readconfig()
			
			
def make_rename_formatmap(l_filename):
	sp=os.path.splitext(l_filename)
	return {'name':sp[0], 'ext':sp[1], 'fullname':l_filename}

version='1.18.3'#.1-pre'+'$Revision: 419 $'[11:-2]

_hassymlinks=hasattr(os,'symlink')

try:
	from binascii import hexlify, unhexlify # only in python >= 2.0
except ImportError:
	def hexlify(d):
		return "%02x"*len(d) % tuple(map(ord, d))
	def unhexlify(s, _splitre=re.compile('..',re.DOTALL)):
		return ''.join(map(chr, map(lambda a: int(a,16), _splitre.findall(s))))

try:
	if os.environ.get('CFV_NOMMAP'): raise ImportError
	# mmap is broken in python 2.4.2 and leaks file descriptors
	if sys.version_info[:3] == (2, 4, 2): raise ImportError
	import mmap
	if hasattr(mmap, 'ACCESS_READ'):
		def dommap(fileno, len):#generic mmap.  python2.2 adds ACCESS_* args that work on both nix and win.
			if len==0: return '' #mmap doesn't like length=0
			return mmap.mmap(fileno, len, access=mmap.ACCESS_READ)
	elif hasattr(mmap, 'PROT_READ'):
		def dommap(fileno, len):#unix mmap.  python default is PROT_READ|PROT_WRITE, but we open readonly.
			if len==0: return '' #mmap doesn't like length=0
			return mmap.mmap(fileno, len, mmap.MAP_SHARED, mmap.PROT_READ)
	else:
		def dommap(fileno, len):#windows mmap.
			if len==0: return ''
			return mmap.mmap(fileno, len)
	nommap=0
except ImportError:
	nommap=1

_MAX_MMAP = 2**32 - 1
_FALLBACK_MMAP = 2**31 - 1
def _getfilechecksum(file, hasher):
	if file=='':
		f=sys.stdin
	else:
		f=open(file,'rb')
	def finish(m,s,f=f,file=file):
		if stdprogress: progress.init(file)
		try:
			while 1:
				x=f.read(65536)
				if not x:
					stats.bytesread=stats.bytesread+s
					return m.digest(),s
				s=s+len(x)
				m.update(x)
				if stdprogress: progress.update(s)
		finally:
			if stdprogress: progress.cleanup()

	if f==sys.stdin or nommap or stdprogress:
		return finish(hasher(),0L)
	else:
		s = os.path.getsize(file)
		try:
			if s > _MAX_MMAP:
				# Work around python 2.[56] problem with md5 of large mmap objects
				raise OverflowError
			m = hasher(dommap(f.fileno(), s))
		except OverflowError:
			mmapsize = min(s, _FALLBACK_MMAP) #mmap size is limited by C's int type, which even on 64 bit arches is often 32 bits, so we can't use sys.maxint either.  If we get the error, just assume 32 bits. 
			m = hasher(dommap(f.fileno(), mmapsize))
			f.seek(mmapsize)
			return finish(m,mmapsize) #unfortunatly, python's mmap module doesn't support the offset parameter, so we just have to do the rest of the file the old fashioned way.
		stats.bytesread = stats.bytesread+s
		return m.digest(),s

try:
	from hashlib import sha1 as sha_new
except ImportError:
	from sha import new as sha_new
try:
	from hashlib import md5 as md5_new
except ImportError:
	from md5 import new as md5_new

def _getfilesha1(file):
	return _getfilechecksum(file, sha_new)
			
try:
	if os.environ.get('CFV_NOFCHKSUM'): raise ImportError
	import fchksum
	try:
		if fchksum.version()<4:raise ImportError
	except:
		stderr.write("old fchksum version installed, using std python modules. please update.\n") #can't use perror yet since config hasn't been done..
		raise ImportError
	def _getfilemd5(file):
		if stdprogress: progress.init(file)
		try:
			c,s=fchksum.fmd5(file, stdprogress and progress.update or None, 0.03)
			stats.bytesread=stats.bytesread+s
		finally:
			if stdprogress: progress.cleanup()
		return c,s
	def _getfilecrc(file):
		if stdprogress: progress.init(file)
		try:
			c,s=fchksum.fcrc32d(file, stdprogress and progress.update or None, 0.03)
			stats.bytesread=stats.bytesread+s
		finally:
			if stdprogress: progress.cleanup()
		return c,s
except ImportError:
	try:
		import zlib
		_crc32=zlib.crc32
	except ImportError:
		import binascii
		_crc32=binascii.crc32
	class CRC32:
		digest_size = 4
		def __init__(self, s=''):
			self.value = _crc32(s)
		def update(self, s):
			self.value = _crc32(s, self.value)
		def digest(self):
			return struct.pack('>I', self.value & 0xFFFFFFFF)

	def _getfilemd5(file):
		return _getfilechecksum(file, md5_new)
			
	def _getfilecrc(file):
		return _getfilechecksum(file, CRC32)

def fcmp(f1, f2):
	import filecmp
	return filecmp.cmp(f1, f2, shallow=0)

try:
	staticmethod #new in python 2.2
except NameError:
	class staticmethod:
		def __init__(self, anycallable):
			self.__call__ = anycallable
					

class PeekFile:
	def __init__(self, fileobj, filename=None):
		self.fileobj = fileobj
		self.name = filename or fileobj.name
	def peek(self, *args):
		self.fileobj.seek(0)
		return self.fileobj.read(*args)
	def peekline(self, *args):
		self.fileobj.seek(0)
		return self.fileobj.readline(*args)
	def peeknextline(self, *args):
		return self.fileobj.readline(*args)
	def _done_peeking(self):
		self.fileobj.seek(0)
		self.peeknextline = None
		self.peekline = None
		self.peek = None
		self.readline = self.fileobj.readline
		self.read = self.fileobj.read
		self.seek = self.fileobj.seek
	def seek(self, *args):
		self._done_peeking()
		return self.seek(*args)
	def readline(self, *args):
		self._done_peeking()
		return self.readline(*args)
	def read(self, *args):
		self._done_peeking()
		return self.read(*args)

def PeekFileNonseekable(fileobj, filename):
	import StringIO
	return PeekFile(StringIO.StringIO(fileobj.read()), filename)

def PeekFileGzip(filename):
	import gzip
	if filename=='-':
		import StringIO
		f = gzip.GzipFile(mode="rb",fileobj=StringIO.StringIO(sys.stdin.read())) #lovely hack since gzip.py requires a bunch of seeking.. bleh.
	else:
		f = gzip.open(filename,"rb")
	try:
		f.tell()
	except (AttributeError, IOError):
		return PeekFileNonseekable(f, filename) #gzip.py prior to python2.2 doesn't support seeking, and prior to 2.0 doesn't support readline(size)
	else:
		return PeekFile(f, filename)

class NoCloseFile:
	def __init__(self, fileobj):
		self.write = fileobj.write
		self.close = fileobj.flush


def doopen_read(filename):
	mode = 'rb' #read all files in binary mode (since .pars are binary, and we don't always know the filetype when opening, just open everything binary.  The text routines should cope with all types of line endings anyway, so this doesn't hurt us.)
	if config.gzip>=2 or (config.gzip>=0 and filename[-3:].lower()=='.gz'):
		return PeekFileGzip(filename)
	else:
		if filename=='-':
			return PeekFileNonseekable(sys.stdin, filename)
		return PeekFile(open(filename, mode))

def doopen_write(filename):
	mode = 'w'
	if config.gzip>=2 or (config.gzip>=0 and filename[-3:].lower()=='.gz'):
		import gzip
		mode=mode+'b' #open gzip files in binary mode
		if filename=='-':
			return gzip.GzipFile(filename=filename, mode=mode, fileobj=sys.stdout)
		return gzip.open(filename, mode)
	else:
		if filename=='-':
			return NoCloseFile(sys.stdout)
		return open(filename, mode)


def auto_filename_match(*names):
	for searchfunc, cftype in config.user_cf_fn_regexs + _cf_fn_matches + _cf_fn_exts + _cf_fn_searches:
		for name in names:
			if searchfunc(name):
				return cftype
	return None

def auto_chksumfile_match(file):
	for p, matchfunc, cftype in _cf_matchers:
		if matchfunc(file):
			return cftype
	return None

def register_cftype(name, cftype):
	cftypes[name] = cftype

	if hasattr(cftype, 'auto_filename_match'):
		if cftype.auto_filename_match[-1]=='$' and cftype.auto_filename_match[0]=='^':
			_cf_fn_matches.append((re.compile(cftype.auto_filename_match, re.I).search, cftype))
		elif cftype.auto_filename_match[-1]=='$':
			_cf_fn_exts.append((re.compile(cftype.auto_filename_match, re.I).search, cftype))
		else:
			_cf_fn_searches.append((re.compile(cftype.auto_filename_match, re.I).search, cftype))

	_cf_matchers.append((getattr(cftype,"auto_chksumfile_order", 0), cftype.auto_chksumfile_match, cftype))
	_cf_matchers.sort()
	_cf_matchers.reverse()


def parse_commentline(comment, commentchars):
	if comment[0] in commentchars:
		return comment[1:].strip()
	return None


class ChksumType:
	def test_chksumfile(self,file,filename):
		if config.showunverified: #we can't expect the checksum file itself to be checksummed
			cache.set_verified(filename)
		try:
			if config.verbose>=0 or config.verbose==-3:
				cf_stats = stats.make_sub_stats()
			if not file:
				file=doopen_read(filename)
			self.do_test_chksumfile(file)
			if config.verbose>=0 or config.verbose==-3:
				cf_stats.sub_stats_end(stats)
				pinfo(perhaps_showpath(file.name)+': ','')
				cf_stats.print_stats()
		except EnvironmentError, a:
			stats.cferror=stats.cferror+1
			perror('%s : %s (CF)'%(perhaps_showpath(filename),enverrstr(a)))

	def do_test_chksumfile_print_testingline(self, file, comment=None):
		if comment:
			comment = ', ' + comment
			if len(comment)>102: #limit the length in case its a really long one.
				comment = comment[:99]+'...'
		else:
			comment = ''
		pverbose('testing from %s (%s%s)'%(file.name, self.__class__.__name__.lower(), comment))

	def do_test_chksumfile(self, file):
		self.do_test_chksumfile_print_testingline(file)
		line=1
		while 1:
			l=file.readline()
			if not l:
				break
			if self.do_test_chksumline(l):
				stats.cferror=stats.cferror+1
				perror('%s : unrecognized line %i (CF)'%(perhaps_showpath(file.name),line))
			line=line+1

	def search_file(self, filename, filecrc, filesize, errfunc, errargs):
		if (not config.search or
			(filesize<0 and (not filecrc or not config.docrcchecks))): #don't bother searching if we don't have anything to compare against
			errfunc(*errargs)
			return -2
		alreadyok=None
		fpath,filenametail = os.path.split(filename)
		try:
			if fpath:
				if config.ignorecase:
					fpath = nocase_findfile(fpath, FINDDIR)
					filename = os.path.join(fpath,filenametail) #fix the dir the orig filename is in, so that the do_f_found can rename it correctly
			else:
				fpath = os.curdir
			ftails = os.listdir(fpath)
		except EnvironmentError:
			ftails = []
		for ftail in ftails:
			fn = os.path.join(fpath,ftail)
			try:
				if filesize>=0:
					fs=os.path.getsize(fn)
					if fs!=filesize:
						#continue
						raise EnvironmentError #can't continue in try: until python 2.1+
				if config.docrcchecks and filecrc!=None:
					c = self.do_test_file(fn, filecrc)
					if c:
						#continue
						raise EnvironmentError #can't continue in try: until python 2.1+
					filecrct=hexlify(filecrc)
				else:
					if not os.path.isfile(fn):
						#continue
						raise EnvironmentError #can't continue in try: until python 2.1+
					filecrct='exists'
			except EnvironmentError:
				continue
			if cache.has_flag(fn, '_ok'):
				alreadyok=(fn,filecrct)
				continue
			errfunc(foundok=1,*errargs)
			do_f_found(filename, fn, filesize, filecrct)
			return 0
		if alreadyok:
			errfunc(foundok=1,*errargs)
			do_f_found(filename,alreadyok[0],filesize,alreadyok[1],alreadyok=1)
			return 0
		errfunc(*errargs)
		return -1

	def test_file(self,filename,filecrc,filesize=-1):
		filename = mangle_filename(filename)
		if cache.testfiles:
			if not cache.should_test(filename):
				return
		stats.num=stats.num+1
		l_filename = filename
		try:
			l_filename = find_local_filename(filename)
			if filesize>=0:
				fs=os.path.getsize(l_filename)
				if fs!=filesize:
					self.search_file(filename, filecrc, filesize,
						do_f_badsize, (l_filename, filesize, fs))
					return -2
			if config.docrcchecks and filecrc!=None:
				c=self.do_test_file(l_filename,filecrc)
				filecrct = hexlify(filecrc)
				if c:
					self.search_file(filename, filecrc, filesize,
						do_f_badcrc, (l_filename, 'crc does not match (%s!=%s)'%(filecrct,hexlify(c))))
					return -2
			else:
				if not os.path.exists(l_filename):
					raise EnvironmentError, (errno.ENOENT,"missing")
				if not os.path.isfile(l_filename):
					raise EnvironmentError, (errno.ENOENT,"not a file")
				filecrct="exists" #since we didn't actually test the crc, make verbose mode merely say it exists
		except (EnvironmentError, UnicodeError), a: #UnicodeError can occur if python can't map the filename to the filesystem's encoding
			self.search_file(filename, filecrc, filesize,
				do_f_enverror, (l_filename, a))
			return -1
		do_f_ok(l_filename, filesize, filecrct)
	
	def make_chksumfile(self, filename):
		return doopen_write(filename)

	def make_chksumfile_finish(self, file):
		pass


def do_f_enverror(l_filename, ex, foundok=0):
	if ex[0]==errno.ENOENT:
		if foundok:
			return
		stats.notfound=stats.notfound+1
		if config.list&LISTNOTFOUND:
			plistf(l_filename)
	else:
		#if not foundok:
		stats.ferror=stats.ferror+1
	perror('%s : %s'%(perhaps_showpath(l_filename),enverrstr(ex)))

def do_f_badsize(l_filename, expected, actual, foundok=0):
	if not foundok:
		stats.badsize=stats.badsize+1
	do_f_verifyerror(l_filename, 'file size does not match (%s!=%i)'%(expected,actual), foundok=foundok)

def do_f_badcrc(l_filename, msg, foundok=0):
	if not foundok:
		stats.badcrc=stats.badcrc+1
	do_f_verifyerror(l_filename, msg, foundok=foundok)

def do_f_verifyerror(l_filename, a, foundok=0):
	reninfo=''
	if not foundok:
		if config.list&LISTBAD:
			plistf(l_filename)
	if config.rename:
		formatmap=make_rename_formatmap(l_filename)
		for count in xrange(0,sys.maxint):
			formatmap['count']=count
			newfilename=config.renameformat%formatmap
			if config.renameformatnocount and count>0:
				newfilename='%s-%i'%(newfilename,count)
			if l_filename==newfilename:
				continue #if the filenames are the same they would cmp the same and be deleted. (ex. when renameformat="%(fullname)s")
			if os.path.exists(newfilename):
				if fcmp(l_filename, newfilename):
					os.unlink(l_filename)
					reninfo=' (dupe of %s removed)'%newfilename
					break
			else:
				rename(l_filename, newfilename)
				reninfo=' (renamed to %s)'%newfilename
				break
	perror('%s : %s%s'%(perhaps_showpath(l_filename),a,reninfo))

def do_f_found(filename, found_fn, filesize, filecrct, alreadyok=0):
	l_filename = found_fn
	if config.rename:
		try:
			if os.path.exists(filename):
				verb=None,"fixing name"
				prep="of"
				raise EnvironmentError, "File exists"
			if alreadyok:
				verb="linked","linking"
				prep="to"
				try:
					os.link(found_fn, filename)
				except (EnvironmentError, AttributeError), e:
					if isinstance(e, EnvironmentError) and e[0] not in (errno.EXDEV, errno.EPERM):
						raise
					verb="copied","copying"
					prep="from"
					import shutil
					shutil.copyfile(found_fn, filename)
			else:
				verb="renamed","renaming"
				prep="from"
				rename(found_fn, filename)
		except EnvironmentError, e:
			action='but error %r occured %s %s'%(enverrstr(e),verb[1],prep)
			stats.ferror=stats.ferror+1
		else:
			action='%s %s'%(verb[0],prep)
			l_filename = filename
	else:
		action="found"
	if config.showunverified:
		cache.set_verified(l_filename)
	stats.misnamed=stats.misnamed+1
	do_f_ok(filename, filesize, filecrct, msg="OK(%s %s)"%(action,found_fn), l_filename=l_filename)

def do_f_ok(filename, filesize, filecrct, msg="OK", l_filename=None):
	cache.set_flag(l_filename or filename, '_ok')
	stats.ok=stats.ok+1
	if config.list&LISTOK:
		plistf(filename)
	if filesize>=0:
		pverbose('%s : %s (%i,%s)'%(perhaps_showpath(filename),msg,filesize,filecrct))
	else:
		pverbose('%s : %s (%s)'%(perhaps_showpath(filename),msg,filecrct))

	
#---------- sha1sum ----------

class SHA1_MixIn:
	def do_test_file(self, filename, filecrc):
		c=getfilesha1(filename)[0]
		if c!=filecrc:
			return c

# Base class for md5sum/sha1sum style checksum file formats.
class FooSum_Base(ChksumType):
	def do_test_chksumfile_print_testingline(self, file):
		ChksumType.do_test_chksumfile_print_testingline(self, file, parse_commentline(file.peekline(128), ';#'))
	
	def do_test_chksumline(self, l):
		if l[0] in ';#': return
		x=self._foosum_rem.match(l)
		if not x: return -1
		if x.group(2)==' ':
			if stats.textmode==0:
				perror('warning: file(s) tested in textmode')
			stats.textmode = stats.textmode + 1
		self.test_file(x.group(3),unhexlify(x.group(1)))


class SHA1(FooSum_Base, SHA1_MixIn):
	description = 'GNU sha1sum'
	descinfo = 'SHA1,name'
	
	def auto_chksumfile_match(file, _autorem=re.compile(r'[0-9a-fA-F]{40} [ *].')):
		l = file.peekline(4096)
		while l:
			if l[0] not in ';#':
				return _autorem.match(l) is not None
			l = file.peeknextline(4096)
	auto_chksumfile_match=staticmethod(auto_chksumfile_match)

	auto_filename_match = 'sha1'

	_foosum_rem=re.compile(r'([0-9a-fA-F]{40}) ([ *])([^\r\n]+)[\r\n]*$')
	
	def make_std_filename(filename):
		return filename+'.sha1'
	make_std_filename = staticmethod(make_std_filename)
	
	def make_addfile(self, filename):
		crc=hexlify(getfilesha1(filename)[0])
		return (crc, -1), '%s *%s\n'%(crc,filename)

register_cftype('sha1', SHA1)


#---------- md5 ----------

class MD5_MixIn:
	def do_test_file(self, filename, filecrc):
		c=getfilemd5(filename)[0]
		if c!=filecrc:
			return c

class MD5(FooSum_Base, MD5_MixIn):
	description = 'GNU md5sum'
	descinfo = 'MD5,name'

	def auto_chksumfile_match(file, _autorem=re.compile(r'[0-9a-fA-F]{32} [ *].')):
		l = file.peekline(4096)
		while l:
			if l[0] not in ';#':
				return _autorem.match(l) is not None
			l = file.peeknextline(4096)
	auto_chksumfile_match=staticmethod(auto_chksumfile_match)

	auto_filename_match = 'md5'

	_foosum_rem=re.compile(r'([0-9a-fA-F]{32}) ([ *])([^\r\n]+)[\r\n]*$')
	
	def make_std_filename(filename):
		return filename+'.md5'
	make_std_filename = staticmethod(make_std_filename)
	
	def make_addfile(self, filename):
		crc=hexlify(getfilemd5(filename)[0])
		return (crc, -1), '%s *%s\n'%(crc,filename)

register_cftype('md5', MD5)


#---------- bsdmd5 ----------

class BSDMD5(ChksumType, MD5_MixIn):
	description = 'BSD md5'
	descinfo = 'name,MD5'

	def auto_chksumfile_match(file, _autorem=re.compile(r'MD5 \(.+\) = [0-9a-fA-F]{32}'+'[\r\n]*$')):
		return _autorem.match(file.peekline(4096)) is not None
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	auto_filename_match = '^md5$'
	
	_bsdmd5rem=re.compile(r'MD5 \((.+)\) = ([0-9a-fA-F]{32})[\r\n]*$')
	def do_test_chksumline(self, l):
		x=self._bsdmd5rem.match(l)
		if not x: return -1
		self.test_file(x.group(1),unhexlify(x.group(2)))
	
	def make_std_filename(filename):
		return filename+'.md5'
	make_std_filename = staticmethod(make_std_filename)
	
	def make_addfile(self, filename):
		crc=hexlify(getfilemd5(filename)[0])
		return (crc, -1), 'MD5 (%s) = %s\n'%(filename, crc)

register_cftype('bsdmd5', BSDMD5)


#---------- par ----------

def ver2str(v):
	vers=[]
	while v or len(vers)<3:
		vers.insert(0, str(v&0xFF))
		v = v >> 8
	return '.'.join(vers)

try: #support for 64bit ints in struct module was only added in python 2.2
	struct.calcsize('< Q')
except struct.error:
	class _Struct:
		def calcsize(self, fmt, _calcsize=struct.calcsize):
			return _calcsize(fmt.replace('Q','II'))
		def unpack(self, fmt, data, _unpack=struct.unpack):
			unpacked = _unpack(fmt.replace('Q','II'), data)
			upos = 0
			ret = []
			for f in fmt.split(' '):
				if f=='Q':
					ret.append(long(unpacked[upos])+long(unpacked[upos+1])*2**32L)
					upos = upos + 2
				elif f=='<':pass
				else:
					ret.append(unpacked[upos])
					upos = upos + 1
			return tuple(ret)
		pack = struct.pack
	struct = _Struct()

class PAR(ChksumType, MD5_MixIn):
	description = 'Parchive v1 (test-only)'
	descinfo = 'name,size,MD5'

	def auto_chksumfile_match(file):
		return file.peek(8)=='PAR\0\0\0\0\0'
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	def do_test_chksumfile(self, file):
		def prog2str(v):
			return {0x01: 'Mirror', 0x02: 'PAR', 0x03:'SmartPar', 0xFF:'FSRaid'}.get(v, 'unknown(%x)'%v)
		par_header_fmt = '< 8s I I 16s 16s Q Q Q Q Q Q'
		par_entry_fmt = '< Q Q Q 16s 16s'
		par_entry_fmtsize = struct.calcsize(par_entry_fmt)

		d = file.read(struct.calcsize(par_header_fmt))
		magic, version, client, control_hash, set_hash, vol_number, num_files, file_list_ofs, file_list_size, data_ofs, data_size = struct.unpack(par_header_fmt, d)
		if config.docrcchecks:
			control_md5 = md5_new()
			control_md5.update(d[0x20:])
			stats.bytesread=stats.bytesread+len(d)
		if version not in (0x00000900, 0x00010000): #ver 0.9 and 1.0 are the same, as far as we care.  Future versions (if any) may very likey have incompatible changes, so don't accept them either.
			raise EnvironmentError, (errno.EINVAL,"can't handle PAR version %s"%ver2str(version))

		pverbose('testing from %s (par v%s, created by %s v%s)'%(file.name,ver2str(version), prog2str(client>>24), ver2str(client&0xFFFFFF)))

		for i in range(0, num_files):
			d = file.read(par_entry_fmtsize)
			size, status, file_size, md5, md5_16k = struct.unpack(par_entry_fmt, d)
			if config.docrcchecks:
				control_md5.update(d)
			d = file.read(size - par_entry_fmtsize)
			filename = unicode(d, 'utf-16-le')
			if config.docrcchecks:
				control_md5.update(d)
				stats.bytesread=stats.bytesread+size
			self.test_file(filename, md5, file_size)

		if config.docrcchecks:
			while 1:
				d=file.read(65536)
				if not d:
					if control_md5.digest() != control_hash:
						raise EnvironmentError, (errno.EINVAL,"corrupt par file - bad control hash")
					break
				stats.bytesread=stats.bytesread+len(d)
				control_md5.update(d)

	#we don't support PAR in create mode, but add these methods so that we can get error messages that are probaby more user friendly.
	auto_filename_match = 'par$'

	def make_std_filename(filename):
		return filename+'.par'
	make_std_filename = staticmethod(make_std_filename)

register_cftype('par', PAR)


class PAR2(ChksumType, MD5_MixIn):
	description = 'Parchive v2 (test-only)'
	descinfo = 'name,size,MD5'

	def auto_chksumfile_match(file):
		return file.peek(8)=='PAR2\0PKT'
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	def do_test_chksumfile(self, file):
		pkt_header_fmt = '< 8s Q 16s 16s 16s'
		pkt_header_size = struct.calcsize(pkt_header_fmt)
		file_pkt_fmt = '< 16s 16s 16s Q'
		file_pkt_size = struct.calcsize(file_pkt_fmt)
		main_pkt_fmt = '< Q I'
		main_pkt_size = struct.calcsize(main_pkt_fmt)

		def get_creator(file, pkt_header_fmt=pkt_header_fmt, pkt_header_size=pkt_header_size):#nested scopes not supported until python 2.1
			if not config.verbose>0:
				return None #avoid doing the work if we aren't going to use it anyway.
			while 1:
				d = file.read(pkt_header_size)
				if not d:
					return None
				magic, pkt_len, pkt_md5, set_id, pkt_type = struct.unpack(pkt_header_fmt, d)
				if pkt_type == 'PAR 2.0\0Creator\0':
					return chompnulls(file.read(pkt_len - pkt_header_size))
				else:
					file.seek(pkt_len - pkt_header_size, 1)
				
		self.do_test_chksumfile_print_testingline(file, get_creator(file))
		file.seek(0) # reset file position after looking for creator packet
		
		seen_file_ids = {}
		expected_file_ids = None

		while 1:
			d = file.read(pkt_header_size)
			if not d:
				break

			magic, pkt_len, pkt_md5, set_id, pkt_type = struct.unpack(pkt_header_fmt, d)

			if config.docrcchecks:
				control_md5 = md5_new()
				control_md5.update(d[0x20:])
				stats.bytesread=stats.bytesread+len(d)
				d = file.read(pkt_len - pkt_header_size)
				control_md5.update(d)
				stats.bytesread=stats.bytesread+len(d)
				if control_md5.digest() != pkt_md5:
					raise EnvironmentError, (errno.EINVAL,"corrupt par2 file - bad packet hash")

			if pkt_type == 'PAR 2.0\0FileDesc':
				if not config.docrcchecks:
					d = file.read(pkt_len - pkt_header_size)
				file_id, file_md5, file_md5_16k, file_size = struct.unpack(file_pkt_fmt, d[:file_pkt_size])
				if seen_file_ids.get(file_id) is None:
					seen_file_ids[file_id] = 1
					filename = chompnulls(d[file_pkt_size:])
					self.test_file(filename, file_md5, file_size)
			elif pkt_type == "PAR 2.0\0Main\0\0\0\0":
				if not config.docrcchecks:
					d = file.read(pkt_len - pkt_header_size)
				if expected_file_ids is None:
					expected_file_ids = []
					slice_size, num_files = struct.unpack(main_pkt_fmt, d[:main_pkt_size])
					num_nonrecovery = (len(d)-main_pkt_size)/16 - num_files
					for i in range(main_pkt_size,main_pkt_size+(num_files+num_nonrecovery)*16,16):
						expected_file_ids.append(d[i:i+16])
			else:
				if not config.docrcchecks:
					file.seek(pkt_len - pkt_header_size, 1)

		if expected_file_ids is None:
			raise EnvironmentError, (errno.EINVAL,"corrupt or unsupported par2 file - no main packet found")
		for id in expected_file_ids:
			if not seen_file_ids.has_key(id):
				raise EnvironmentError, (errno.EINVAL,"corrupt or unsupported par2 file - expected file description packet not found")

	#we don't support PAR2 in create mode, but add these methods so that we can get error messages that are probaby more user friendly.
	auto_filename_match = 'par2$'

	def make_std_filename(filename):
		return filename+'.par2'
	make_std_filename = staticmethod(make_std_filename)

register_cftype('par2', PAR2)


#---------- .torrent ----------
_btimporterror = None
try:
	from BTL import bencode, btformats
except ImportError, e1:
	try:
		from BitTorrent import bencode, btformats
	except ImportError, e2:
		try:
			from BitTornado import bencode; from BitTornado.BT1 import btformats
		except ImportError, e3:
			_btimporterror = '%s and %s and %s'%(e1,e2,e3)

class Torrent(ChksumType):
	description = 'BitTorrent metainfo'
	descinfo = 'name,size,SHA1(piecewise)'

	def auto_chksumfile_match(file):
		return file.peek(1)=='d' and file.peek(4096).find('8:announce')>=0
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	def do_test_chksumfile(self, file):
		if _btimporterror:
			raise EnvironmentError, _btimporterror
		try:
			metainfo = bencode.bdecode(file.read())
			btformats.check_message(metainfo)
		except ValueError, e:
			raise EnvironmentError, str(e) or 'invalid or corrupt torrent'

		comments = []
		if metainfo.has_key('creation date'):
			try:
				comments.append('created '+time.ctime(metainfo['creation date']))
			except TypeError:
				comments.append('created '+repr(metainfo['creation date']))
		if metainfo.has_key('comment'):
			comments.append(metainfo['comment'])
		self.do_test_chksumfile_print_testingline(file, ', '.join(comments))

		encoding = metainfo.get('encoding')

		def init_file(filenameparts, ftotpos, filesize, encoding=encoding, file=file, self=self):
			if encoding:
				try:
					filenameparts = map(lambda p,encoding=encoding: unicode(p,encoding), filenameparts)
				except LookupError, e: #lookup error is raised when specified encoding isn't found.
					raise EnvironmentError, str(e)
				except UnicodeError, e:
					stats.cferror=stats.cferror+1
					perror('%s : error decoding filename %r : %s (CF)'%(perhaps_showpath(file.name),filenameparts,str(e)))
					#here we fall through and just let it use the undecoded filename.. is that really ok?
					
			filename = os.path.join(*filenameparts)
			if not config.docrcchecks: #if we aren't testing checksums, just use the standard test_file function, so that -s and such will work.
				self.test_file(filename, None, filesize)
				return
			filename = mangle_filename(filename)
			done = cache.testfiles and not cache.should_test(filename) #if we don't want to test this file, just pretending its done already has the desired effect.
			if not done:
				stats.num=stats.num+1
			try:
				l_filename = find_local_filename(filename)
				if not os.path.exists(l_filename):
					raise EnvironmentError, (errno.ENOENT,"missing")
				if not os.path.isfile(l_filename):
					raise EnvironmentError, (errno.ENOENT,"not a file")
				fs=os.path.getsize(l_filename)
				if fs!=filesize:
					if not done:
						do_f_badsize(l_filename, filesize, fs)
						done=1
			except (EnvironmentError, UnicodeError), e: #UnicodeError can occur if python can't map the filename to the filesystem's encoding
				if not done:
					do_f_enverror(filename, e)
					done=1
				l_filename = None
			return Data(totpos=ftotpos, size=filesize, filename=filename, l_filename=l_filename, done=done)

		info = metainfo['info']
		piecelen = info['piece length']
		hashes = re.compile('.'*20, re.DOTALL).findall(info['pieces'])
		if info.has_key('length'):
			total_len = info['length']
			files = [init_file([info['name']], 0, total_len)]
		else:
			dirpart = [info['name']]
			if config.strippaths==0:
				dirname = info['name']
				if encoding:
					try:
						dirname = unicode(dirname, encoding)
					except (LookupError, UnicodeError): #lookup error is raised when specified encoding isn't found.
						pass
				dirname = strippath(dirname, 0)
				if config.ignorecase:
					try:
						nocase_findfile(dirname,FINDDIR)
					except EnvironmentError:
						dirpart = []
				else:
					if not os.path.isdir(dirname):
						dirpart = []
				#if not dirpart:
				#	pinfo("enabling .torrent path strip hack")
			files = []
			total_len = 0
			for finfo in info['files']:
				flength = finfo['length']
				files.append(init_file(dirpart+finfo['path'], total_len, flength))
				total_len = total_len + flength
		
		if not config.docrcchecks:
			return

		curfh = Data(fh=None, f=None)
		def readfpiece(f, pos, size, curfh=curfh):
			if f.l_filename is None:
				return None
			try:
				if not curfh.fh or curfh.f is not f:
					if curfh.fh:
						curfh.fh.close()
					curfh.fh = open(f.l_filename,'rb')
					curfh.f = f
				curfh.fh.seek(pos)
				d = curfh.fh.read(size)
			except EnvironmentError, e:
				if not f.done:
					if stdprogress: progress.cleanup()
					do_f_enverror(f.l_filename, e)
					f.done=1
				return None
			stats.bytesread=stats.bytesread+len(d)
			if len(d)<size:
				return None
			return d

		curf = 0
		curpos = 0
		for piece in range(0, len(hashes)):
			if piece < len(hashes) - 1:
				curend = curpos + piecelen
			else:
				curend = total_len
			piecefiles = []
			wanttest = 0
			while curpos < curend:
				while curpos >= files[curf].totpos+files[curf].size:
					curf = curf + 1
				f=files[curf]
				fcurpos = curpos-f.totpos
				assert fcurpos >= 0
				fpiecelen = min(curend-curpos, f.size-fcurpos)
				piecefiles.append((f,fcurpos,fpiecelen))
				curpos = curpos + fpiecelen
				if not f.done:
					wanttest = 1
			if wanttest:
				sh = sha_new()
				for f,fcurpos,fpiecelen in piecefiles:
					if stdprogress and f.l_filename and f.l_filename!=progress.filename:
						progress.cleanup()
						progress.init(f.l_filename,f.size,fcurpos)
					d = readfpiece(f, fcurpos, fpiecelen)
					if d is None:
						break
					sh.update(d)
					if stdprogress and f.l_filename: progress.update(fcurpos+fpiecelen)
				if d is None:
					if curfh.fh:
						#close the file in case do_f_badcrc wants to try to rename it
						curfh.fh.close(); curfh.fh=None
					for f,fcurpos,fpiecelen in piecefiles:
						if not f.done:
							if stdprogress: progress.cleanup()
							do_f_badcrc(f.l_filename, 'piece %i missing data'%(piece))
							f.done=1
				elif sh.digest()!=hashes[piece]:
					if curfh.fh:
						#close the file in case do_f_badcrc wants to try to rename it
						curfh.fh.close(); curfh.fh=None
					for f,fcurpos,fpiecelen in piecefiles:
						if not f.done:
							if stdprogress: progress.cleanup()
							do_f_badcrc(f.l_filename, 'piece %i (at %i..%i) crc does not match (%s!=%s)'%(piece,fcurpos,fcurpos+fpiecelen,hexlify(hashes[piece]), sh.hexdigest()))
							f.done=1
				else:
					for f,fcurpos,fpiecelen in piecefiles:
						if not f.done:
							if fcurpos+fpiecelen==f.size:
								if stdprogress: progress.cleanup()
								do_f_ok(f.l_filename,f.size,'all pieces ok')
								f.done=1

		if stdprogress: progress.cleanup()

		if curfh.fh:
			curfh.fh.close()
		for f in files:
			if not f.done:
				do_f_ok(f.l_filename,f.size,'all pieces ok')
			
	auto_filename_match = 'torrent$'

	def make_std_filename(filename):
		return filename+'.torrent'
	make_std_filename = staticmethod(make_std_filename)

	def make_chksumfile(self, filename):
		if _btimporterror:
			raise EnvironmentError, _btimporterror
		if config.announceurl==None:
			raise EnvironmentError, 'announce url required'
		file = ChksumType.make_chksumfile(self, filename)
		self.sh = sha_new()
		self.files = []
		self.pieces = []
		self.piece_done = 0
		self.piece_length = 2**config.piece_size_pow2
		return file
	
	def make_addfile(self, filename):
		firstpiece = len(self.pieces)
		f = open(filename, 'rb')
		if stdprogress: progress.init(filename)
		fs = 0
		while 1:
			piece_left = self.piece_length - self.piece_done
			d = f.read(piece_left)
			if not d:
				break
			self.sh.update(d)
			s = len(d)
			stats.bytesread=stats.bytesread+s
			fs = fs + s
			if stdprogress: progress.update(fs)
			self.piece_done = self.piece_done + s
			if self.piece_done == self.piece_length:
				self.pieces.append(self.sh.digest())
				self.sh = sha_new()
				self.piece_done = 0
		f.close()
		if stdprogress: progress.cleanup()
		self.files.append({'length':fs, 'path':path_split(filename)})
		return ('pieces %i..%i'%(firstpiece,len(self.pieces)),fs), ''
	
	def make_chksumfile_finish(self, file):
		if self.piece_done > 0:
			self.pieces.append(self.sh.digest())

		info = {'pieces':''.join(self.pieces), 'piece length':self.piece_length}
		if config.private_torrent:
			info['private'] = 1
		if len(self.files)==1 and len(self.files[0]['path'])==1:
			info['name'],info['length'] = self.files[0]['path'][0],self.files[0]['length']
		else:
			commonroot = self.files[0]['path'][0]
			for fileinfo in self.files[1:]:
				if commonroot!=fileinfo['path'][0]:
					commonroot = None
					break
			if commonroot:
				for fileinfo in self.files:
					del fileinfo['path'][0]
			else:
				commonroot = os.path.split(os.getcwd())[1]
			info['files'] = self.files
			info['name'] = commonroot

		btformats.check_info(info)
		data = {'info': info, 'announce': config.announceurl.strip(), 'creation date': long(time.time())}
		#if comment:
		#	data['comment'] = comment
		file.write(bencode.bencode(data))
		

register_cftype('torrent', Torrent)


#---------- sfv ----------

class CRC_MixIn:
	def do_test_file(self, filename, filecrc):
		c=getfilecrc(filename)[0]
		if c!=filecrc:
			return c
		
class SFV_Base(ChksumType):
	description = 'Simple File Verify'

	def do_test_chksumfile_print_testingline(self, file):
		#override the default testing line to show first SFV comment line, if any
		ChksumType.do_test_chksumfile_print_testingline(self, file, parse_commentline(file.peekline(128), ';'))

	def do_test_chksumline(self, l):
		if l[0]==';': return
		x=self._sfvrem.match(l)
		if not x: return -1
		self.test_file(x.group(1),unhexlify(x.group(2)))
	
	def make_chksumfile(self, filename):
		file = ChksumType.make_chksumfile(self, filename)
		file.write('; Generated by cfv v%s on %s\n;\n'%(version,time.strftime('%Y-%m-%d at %H:%M.%S',time.gmtime(time.time()))))
		return file


class SFV(SFV_Base, CRC_MixIn):
	descinfo = 'name,CRC32'

	def auto_chksumfile_match(file, _autorem=re.compile('.+ [0-9a-fA-F]{8}[\n\r]*$')):
		l = file.peekline(4096)
		while l:
			if l[0]!=';':
				return _autorem.match(l) is not None
			l = file.peeknextline(4096)
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	auto_filename_match = 'sfv$'
	
	_sfvrem=re.compile(r'(.+) ([0-9a-fA-F]{8})[\r\n]*$')
	
	def make_std_filename(filename):
		return filename+'.sfv'
	make_std_filename = staticmethod(make_std_filename)
		
	def make_addfile(self, filename):
		crc=hexlify(getfilecrc(filename)[0])
		return (crc, -1), '%s %s\n'%(filename,crc)

register_cftype('sfv', SFV)


class SFVMD5(SFV_Base, MD5_MixIn):
	descinfo = 'name,MD5'

	def auto_chksumfile_match(file, _autorem=re.compile('.+ [0-9a-fA-F]{32}[\n\r]*$')):
		l = file.peekline(4096)
		while l:
			if l[0]!=';':
				return _autorem.match(l) is not None
			l = file.peeknextline(4096)
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)
	auto_chksumfile_order = -1 # do -1 prio since we can mistakenly match a bsdmd5 file

	#auto_filename_match = 'md5$' #hm. People are probably used to .md5 making a md5sum format file, so we can't just do this.
	
	_sfvrem=re.compile(r'(.+) ([0-9a-fA-F]{32})[\r\n]*$')
	
	def make_std_filename(filename):
		return filename+'.md5'
	make_std_filename = staticmethod(make_std_filename)
		
	def make_addfile(self, filename):
		crc=hexlify(getfilemd5(filename)[0])
		return (crc, -1), '%s %s\n'%(filename,crc)
	
register_cftype('sfvmd5', SFVMD5)


#---------- csv ----------

_csvstrautore=r'(?:".*"|[^,]*),'
_csvstrre=r'(?:"(.*)"|([^,]*)),'
_csvfnautore=_csvstrautore.replace('*','+')
_csvfnre=_csvstrre.replace('*','+')
def csvquote(s):
	if ',' in s or '"' in s: return '"%s"'%(s.replace('"','""'))
	return s
def csvunquote(g1,g2):
	if g1 is not None:
		return g1.replace('""','"')
	return g2

class CSV(ChksumType, CRC_MixIn):
	description = 'Comma Separated Values'
	descinfo = 'name,size,CRC32'

	def auto_chksumfile_match(file, _autorem=re.compile(_csvfnautore+'[0-9]+,[0-9a-fA-F]{8},[\n\r]*$')):
		return _autorem.match(file.peekline(4096)) is not None
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	auto_filename_match = 'csv$'
	
	_csvrem=re.compile(_csvfnre+r'([0-9]+),([0-9a-fA-F]{8}),')
	def do_test_chksumline(self, l):
		x=self._csvrem.match(l)
		if not x: return -1
		self.test_file(csvunquote(x.group(1),x.group(2)), unhexlify(x.group(4)), int(x.group(3)))
	
	def make_std_filename(filename):
		return filename+'.csv'
	make_std_filename = staticmethod(make_std_filename)
	
	def make_addfile(self, filename):
		c,s=getfilecrc(filename)
		c = hexlify(c)
		return (c,s), '%s,%i,%s,\n'%(csvquote(filename),s,c)

register_cftype('csv', CSV)


#---------- csv with 4 fields ----------

class CSV4(ChksumType, CRC_MixIn):
	description = 'Comma Separated Values'
	descinfo = 'name,size,CRC32,path'
	
	def auto_chksumfile_match(file, _autorem=re.compile(r'%s[0-9]+,[0-9a-fA-F]{8},%s[\n\r]*$'%(_csvfnautore,_csvstrautore))):
		return _autorem.match(file.peekline(4096)) is not None
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	_csv4rem=re.compile(r'%s([0-9]+),([0-9a-fA-F]{8}),%s'%(_csvfnre,_csvstrre))
	def do_test_chksumline(self, l):
		x=self._csv4rem.match(l)
		if not x: return -1
		name = csvunquote(x.group(1),x.group(2))
		path = csvunquote(x.group(5),x.group(6))
		self.test_file(os.path.join(fixpath(path),name),unhexlify(x.group(4)),int(x.group(3))) #we need to fixpath before path.join since os.path.join looks for path.sep
	
	def make_std_filename(filename):
		return filename+'.csv'
	make_std_filename = staticmethod(make_std_filename)
	
	def make_addfile(self, filename):
		c,s=getfilecrc(filename)
		c = hexlify(c)
		p=os.path.split(filename)
		return (c,s), '%s,%i,%s,%s,\n'%(csvquote(p[1]),s,c,csvquote(p[0]))

register_cftype('csv4', CSV4)


#---------- csv with only 2 fields ----------

class CSV2(ChksumType):
	description = 'Comma Separated Values'
	descinfo = 'name,size'

	def auto_chksumfile_match(file, _autorem=re.compile(_csvfnautore+'[0-9]+,[\n\r]*$')):
		return _autorem.match(file.peekline(4096)) is not None
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	_csv2rem=re.compile(_csvfnre+r'([0-9]+),')
	def do_test_chksumline(self, l):
		x=self._csv2rem.match(l)
		if not x: return -1
		self.test_file(csvunquote(x.group(1),x.group(2)),None,int(x.group(3)))
	
	def make_std_filename(filename):
		return filename+'.csv'
	make_std_filename = staticmethod(make_std_filename)
	
	def make_addfile(self, filename):
		if filename=='':
			s=getfilecrc(filename)[1]#no way to get size of stdin other than to read it
		else:
			s=os.path.getsize(filename)
		return (None, s), '%s,%i,\n'%(csvquote(filename),s)

register_cftype('csv2', CSV2)


#---------- jpegsheriff .crc ----------

def getimagedimensions(filename):
	if filename == '':
		return '0','0'
	try:
		import Image
		im1=Image.open(filename)
		return map(str, im1.size)
	except (ImportError, IOError):
		return '0','0'

def commaize(n):
	n = str(n)
	s = n[-3:]
	n = n[:-3]
	while n:
		s = n[-3:]+','+s
		n = n[:-3]
	return s

class JPEGSheriff_CRC(ChksumType, CRC_MixIn):
	description = 'JPEGSheriff'
	descinfo = 'name,size,dimensions,CRC32'

	def auto_chksumfile_match(file, _autorem=re.compile(r'^Filename\s+(Filesize\s+)?.*?CRC-?32.*^-+(\s+-+){1,4}\s*$', re.DOTALL|re.IGNORECASE|re.MULTILINE)):
		return _autorem.search(file.peek(1024)) is not None
	auto_chksumfile_match = staticmethod(auto_chksumfile_match)

	auto_filename_match = 'crc$'
	
	def do_test_chksumfile_print_testingline(self, file):
		x=re.search(r'^Filename\s+(Filesize\s+)?(.+?)??CRC-?32.*^-+(\s+-+){1,4}\s*$', file.peek(1024), re.DOTALL|re.IGNORECASE|re.MULTILINE)
		self.has_size = x.group(1) is not None
		self.has_dimensions = x.group(2) is not None
		
		crcre = r'(?P<name>.*\S)\s+'
		if self.has_size:
			crcre = crcre + r'(?P<size>[0-9.,]+)\s+'
		if self.has_dimensions:
			crcre = crcre + r'\S+\s*x\s*\S+\s+'
		crcre = crcre + r'(?P<crc>[0-9a-fA-F]{8})\b'
		self._crcrem = re.compile(crcre, re.I)
		
		self.in_comments = 1
		
		#override the default testing line to show first comment line, if any
		comment = ', ' + file.peekline().strip()
		if len(comment)>102: #but limit the length in case its a really long one.
			comment = comment[:99]+'...'
		pverbose('testing from %s (crc%s)'%(file.name, comment))

	_commentboundary=re.compile(r'^-+(\s+-+){1,4}\s*$')
	_nstrip=re.compile(r'[.,]')
	def do_test_chksumline(self, l):
		if self._commentboundary.match(l):
			self.in_comments = not self.in_comments
			return
		x=self._crcrem.match(l)
		if not x:
			if self.in_comments:
				return
			return -1
		if self.has_size:
			size = int(self._nstrip.sub('',x.group('size')))
		else:
			size = -1
		self.test_file(x.group('name'), unhexlify(x.group('crc')), size)
	
	def make_std_filename(filename):
		return filename+'.crc'
	make_std_filename = staticmethod(make_std_filename)

	def make_chksumfile(self, filename):
		file = ChksumType.make_chksumfile(self, filename)
		file.write('Generated by: cfv (v%s)\n'%(version))
		file.write('Generated at: %s\n'%time.strftime('%a, %d %b %Y %H:%M:%S',time.gmtime(time.time())))
		file.write('Find  it  at: http://cfv.sourceforge.net/\n\n')
		try:
			import Image
			self.use_dimensions=1 ####should this be made optional somehow?
		except ImportError:
			self.use_dimensions=0
		self._flist = []
		self._fieldlens = [0]*6
		self._ftotal = 0L
		return file
	
	def make_chksumfile_finish(self, file):
		flens = self._fieldlens
		def writedata(data, self=self, flens=flens, file=file):
			file.write(data[0].ljust(flens[0]) + '  ' + data[1].rjust(flens[1]))
			if self.use_dimensions:
				file.write('  ' + data[2].rjust(flens[2]) + ' x ' + data[3].rjust(flens[3]))
			file.write('  ' + data[4].rjust(flens[4]) +	'  ' + data[5] + '\n')
			
		header = ('Filename', ' Filesize ', ' W', 'H '.ljust(flens[3]), ' CRC-32 ', 'Description')
		for i in range(0, len(header)-1):
			self._fieldlens[i] = max(self._fieldlens[i], len(header[i]))
		
		boundary = '-'*flens[0] + '  ' + '-'*flens[1] + '  '
		if self.use_dimensions:
			boundary = boundary + '-'*(flens[2]+flens[3]+3) + '  '
		boundary = boundary + '-'*flens[4] + '  ' + '-'*flens[5] + '\n'
		
		writedata(header)
		file.write(boundary)
		for fdata in self._flist:
			writedata(fdata)
		file.write(boundary)
		file.write('\nCount of files: %i\n'%len(self._flist))
		file.write('Total of sizes: %s\n'%commaize(self._ftotal))
	
	def make_addfile(self, filename):
		crc,size=getfilecrc(filename)
		crc = hexlify(crc)
		if self.use_dimensions:
			w,h = getimagedimensions(filename)
		else:
			w=h=''
		fdata = (filename, commaize(size), w, h, crc, '')
		for i in range(0, len(fdata)):
			self._fieldlens[i] = max(self._fieldlens[i], len(fdata[i]))
		self._flist.append(fdata)
		self._ftotal = self._ftotal + size
		
		#we cheat and do all writing in make_chksumfile_finish since it needs to be properly formatted, and we can't do that until we have seen the max lengths for all fields
		return (crc, size), ''


register_cftype('crc', JPEGSheriff_CRC)


#---------- generic ----------

def perhaps_showpath(file):
	if config.showpaths==1 or (config.showpaths==2 and config.recursive):
		if config.showpathsabsolute:
			dir=curdir
		else:
			dir=reldir[-1]
		return os.path.join(dir,file)
	return file

_nocase_dir_cache = {}
def nocase_dirfiles(dir,match):
	"return list of filenames in dir whose lowercase value equals match"
	dirkey=get_path_key(dir)
	if not _nocase_dir_cache.has_key(dirkey):
		d={}
		_nocase_dir_cache[dirkey]=d
		for a in os.listdir(dir):
			l=a.lower()
			if d.has_key(l):
				d[l].append(a)
			else:
				d[l]=[a]
	else:
		d=_nocase_dir_cache[dirkey]
	if d.has_key(match):
		return d[match]
	return []

def path_split(filename):
	"returns a list of components of filename"
	head=filename
	parts=[]
	while 1:
		head,tail=os.path.split(head)
		if tail:
			parts.insert(0,tail)
		else:
			if head:
				parts.insert(0,head)
			break
	return parts

FINDFILE=1
FINDDIR=0
def nocase_findfile(filename,find=FINDFILE):
	cur=os.curdir
	parts=path_split(filename.lower())
	#print 'nocase_findfile:',filename,parts,len(parts)
	for i in range(0,len(parts)):
		p=parts[i]
		#matches=filter(lambda f,p=p: string.lower(f)==p,dircache.listdir(cur)) #too slooow, even with dircache (though not as slow as without it ;)
		matches=nocase_dirfiles(cur,p) #nice and speedy :)
		#print 'i:',i,' cur:',cur,' p:',p,' matches:',matches
		if i==len(parts)-find:#if we are on the last part of the path and using FINDFILE, we want to match a file
			matches=filter(lambda f,cur=cur: os.path.isfile(os.path.join(cur,f)), matches)
		else:#otherwise, we want to match a dir
			matches=filter(lambda f,cur=cur: os.path.isdir(os.path.join(cur,f)), matches)
		if not matches:
			raise IOError, (errno.ENOENT,os.strerror(errno.ENOENT))
		if len(matches)>1:
			raise IOError, (errno.EEXIST,"More than one name matches %s"%os.path.join(cur,p))
		if cur==os.curdir:
			cur=matches[0] #don't put the ./ on the front of the name
		else:
			cur=os.path.join(cur,matches[0])
	return cur

def nocase_findfile_updstats(filename):
	cur = nocase_findfile(filename)
	if filename != cur:
		stats.diffcase = stats.diffcase + 1
	return cur
	
def strippath(filename, num='a', _splitdrivere=re.compile(r"[a-z]:[/\\]",re.I)):
	if num=='a':#split all the path off
		return os.path.split(filename)[1]
	if num=='n':#split none of the path
		return filename

	if _splitdrivere.match(filename,0,3): #we can't use os.path.splitdrive, since we want to get rid of it even if we are not on a dos system.
		filename=filename[3:]
	if filename[0]==os.sep:
		filename=lstrip(filename, os.sep)
	
	if num==0:#only split drive letter/root slash off
		return filename

	parts = path_split(filename)
	if len(parts) <= num:
		return parts[-1]
	return os.path.join(*parts[num:])

_os_sep_escaped = os.sep.replace('\\','\\\\')
def fixpath(filename):
	if config.fixpaths:
		return config.fixpaths.sub(_os_sep_escaped, filename)
	return filename

def mangle_filename(filename):
	if config.unquote and len(filename)>1 and filename[0]=='"' and filename[-1]=='"':
		filename = filename[1:-1] #work around buggy sfv encoders that quote filenames
		stats.quoted = stats.quoted + 1
	if config.fixpaths:
		filename=fixpath(filename)
	filename=os.path.normpath(filename)
	if config.strippaths!='n':
		filename=strippath(filename, config.strippaths)
	return filename

def find_local_filename(l_filename):
	if config.ignorecase:
		#we need to find the correct filename if using showunverified, even if the filename we are given works, since on FAT/etc filesystems the incorrect case will still return true from os.path.exists, but it could be the incorrect case to remove from the unverified files list.
		if config.showunverified or not os.path.exists(l_filename):
			l_filename=nocase_findfile_updstats(l_filename)
	if config.showunverified:
		cache.set_verified(l_filename)
	return l_filename


_visited_dirs = {}
def visit_dir(name, st=None, noisy=1):
	if not config.dereference and os.path.islink(name):
		return 0
	if st is None:
		try:
			st = os.stat(name)
		except os.error:
			return 0
	if S_ISDIR(st[ST_MODE]):
		#the inode check is kinda a hack, but systems that don't have inode numbers probably don't have symlinks either.
		if config.dereference and st[ST_INO]:
			dir_key = (st[ST_DEV],  st[ST_INO])
			if _visited_dirs.has_key(dir_key):
				if noisy:
					perror("skipping already visited dir %s %s"%(perhaps_showpath(name), dir_key))
				return 0
			_visited_dirs[dir_key] = 1
		return 1
	return 0

def test(filename, typename, restrict_typename='auto'):
	if typename != 'auto':
		cf = cftypes[typename]()
		cf.test_chksumfile(None, filename)
		return

	try:
		file=doopen_read(filename)
		cftype = auto_chksumfile_match(file)
		if restrict_typename != 'auto' and cftypes[restrict_typename] != cftype:
			return
		if cftype:
			cf = cftype()
			cf.test_chksumfile(file, filename)
			return
	except EnvironmentError, a:
		stats.cferror=stats.cferror+1
		perror('%s : %s (CF)'%(perhaps_showpath(filename),enverrstr(a)))
		return -1
	perror("I don't recognize the type of %s"%filename)
	stats.cferror=stats.cferror+1

def make(cftype,ifilename,testfiles):
	file=None
	if ifilename:
		filename=ifilename
	else:
		filename=cftype.make_std_filename(os.path.basename(curdir))
		if config.gzip==1 and filename[-3:]!='.gz': #if user does -zz, perhaps they want to force the filename to be kept?
			filename=filename+'.gz'
	if not hasattr(cftype, "make_addfile"):
		perror("%s : %s not supported in create mode"%(filename, cftype.__name__.lower()))
		stats.cferror=stats.cferror+1
		return
	if os.path.exists(filename):
		perror("%s already exists"%perhaps_showpath(filename))
		stats.cferror=stats.cferror+1
		file=IOError #just need some special value to indicate a cferror so that recursive mode still continues to work, IOError seems like a good choice ;)
	if not testfiles:
		tfauto=1
		testfiles=os.listdir(os.curdir)
		if config.dirsort:
			testfiles.sort()
	else:
		tfauto=0
	testdirs=[]

	if config.verbose>=0 or config.verbose==-3:
		cf_stats = stats.make_sub_stats()
	
	i=0
	while i<len(testfiles):
		f=testfiles[i]
		i=i+1
		if not tfauto and f=='-':
			f=''
		elif not os.path.isfile(f):
			if config.recursive and visit_dir(f):
				if config.recursive==1:
					testdirs.append(f)
				elif config.recursive==2:
					try:
						rfiles=os.listdir(f)
						if config.dirsort:
							rfiles.sort()
						testfiles[:i]=map(lambda x,p=f: os.path.join(p,x), rfiles)
						i=0
					except EnvironmentError, a:
						perror('%s%s : %s'%(f, os.sep, enverrstr(a)))
						stats.ferror=stats.ferror+1
				continue
			if tfauto:#if user isn't specifying files, don't even try to add dirs and stuff, and don't print errors about it.
				continue
		stats.num=stats.num+1
		if file==IOError:
			continue
		if file==None:
			try:
				cf = cftype()
				file = cf.make_chksumfile(filename)
			except EnvironmentError, a:
				stats.cferror=stats.cferror+1
				perror('%s : %s (CF)'%(perhaps_showpath(filename),enverrstr(a)))
				file=IOError
				continue
			dof=cf.make_addfile
		try:
			(filecrc,filesize),dat = dof(f)
		except EnvironmentError, a:
			if a[0]==errno.ENOENT:
				stats.notfound=stats.notfound+1
			else:
				stats.ferror=stats.ferror+1
			perror('%s : %s'%(f,enverrstr(a)))
			continue
		try:
			file.write(dat)
		except EnvironmentError, a:
			stats.cferror=stats.cferror+1
			perror('%s : %s (CF)'%(perhaps_showpath(filename),enverrstr(a)))
			file=IOError
			continue
		if filesize>=0:
			pverbose('%s : OK (%i,%s)'%(perhaps_showpath(f),filesize,filecrc))
		else:
			pverbose('%s : OK (%s)'%(perhaps_showpath(f),filecrc))
		stats.ok=stats.ok+1
	if file and file!=IOError:
		try:
			cf.make_chksumfile_finish(file)
			file.close()
		except EnvironmentError, a:
			stats.cferror=stats.cferror+1
			perror('%s : %s (CF)'%(perhaps_showpath(filename),enverrstr(a)))
		else:
			if config.verbose>=0 or config.verbose==-3:
				cf_stats.sub_stats_end(stats)
				pinfo(perhaps_showpath(filename)+': ','')
				cf_stats.print_stats()
	
	for f in testdirs:
		try:
			chdir(f)
		except EnvironmentError, a:
			perror('%s%s : %s'%(f, os.sep, enverrstr(a)))
			stats.ferror=stats.ferror+1
		else:
			make(cftype,ifilename,None)
			cdup()


def print_unverified(file):
	perror('%s : not verified'%perhaps_showpath(file))

def show_unverified_file(file):
	unverified_file(file)
	print_unverified(file)
	
def unverified_file(file):
	if config.list&LISTUNVERIFIED:
		plistf(file)
	stats.unverified=stats.unverified+1

def show_unverified_dir(path, unvchild=0):
	pathcache = cache.getpathcache(path)
	pathfiles = os.listdir(path or os.curdir)
	vsub = 0
	unvsave = stats.unverified
	unv = 0
	unv_sub_dirs = []
	for fn in pathfiles:
		file = os.path.join(path, fn)
		try:
			st = os.stat(file)
			if S_ISDIR(st[ST_MODE]) and visit_dir(file,st,noisy=0):
				dunvsave = stats.unverified
				dv = show_unverified_dir(file,not pathcache)
				vsub = vsub + dv
				if stats.unverified - dunvsave and not dv: #if this directory (and its subdirs) had unverified files and no verified files
					unv_sub_dirs.append(file)
			elif pathcache:
				if not pathcache.get(fn,{}).get('_verified'):
					if S_ISREG(st[ST_MODE]):
						show_unverified_file(file)
			else:
				if S_ISREG(st[ST_MODE]):
					unverified_file(file)
					unv = unv + 1
		except OSError:
			pass
	if not pathcache and pathfiles:
		if vsub: # if sub directories do have verified files
			if unv: # and this directory does have unverified files
				print_unverified(os.path.join(path,'*'))
			for file in unv_sub_dirs: # print sub dirs that had unverified files and no verified files
				print_unverified(os.path.join(file,'**'))
		elif not unvchild: # if this is the root of a tree with no verified files
			if stats.unverified - unvsave: # and there were unverified files in the tree
				print_unverified(os.path.join(path,'**'))
	return vsub + (not not pathcache)
			
def show_unverified_dir_verbose(path):
	pathcache = cache.getpathcache(path)
	pathfiles = os.listdir(path or os.curdir)
	for fn in pathfiles:
		file = os.path.join(path, fn)
		try:
			st = os.stat(file)
			if S_ISDIR(st[ST_MODE]) and visit_dir(file,st,noisy=0):
				show_unverified_dir_verbose(file)
			elif not pathcache.get(fn,{}).get('_verified'):
				if S_ISREG(st[ST_MODE]):
					show_unverified_file(file)
		except OSError,e:
			pass
	
def show_unverified_files(filelist):
	if not config.showunverified:
		return
	if filelist:
		for file in filelist:
			if config.ignorecase:
				try:
					file = nocase_findfile(file)
				except IOError:
					continue
			else:
				if not os.path.isfile(file):
					continue
			if not cache.is_verified(file):
				show_unverified_file(file)
	else:
		_visited_dirs.clear()
		if config.showunverified==2:
			show_unverified_dir_verbose('')
		else:
			show_unverified_dir('')


atrem=re.compile(r'md5|sha1|\.(csv|sfv|par|p[0-9][0-9]|par2|torrent|crc)(\.gz)?$',re.IGNORECASE)#md5sum/sha1sum files have no standard extension, so just search for files with md5/sha1 in the name anywhere, and let the test func see if it really is one.
def autotest(typename):
	for a in os.listdir(os.curdir):
		if config.recursive and visit_dir(a):
			try:
				chdir(a)
			except EnvironmentError, e:
				perror('%s%s : %s'%(a, os.sep, enverrstr(e)))
				stats.ferror=stats.ferror+1
			else:
				autotest(typename)
				cdup()
		if atrem.search(a):
			test(a, 'auto', typename)

def printusage(err=0):
	phelp = err and perror or pinfo
	phelp('Usage: cfv [opts] [-p dir] [-T|-C] [-t type] [-f file] [files...]')
	phelp('  -r       recursive mode 1 (make seperate chksum files for each dir)')
	phelp('  -rr      recursive mode 2 (make a single file with deep listing in it)')
	phelp('  -R       not recursive (default)')
	if _hassymlinks:
		phelp('  -l       follow symlinks (default)')
		phelp('  -L       don\'t follow symlinks')
	phelp('  -T       test mode (default)')
	phelp('  -C       create mode')
	phelp('  -t <t>   set type to <t> (%s, or auto(default))'%', '.join(sorted(cftypes.keys())))
	phelp('  -f <f>   use <f> as list file')
	phelp('  -m       check only for missing files (don\'t compare checksums)')
	phelp('  -M       check checksums (default)')
	phelp('  -n       rename bad files')
	phelp('  -N       don\'t rename bad files (default)')
	phelp('  -s       search for correct file')
	phelp('  -S       don\'t search for correct file (default)')
	phelp('  -p <d>   change to directory <d> before doing anything')
	phelp('  -i       ignore case')
	phelp('  -I       don\'t ignore case (default)')
	phelp('  -u       show unverified files')
	phelp('  -U       don\'t show unverified files (default)')
	phelp('  -v       verbose')
	phelp('  -V       not verbose (default)')
	phelp('  -VV      don\'t print status line at end either')
	phelp('  -q       quiet mode.  check exit code for success.')
	phelp('  -Q       mostly quiet mode.  only prints status lines.')
	phelp('  -zz      force making gzipped files, even if not ending in .gz')
	phelp('  -z       make gzipped files in auto create mode')
	phelp('  -Z       don\'t create gzipped files automatically. (default)')
	phelp('  -ZZ      never use gzip, even if file ends in .gz')
	phelp(' --list=<l> raw list files of type <l> (%s)'%', '.join(LISTARGS.keys()))
	phelp(' --list0=<l> same as list, but seperate files with nulls (useful for xargs -0)')
	phelp(' --unquote=VAL  handle checksum files with quoted filenames (yes or no(default))')
	phelp(' --fixpaths=<s>  replace any chars in <s> with %s'%os.sep)
	phelp(' --strippaths=VAL  strip leading components from file names.')
	phelp(' --showpaths=<p> show full paths (none/auto/yes-absolute/relative)')
	phelp(' --renameformat=<f> format string to use with -n option')
	phelp(' --progress=VAL  show progress meter (yes, no, or auto(default))')
	phelp(' --help/-h show help')
	phelp(' --version show cfv and module versions')
	phelp('torrent creation options:')
	phelp(' --announceurl=URL    tracker announce url')
	phelp(' --piece_size_pow2=N  power of two to set the piece size to (default 18)')
	phelp(' --private_torrent    set private flag in torrent')
	sys.exit(err)
def printhelp():
	pinfo('cfv v%s - Copyright (C) 2000-2009 Matthew Mueller - GPL license'%version)
	printusage()
def printcftypehelp(err):
	phelp = err and perror or pinfo
	phelp('Valid types:')
	def printtypeinfo(typename, info, desc, phelp=phelp):
		phelp(' %-8s %-26s %s'%(typename,desc,info))
	printtypeinfo('TYPE', 'FILE INFO STORED', 'DESCRIPTION')
	printtypeinfo('auto', '', 'autodetect type (default)')
	for typename in sorted(cftypes.keys()):
		printtypeinfo(typename, cftypes[typename].descinfo, cftypes[typename].description)
	sys.exit(err)

stats=Stats()
config=Config()
cache=FileInfoCache()
progress=TimedProgressMeter()


def main(argv=None):
	if argv is None:
		argv = sys.argv[1:]
	manual=0
	mode=0
	typename='auto'

	try:
		optlist, args = getopt.getopt(argv, 'rRlLTCt:f:mMnNsSp:uUiIvVzZqQh?',
				['list=', 'list0=', 'fixpaths=', 'strippaths=', 'showpaths=','renameformat=','progress=','unquote=','help','version',
				'announceurl=','piece_size_pow2=','private_torrent','noprivate_torrent', #torrent options
				])
	except getopt.error, a:
		perror("cfv: %s"%a)
		printusage(1)

	try:
		if config.cmdlineglob=='y' or (config.cmdlineglob=='a' and os.name in ('os2', 'nt', 'dos')):
			from glob import glob
			globbed = []
			for arg in args:
				if '*' in arg or '?' in arg or '[' in arg:
					g = glob(arg)
					if not g:
						raise CFVValueError, 'no matches for %s'%arg
					globbed.extend(g)
				else:
					globbed.append(arg)
			args = globbed
			
		if config.cmdlinesort:
			args.sort()

		prevopt=''
		for o,a in optlist:
			if o=='-T':
				mode=0
			elif o=='-C':
				mode=1
			elif o=='-t':
				if a=='help':
					printcftypehelp(err=0)
				if not a in ['auto']+cftypes.keys():
					perror('cfv: type %s not recognized'%a)
					printcftypehelp(err=1)
				typename=a
			elif o=='-f':
				setup_output()
				manual=1 #filename selected manually, don't try to autodetect
				if mode==0:
					cache.set_testfiles(args)
					test(a, typename)
				else:
					if typename!='auto':
						make(cftypes[typename],a,args)
					else:
						testa=''
						if config.gzip>=0 and a[-3:]=='.gz':
								testa=a[:-3]
						cftype = auto_filename_match(a, testa)
						if not cftype:
							raise CFVValueError, 'specify a filetype with -t, or use standard extension'
						make(cftype,a,args)
			elif o=='-U':
				config.showunverified=0
			elif o=='-u':
				if prevopt=='-u':
					config.showunverified=2
				else:
					config.showunverified=1
			elif o=='-I':
				config.ignorecase=0
			elif o=='-i':
				config.ignorecase=1
			elif o=='-n':
				config.rename=1
			elif o=='-N':
				config.rename=0
			elif o=='-s':
				config.search=1
			elif o=='-S':
				config.search=0
			elif o=='--renameformat':
				config.setx('renameformat', a)
			elif o=='-m':
				config.docrcchecks=0
			elif o=='-M':
				config.docrcchecks=1
			elif o=='-p':
				chdir(a)
			elif o=='-r':
				if prevopt=='-r':
					config.recursive=2
				else:
					config.recursive=1
			elif o=='-R':
				config.recursive=0
			elif o=='-l':
				config.dereference=1
			elif o=='-L':
				config.dereference=0
			elif o=='-v':
				config.setx('verbose', 'v')
			elif o=='-V':
				if prevopt=='-V':
					config.setx('verbose', 'VV')
				else:
					config.setx('verbose', 'V')
			elif o=='-q':
				config.setx('verbose', 'q')
			elif o=='-Q':
				config.setx('verbose', 'Q')
			elif o=='--progress':
				config.setx('progress', a)
			elif o=='-z':
				if prevopt=='-z':
					config.gzip=2
				else:
					config.gzip=1
			elif o=='-Z':
				if prevopt=='-Z':
					config.gzip=-1
				else:
					config.gzip=0
			elif o=='--list' or o=='--list0':
				if a not in LISTARGS.keys():
					raise CFVValueError, 'list arg must be one of '+`LISTARGS.keys()`
				config.list=LISTARGS[a]
				config.listsep = o=='--list0' and '\0' or '\n'
				if config.list==LISTUNVERIFIED:
					config.showunverified=1
				set_stdout_special() #redirect all messages to stderr so only the list gets on stdout
			elif o=='--showpaths':
				config.setx('showpaths', a)
			elif o=='--strippaths':
				config.setx('strippaths', a)
			elif o=='--fixpaths':
				config.setx('fixpaths', a)
			elif o=='--unquote':
				config.setx('unquote', a)
			elif o=='--announceurl':
				config.setx('announceurl', a)
			elif o=='--piece_size_pow2':
				config.setx('piece_size_pow2', a)
			elif o=='--private_torrent':
				config.private_torrent=True
			elif o=='--noprivate_torrent':
				config.private_torrent=False
			elif o=='-h' or o=='-?' or o=='--help':
				printhelp()
			elif o=='--version':
				print 'cfv %s'%version
				try:
					if not nommap: print '+mmap'
				except NameError: pass
				try: print 'fchksum %s'%fchksum.version()
				except NameError: pass
				print 'python %08x-%s'%(sys.hexversion,sys.platform)
				sys.exit(0)
			prevopt=o
	except CFVValueError, e:
		perror('cfv: %s'%e)
		sys.exit(1)
	
	setup_output()
	
	if not manual:
		if mode==0:
			cache.set_testfiles(args)
			autotest(typename)
		else:
			if typename=='auto':
				typename=config.defaulttype
			make(cftypes[typename],None,args)
	
	if mode==0:
		show_unverified_files(args)

	#only print total stats if more than one checksum file has been checked. (or if none have)
	#We must also print stats here if there are unverified files or checksum file errors, since those conditions occur outside of the cf_stats section.
	if stats.subcount != 1 or stats.unverified or stats.cferror:
		stats.print_stats()

	sys.exit((stats.badcrc and 2) | (stats.badsize and 4) | (stats.notfound and 8) | (stats.ferror and 16) | (stats.unverified and 32) | (stats.cferror and 64))

if __name__=='__main__':
	main()
