#!/usr/bin/env python
# -*- coding: utf-8 -*-

# All credits go to:
# https://github.com/jmoiron/par2ools/blob/master/par2ools/par2.py

'''
Copyright (c) 2010 Jason Moiron and Contributors

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''


"""A native python implementation of the par2 file format.
This is only intended to be able to read packets in par2, not repair,
verify, or create new par2 files."""


import sys
import fnmatch
import glob
import os
import re
import struct
import rarfile
import subprocess
import pexpect
import time
import inotify_simple
import shutil
import queue
import multiprocessing as mp
import hashlib
from random import randint


signatures = {
    'par2': b'PAR2\x00',
    'zip': b'PK\x03\x04',  # empty is \x05\x06, multi-vol is \x07\x08
    'rar': b'Rar!\x1A\x07\x00',
    '7zip': b'7z\xbc\xaf\x27\x1c',
    'bzip2': b'BZh',
    'gzip': b'\x1f\x8b\x08',
}


def check_for_par_filetype(fname):
    # return: 1 ... par2 main file
    #         2 ... par2vol file
    #         0 ... no par file
    #         -1 .. file no exist err.
    try:
        with open(fname, "rb") as f:
            content = f.read()
    except Exception as e:
        print(str(e) + ": file " + fname + " does not exist!")
        return -1
    # check if PAR 2.0\0Creator\0 in last 50 bytes
    # if not -> it's no par file
    if len(content) > 100:
        cut0 = 100
    else:
        cut0 = len(content)
    lastcontent = content[-cut0:]
    bstr0 = b"PAR 2.0\0Creator\0"
    if bstr0 not in lastcontent:
        return 0
    # check for PAR2\x00 in first 50 bytes
    # if no -> it's par2vol, else: could be par2
    firstcontent = content[:cut0]
    bstr0 = b"PAR2\x00"             # PAR2\x00
    bstr1 = b"PAR 2.0\x00FileDesc"  # PAR 2.0\x00FileDesc'
    if bstr0 not in firstcontent:
        return 2
    elif bstr1 in firstcontent:
        return 1
    else:
        return 2


def check_for_rar_filetype(fname):
    # return: 1 ... rar file
    #         0 ... no rar file
    #         -1 .. file no exist err.
    try:
        with open(fname, "rb") as f:
            content = f.read()
    except Exception:
        return -1
    firstcontent = content[:20]
    bstr0 = b"Rar!"
    if bstr0 in firstcontent:
        return 1
    return 0


lscolors = filter(None, os.environ.get('LS_COLORS', '').split(':'))
dircolormap = dict([x.split('=') for x in lscolors])
colorremap = {}
for k, v in dircolormap.items():
    if '*' not in k:
        continue
    colorremap.setdefault(v, []).append(fnmatch.translate(k))
for k, v in colorremap.items():
    colorremap[k] = re.compile('(%s)' % '|'.join(v))


def baseglob(pat, base):
    """Given a pattern and a base, return files that match the glob pattern
    and also contain the base."""
    return [f for f in glob.glob(pat) if f.startswith(base)]


def cibaseglob(pat, base):
    """Case insensitive baseglob.  Note that it's not *actually* case
    insensitive, since glob is insensitive or not based on local semantics.
    Instead, it tries the original version, an upper() version, a lower()
    version, and a swapcase() version of the glob."""
    results = []
    for func in (str, str.upper, str.lower, str.swapcase):
        results += baseglob(func(pat), base)
    return list(sorted(set(results)))


def dircolorize(path, name_only=True):
    """Use user dircolors settings to colorize a string which is a path.
    If name_only is True, it does this by the name rules (*.x) only; it
    will not check the filesystem to colorize things like pipes, block devs,
    doors, etc."""
    if not name_only:
        raise NotImplemented("Filesystem checking not implemented.")
    for k, regex in colorremap.items():
        if regex.match(path):
            return '\x1b[%(color)sm%(path)s\x1b[00m' % {'color': k, 'path': path}
    return path


PACKET_HEADER = ("<"
                 "8s"   # MAGIC: PAR2\x00PKT
                 "Q"    # unsigned 64bit length of entire packet in bytes
                 "16s"  # md5 of entire packet except first 3 fields
                 "16s"  # 'setid';  hash of the body of the main packet
                 "16s")  # packet type


FILE_DESCRIPTION_PACKET = ("<64s"  # PACKET_HEADER
                           "16s"   # fileid, hash of [hash16k, length, name]
                           "16s"   # hashfull;  hash of the whole file (which?)
                           "16s"   # hash16k;  hash of the first 16k of the file (which?)
                           "Q")    # length of the file
# Length (bytes)	Type	        Description
#   8	                8-byte uint	Slice size. Must be a multiple of 4.
#   4	                4-byte uint	Number of files in the recovery set.
# ?*16	                MD5 Hash array	File IDs of all files in the recovery set. (See File Description packet.) These hashes are sorted by numerical value
#                                       (treating them as 16-byte unsigned integers).
# ?*16	                MD5 Hash array	File IDs of all files in the non-recovery set. (See File Description packet.) These hashes are sorted by numerical value
#                                       (treating them as 16-byte unsigned integers).
MAIN_PACKET = ("<64s"   # PACKET_HEADER
               "q"     # 8-byte uint	Slice size. Must be a multiple of 4.
               "i")     # 4-byte uint	Number of files in the recovery set.

RECOVERY_SLICE_PACKET = ("<64s"
                         "i")   # Exponent used to generate recovery data

IFSC_PACKET = ("<64s"   # PACKET_HEADER
               "16s")   # The File ID of the file.

CREATOR_PACKET = ("<64s")


class Header(object):
    fmt = PACKET_HEADER

    def __init__(self, par2file, offset=0):
        self.raw = par2file[offset:offset+struct.calcsize(self.fmt)]
        parts = struct.unpack(self.fmt, self.raw)
        self.magic = parts[0]
        self.length = parts[1]
        self.hash = parts[2]
        self.setid = parts[3]
        self.type = parts[4]

    def verify(self):
        return self.magic == b'PAR2\x00PKT'


class IFSCPacket(object):
    header_type = b"PAR 2.0\x00IFSC\x00\x00\x00\00"
    fmt = IFSC_PACKET

    def __init__(self, par2file, offset=0):
        self.raw = par2file[offset:offset+struct.calcsize(self.fmt)]
        parts = struct.unpack(self.fmt, self.raw)
        self.header = Header(parts[0])
        self.fileid = parts[1]
        self.fmt0 = "20s"
        self.md5_crc32_array = []
        offset0 = offset+struct.calcsize(self.fmt)
        while offset0 < offset + self.header.length:
            self.raw2 = par2file[offset0:offset0+struct.calcsize(self.fmt0)]
            parts0 = struct.unpack(self.fmt0, self.raw2)
            self.md5_crc32_array.append(parts0[0])
            offset0 += struct.calcsize(self.fmt0)


class RecSlicePacket(object):
    header_type = b"PAR 2.0\x00RecvSlic"
    fmt = RECOVERY_SLICE_PACKET

    def __init__(self, par2file, offset=0):
        self.raw = par2file[offset:offset+struct.calcsize(self.fmt)]
        parts = struct.unpack(self.fmt, self.raw)
        self.header = Header(parts[0])
        self.exponent = parts[1]

class UnknownPar2Packet(object):
    fmt = PACKET_HEADER

    def __init__(self, par2file, offset=0):
        self.raw = par2file[offset:offset+struct.calcsize(self.fmt)]
        self.header = Header(self.raw)


class MainPacket(object):
    header_type = b"PAR 2.0\x00Main\x00\x00\x00\x00"
    fmt = MAIN_PACKET

    def __init__(self, par2file, offset=0):
        self.raw = par2file[offset:offset+struct.calcsize(self.fmt)]
        parts = struct.unpack(self.fmt, self.raw)
        self.header = Header(parts[0])
        self.slice_size = parts[1]
        self.no_files = parts[2]
        self.file_id_list = []

        self.fmt0 = "16s"
        offset0 = offset+struct.calcsize(self.fmt)
        while offset0 < offset + self.header.length:
            self.raw2 = par2file[offset0:offset0+struct.calcsize(self.fmt0)]
            parts0 = struct.unpack(self.fmt0, self.raw2)
            self.file_id_list.append(parts0[0])
            offset0 += struct.calcsize(self.fmt0)


class CreatorPacket(object):
    header_type = b"PAR 2.0\x00Creator\x00"
    fmt = CREATOR_PACKET

    def __init__(self, par2file, offset=0):
        self.raw = par2file[offset:offset+struct.calcsize(self.fmt)]
        parts = struct.unpack(self.fmt, self.raw)
        self.header = Header(parts[0])
        packet = par2file[offset:offset+self.header.length]
        self.clienttext = packet[struct.calcsize(self.fmt):]


class FileDescriptionPacket(object):
    header_type = b'PAR 2.0\x00FileDesc'
    fmt = FILE_DESCRIPTION_PACKET

    def __init__(self, par2file, offset=0):
        name_start = offset+struct.calcsize(self.fmt)
        self.raw = par2file[offset:name_start]
        parts = struct.unpack(self.fmt, self.raw)
        self.header = Header(parts[0])
        packet = par2file[offset:offset+self.header.length]
        self.fileid = parts[1]
        self.file_hashfull = parts[2]
        self.file_hash16k = parts[3]
        self.file_length = parts[4]
        self.name = packet[struct.calcsize(self.fmt):].strip(b'\x00')


class Par2File(object):
    def __init__(self, obj_or_path):
        """A convenient object that reads and makes sense of Par2 blocks."""
        self.path = None
        if isinstance(obj_or_path, str):
            with open(obj_or_path, "rb") as f:
                self.contents = f.read()
                self.path = obj_or_path
        else:
            self.contents = obj_or_path.read()
            if getattr(obj_or_path, 'name', None):
                self.path = obj_or_path.name
        try:
            self.packets = self.read_packets()
        except Exception:
            self.packets = []

    def __bool__(self):
        if self.packets:
            return True
        return False

    def is_par2(self):
        #print("--> #1", self.packets)
        #print("--> #2", self.get_recslice_packets())
        if self.packets and not self.get_recslice_packets():
            return True
        return False

    def is_par2vol(self):
        if self.packets and self.get_recslice_packets():
            return True
        return False

    # -1 no par2 file, 1 par2 file, 2 par2vol file
    def get_par2_type(self):
        if not self.packets:
            return -1
        if self.get_recslice_packets():
            return 2
        return 1

    def read_packets(self):
        offset = 0
        filelen = len(self.contents)
        packets = []
        while offset < filelen:
            header = Header(self.contents, offset)
            if header.type == FileDescriptionPacket.header_type:
                packets.append(FileDescriptionPacket(self.contents, offset))
            elif header.type == RecSlicePacket.header_type:
                packets.append(RecSlicePacket(self.contents, offset))
            elif header.type == MainPacket.header_type:
                packets.append(MainPacket(self.contents, offset))
            elif header.type == CreatorPacket.header_type:
                packets.append(CreatorPacket(self.contents, offset))
            elif header.type == IFSCPacket.header_type:
                packets.append(IFSCPacket(self.contents, offset))
            offset += header.length
        return packets

    def get_structure(self):
        pstruct = {"FileDescriptionPacket": 0,
                   "RecSlicePacket": 0,
                   "MainPacket": 0,
                   "CreatorPacket": 0,
                   "IFSCPacket": 0,
                   "UnknownPar2Packet": 0}

        for p in self.packets:
            if isinstance(p, FileDescriptionPacket):
                pstruct["FileDescriptionPacket"] += 1
            if isinstance(p, RecSlicePacket):
                pstruct["RecSlicePacket"] += 1
            if isinstance(p, MainPacket):
                pstruct["MainPacket"] += 1
            if isinstance(p, CreatorPacket):
                pstruct["CreatorPacket"] += 1
            if isinstance(p, IFSCPacket):
                pstruct["IFSCPacket"] += 1
            if isinstance(p, UnknownPar2Packet):
                pstruct["UnknownPar2Packet"] += 1

        return pstruct

    def filenames_only(self):
        """Returns the filenames that this par2 file repairs."""
        fnlist = [p.name.decode("utf-8") for p in self.packets if isinstance(p, FileDescriptionPacket)]
        return list(dict.fromkeys(fnlist))

    def filenames(self):
        """Returns the filenames that this par2 file repairs."""
        fnlist = [(p.name.decode("utf-8"), p.file_hashfull) for p in self.packets if isinstance(p, FileDescriptionPacket)]
        return list(dict.fromkeys(fnlist))

    def get_main_packets(self, clean=False):
        mlist = [(i, p.slice_size, p.no_files, p.file_id_list)
                 for i, p in enumerate(self.packets) if isinstance(p, MainPacket)]
        if clean:
            return list(dict.fromkeys(mlist))
        return mlist

    def get_filedescription_packets(self, clean=False):
        mlist = [(i, p.fileid, p.file_hashfull, p.file_hash16k, p.file_length, p.name)
                 for i, p in enumerate(self.packets) if isinstance(p, FileDescriptionPacket)]
        if clean:
            return list(dict.fromkeys(mlist))
        return mlist

    def get_recslice_packets(self, clean=False):
        mlist = [(i, p.exponent) for i, p in enumerate(self.packets) if isinstance(p, RecSlicePacket)]
        if clean:
            return list(dict.fromkeys(mlist))
        return mlist

    def get_creator_packets(self, clean=False):
        mlist = [(i, p.clienttext) for i, p in enumerate(self.packets) if isinstance(p, CreatorPacket)]
        if clean:
            return list(dict.fromkeys(mlist))
        return mlist

    def get_ifsc_packets(self, clean=False):
        mlist = [(i, p.fileid, p.md5_crc32_array) for i, p in enumerate(self.packets) if isinstance(p, IFSCPacket)]
        if clean:
            return list(dict.fromkeys(mlist))
        return mlist

    # returns xxx, yyy in ...volxxx+yyy.par2
    def get_vol_exponents(self):
        exp_list = [p.exponent for p in self.packets if isinstance(p, RecSlicePacket)]
        if exp_list:
            return min(exp_list), max(exp_list) - min(exp_list) + 1
        return -1, -1

    def md5_16khash(self):
        hashlist = [(p.name.decode("utf-8"), p.file_hash16k) for p in self.packets if isinstance(p, FileDescriptionPacket)]
        return list(dict.fromkeys(hashlist))

    def related_pars(self):
        """Returns a list of related par2 files (ones par2 will try to read
        from to find file recovery blocks).  If this par2 file was a file-like
        object (like a StringIO) without an associated path, return [].
        Otherwise, the name of this file + associated files are returned."""
        if not self.path:
            return []
        names = [self.path]
        basename = self.path.replace('.par2', '').replace('.PAR2', '')
        names += cibaseglob('*.vol*.PAR2', basename)
        return names


def calc_file_md5hash(fn):
    hash_md5 = hashlib.md5()
    try:
        with open(fn, "rb") as f0:
            for chunk in iter(lambda: f0.read(4096), b""):
                hash_md5.update(chunk)
        md5 = hash_md5.digest()
    except Exception:
        md5 = -1
    return md5


def calc_file_md5hash_16k(fn):
    hash_md5 = hashlib.md5()
    try:
        with open(fn, "rb") as f0:
            for i, chunk in enumerate(iter(lambda: f0.read(4096), b"")):
                hash_md5.update(chunk)
                if i == 3:
                    break
        md5 = hash_md5.digest()
    except Exception:
        md5 = -1
    return md5


def get_file_type(filename, inspect=False):
    if inspect:
        # check for rar
        try:
            with open(filename, "rb") as f:
                contents = f.read()
            rar_sig = signatures["rar"]
            rar_sig_len = len(rar_sig)
            if rar_sig in contents[:rar_sig_len + 20]:
                return "rar"
        except Exception:
            pass
        # check for par2 / par2vol
        try:
            p2 = Par2File(filename)
            if p2.is_par2():
                return "par2"
            if p2.is_par2vol():
                return "par2vol"
        except Exception:
            pass

    # if cannot match rars or par2 -> try simply with suffix
    if re.search(r"[.]rar$", filename, flags=re.IGNORECASE):
        filetype0 = "rar"
    elif re.search(r"[.]nfo$", filename, flags=re.IGNORECASE) or re.search(r"[.]nfo.txt$", filename, flags=re.IGNORECASE):
        filetype0 = "nfo"
    elif re.search(r"[.]sfv$", filename, flags=re.IGNORECASE):
        filetype0 = "sfv"
    elif re.search(r"[.]par2$", filename, flags=re.IGNORECASE):
        if re.search(r"vol[0-9][0-9]*[+]", filename, flags=re.IGNORECASE):
            filetype0 = "par2vol"
        else:
            filetype0 = "par2"
    else:
        filetype0 = "etc"
    return filetype0



if __name__ == "__main__":
    # fn = "/home/stephan/.ginzibix/incomplete/Der.Gloeckner.von.Notre/_renamed0/Walt.Disneys.Der.Gloeckner.von.Notre.Dame.2.German.2000.DVDRIP.XviD-AIO.vol15+16.par2"
    #fn = "/home/stephan/.ginzibix/incomplete/Der.Gloeckner.von.Notre/_renamed0/Walt.Disneys.Der.Gloeckner.von.Notre.Dame.2.German.2000.DVDRIP.XviD-AIO.nfo"
    #fn = "/home/stephan/.ginzibix/incomplete/Sons.of.Norway.German.1080p.BluRay.x264-EPHEMERiD/_renamed0/epd-sonsofnorway.1080p.r66"
    fn ="/home/stephan/.ginzibix/incomplete/The.Death.of.Stalin.2017.German.DTS.DL.1080p.BluRay.x265-UNFIrED/_renamed0/b50f1df4bbbb4d3f7f385bd425.vol000+001.PAR2"
    # fn = "/home/stephan/.ginzibix/incomplete/Der.Gloeckner.von.Notre/_renamed0/Walt.Disneys.Der.Gloeckner.von.Notre.Dame.2.German.2000.DVDRIP.XviD-AIO.par2"
    # fn = "/home/stephan/.ginzibix/incomplete/Der.Gloeckner.von.Notre/_renamed0/Walt.Disneys.Der.Gloeckner.von.Notre.Dame.2.German.2000.DVDRIP.XviD-AIO.part01.rar"

    print(get_file_type(fn, inspect=True))
    p2obj = Par2File(fn)
    print(p2obj.is_par2())
    sys.exit()
    p2obj = Par2File(fn)
    print(p2obj.get_structure())

    if not p2obj:
        print("No par2 file!")
        sys.exit()

    if p2obj.is_par2():
        print("This is a par2 main file")

    if p2obj.is_par2vol():
        vol_xxx, vol_yyy = p2obj.get_vol_exponents()
        print("This is a par2vol file with volumes vol" + str(vol_xxx) + "+" + str(vol_yyy))

    sys.exit()

    print("-" * 80)
    print("--- FileDescriptionPackets ---")
    print(p2obj.get_filedescription_packets())

    print("-" * 80)
    print("--- MainPackets ---")
    print(p2obj.get_main_packets())

    print("-" * 80)
    print("--- RecSlicePacket ---")
    print(p2obj.get_recslice_packets())

    print("-" * 80)
    print("--- CreatorPackets ---")
    print(p2obj.get_creator_packets())

    print("-" * 80)
    print("--- IFSCPackets ---")
    print(p2obj.get_ifsc_packets())

    print("-" * 80)
    print("--- MainPackets ---")
    print(p2obj.get_main_packets())

    print("-" * 80)
    print("--- Vol Info ---")
    vol_xxx, vol_yyy = p2obj.get_vol_exponents()
    print(vol_xxx, "+", vol_yyy)
