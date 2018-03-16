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

signatures = {
    'par2': 'PAR2\x00',
    'zip': 'PK\x03\x04',  # empty is \x05\x06, multi-vol is \x07\x08
    'rar': 'Rar!\x1A\x07\x00',
    '7zip': '7z\xbc\xaf\x27\x1c',
    'bzip2': 'BZh',
    'gzip': '\x1f\x8b\x08',
}

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


class UnknownPar2Packet(object):
    fmt = PACKET_HEADER

    def __init__(self, par2file, offset=0):
        self.raw = par2file[offset:offset+struct.calcsize(self.fmt)]
        self.header = Header(self.raw)


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
        self.packets = self.read_packets()

    def read_packets(self):
        offset = 0
        filelen = len(self.contents)
        packets = []
        while offset < filelen:
            header = Header(self.contents, offset)
            if header.type == FileDescriptionPacket.header_type:
                packets.append(FileDescriptionPacket(self.contents, offset))
            else:
                packets.append(UnknownPar2Packet(self.contents, offset))
            offset += header.length
        return packets

    def filenames(self):
        """Returns the filenames that this par2 file repairs."""
        return [(p.name.decode("utf-8"), p.file_hashfull) for p in self.packets if isinstance(p, FileDescriptionPacket)]

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


def multipartrar_test(directory, rarname0):
    rarnames = []
    sortedrarnames = []
    cwd0 = os.getcwd()
    os.chdir(directory)
    for r in glob.glob("*.rar"):
        rarnames.append(r)
    for r in rarnames:
        rarnr = r.split(".part")[-1].split(".rar")[0]
        sortedrarnames.append((int(rarnr), r))
    sortedrarnames = sorted(sortedrarnames, key=lambda nr: nr[0])
    rar0_nr, rar0_nm = [(nr, rarn) for (nr, rarn) in sortedrarnames if rarn == rarname0][0]
    ok_sorted = True
    for i, (nr, rarnr) in enumerate(sortedrarnames):
        if i + 1 == rar0_nr:
            break
        if i + 1 != nr:
            ok_sorted = False
            break
    if not ok_sorted:
        print(-1)
        return -1              # -1 cannot check, rar in between is missing
    # ok sorted, unrar t
    ssh = subprocess.Popen(["unrar", "t", rarname0], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
    status = 1
    for ss in ssh.stderr.readlines():
        ss0 = ss.decode("utf-8")
        if rarname0 in ss0:
            status = -2
            break

    #    1  success tested
    #    -1 cannot check, rar sequence not complete
    #    -2 rarfile error

    print(status)
    os.chdir(cwd0)


def get_inotify_events(inotify):
    rar_events = []
    for event in inotify.read():
        str0 = event.name
        is_created_file = False
        flgs0 = []
        for flg in inotify_simple.flags.from_mask(event.mask):
            if "flags.CREATE" in str(flg) and "flags.ISDIR" not in str(flg):
                flgs0.append(str(flg))
                is_created_file = True
        if not is_created_file:
            continue
        gg = re.search(r"\S*[.]rar", str0, flags=re.IGNORECASE)
        if gg.group():
            rar_events.append((gg.group(), flgs0))
    return rar_events


def get_rar_files(directory):
    rarlist = []
    for rarf in glob.glob("*.rar"):
        gg = re.search(r"[0-9]+[.]rar", rarf, flags=re.IGNORECASE)
        rarlist.append((int(gg.group().split(".")[0]), rarf))
    return rarlist


def partial_unrar(directory):
    cwd0 = os.getcwd()
    os.chdir(directory)
    # create unpack dir
    if directory[-1] != "/":
        directory += "/"
    unpack_dir = directory + "_unpack0"
    '''dir_done = False
    i = 0
    while not dir_done:
        unpack_dir = directory + "_unpack" + str(i)
        if os.path.exists(unpack_dir):
            i += 1
        else:
            break'''
    try:
        # os.mkdir(unpack_dir)
        pass
    except Exception as e:
        print(str(e) + ": cannot create unpack_dir")
        return -1
    # init inotify
    inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    inotify.add_watch(directory, watch_flags)

    # get already present rar files
    eventslist = []
    rar_basislist = get_rar_files(directory)
    rar_sortedlist = sorted(rar_basislist, key=lambda nr: nr[0])

    # wait for first file to arrive before starting unrar if no rar files present
    while not rar_sortedlist or rar_sortedlist[0][0] != 1:
        events = get_inotify_events(inotify)
        if events not in eventslist:
            eventslist.append(events)
            rar_basislist = get_rar_files(directory)
            rar_sortedlist = sorted(rar_basislist, key=lambda nr: nr[0])

    # first valid rar_sortedlist in place, start unrar!
    cmd = "unrar x -y -o+ -vp " + rar_sortedlist[0][1] + " " + unpack_dir
    print(cmd)
    child = pexpect.spawn(cmd)
    status = 1      # 1 ... running, 0 ... exited ok, -1 ... rar corrupt, -2 ..missing rar, -3 ... unknown error
    rarindex = 1
    while True:
        str0 = ""
        while True:
            try:
                a = child.read_nonblocking().decode("utf-8")
                str0 += a
            except pexpect.exceptions.EOF:
                break
            if str0[-6:] == "[Q]uit":
                break
        if "error" in str0:
            gg = re.search(r"\S*[.]rar", str0, flags=re.IGNORECASE)
            if "packed data checksum" in str0:
                statmsg = gg.group() + " : packed data checksum error (= corrupt rar!)"
                status = -1
            elif "- checksum error" in str0:
                statmsg = gg.group() + " : checksum error (= rar is missing!)"
                status = -2
            else:
                statmsg = gg.group() + " : unknown error"
                status = -3
            print(str0)
            break
        if "All OK" in str0:
            statmsg = "All OK"
            status = 0
            break
        # percstr = re.findall(r" [0-9]+%", str0)[-1]
        print(str0)
        # print(str0)
        print("-" * 16)
        rarindex += 1
        if rarindex not in [nr for nr, _ in rar_sortedlist]:
            gotnextrar = False
            while not gotnextrar:
                events = get_inotify_events(inotify)
                if events not in eventslist:
                    eventslist.append(events)
                    rar_basislist = get_rar_files(directory)
                    rar_sortedlist = sorted(rar_basislist, key=lambda nr: nr[0])
                    if rarindex in [nr for nr, _ in rar_sortedlist]:
                        gotnextrar = True
        child.sendline("C")
    print("100% - done")
    print(status, statmsg)
    os.chdir(cwd0)


partial_unrar("/home/stephan/.ginzibix/incomplete/st502304a4df4c023adf43c1462a.nfo")
