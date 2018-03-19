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

    def md5_16khash(self):
        return [(p.name.decode("utf-8"), p.file_hash16k) for p in self.packets if isinstance(p, FileDescriptionPacket)]
    
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
    except Exception as e:
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
    except Exception as e:
        md5 = -1
    return md5


def par_verifier(mp_inqueue, mp_outqueue, download_dir, verifiedrar_dir, main_dir, logger, filetypecounter, p2=None):

    maxrar = filetypecounter["rar"]["max"]
    logger.info("PAR_VERIFIER > Starting!")
    # phase I: verify
    rarlist = []
    while True:
        # get from inqueue
        res0 = None
        while True:
            try:
                res0 = mp_inqueue.get_nowait()
            except queue.Empty:
                break
        if res0:
            p2_0 = res0
            if p2_0 == -1:
                logger.warning("PAR_VERIFIER > Received signal to stop from main!")
                break
            # 0 ... not tested
            # -1 ... not ok (yet)
            # 1 ... tested & ok
            if p2_0:
                p2 = p2_0

        doloadpar2vols = False
        verifiedstatus = 0  # 0 .. still running, 1 .. all ok/finished, -1 .. not ok/finished
        corruptrars = []
        rarf = [r.split("/")[-1] for r in glob.glob(download_dir + "*.rar")]

        for r in rarf:
            if r not in [rn for rn, isok in rarlist]:
                rarlist.append((r, 0))
        # if no par2 given have to test with "unrar t"
        if not p2:
            for i, (filename, isok) in enumerate(rarlist):
                if os.path.isfile(verifiedrar_dir + filename):
                    continue
                if filename in rarf and isok == 0 or isok == -1:
                    res0 = multipartrar_test(download_dir, filename, logger)
                    logger.info("PAR_VERIFIER > unrar t: " + filename + " = " + str(res0))
                    rarlist[i] = filename, res0
                    if res0 == 1:
                        logger.info("PAR_VERIFIER > copying " + filename + " to " + verifiedrar_dir)
                        shutil.copy(download_dir + filename, verifiedrar_dir)
                    else:
                        corruptrars.append(((filename, isok)))
                        logger.warning("PAR_VERIFIER > error in 'unrar t' for file " + filename)
                        doloadpar2vols = True
        else:
            for i, (filename, isok) in enumerate(rarlist):
                if os.path.isfile(verifiedrar_dir + filename) or isok == -1:
                    continue
                if filename in rarf and isok == 0:
                    md5 = calc_file_md5hash(download_dir + filename)
                    md5match = [(pmd5 == md5) for pname, pmd5 in p2.filenames() if pname == filename]
                    logger.info("PAR_VERIFIER > p2 md5: " + filename + " = " + str(md5match))
                    if False in md5match:
                        rarlist[i] = filename, -1
                        corruptrars.append(((filename, md5, -1)))
                        logger.warning("PAR_VERIFIER > error in 'p2 md5' for file " + filename)
                        doloadpar2vols = True
                    else:
                        rarlist[i] = filename, 1
                        logger.info("PAR_VERIFIER > copying " + filename + " to " + verifiedrar_dir)
                        shutil.copy(download_dir + filename, verifiedrar_dir)
        if doloadpar2vols:
            mp_outqueue.put((doloadpar2vols, -9999))
        # if all checked is there still a fill which is not ok??
        if len(rarf) == maxrar and len(rarlist) == maxrar:
            allok = True
            for (filename, isok) in rarlist:
                if isok != 1:
                    allok = False
                    break
            verifiedstatus = 1
            if not allok:
                verifiedstatus = -1
            break
        time.sleep(0.5)

    logger.info("PAR_VERIFIER > rar verify status is " + str(verifiedstatus))

    # phase II: repair
    if corruptrars or verifiedstatus == -1:
        logger.info("PAR_VERIFIER > Starting repair")
        rf = [(r, re.search(r"[.]par2$", r.split("/")[-1], flags=re.IGNORECASE)) for r in glob.glob(download_dir + "*")]
        rf_par2 = [r for r, rs in rf if rs is not None]
        if rf_par2:
            rf_0 = [(r, re.search(r"vol[0-9][0-9]*[+]", r, flags=re.IGNORECASE)) for r in rf_par2]
            rf_par2vol = [r for r, rs in rf_0 if rs is not None]
            if rf_par2vol:
                logger.info("PAR_VERIFIER > par2vol files present, repairing ...")
                res0 = multipartrar_repair(download_dir, rf_par2vol[0], logger)
                if res0 == 1:
                    logger.info("PAR_VERIFIER > repair success")
                    # copy all no yet copied rars to verifiedrar_dir
                    rf_0 = [(r, re.search(r"[.]rar$", r, flags=re.IGNORECASE)) for r in glob.glob(download_dir + "*")]
                    rars_in_downloaddir = [r.split("/")[-1] for r, rs in rf_0 if rs is not None]
                    files_in_verifieddir = [r.split("/")[-1] for r in glob.glob(verifiedrar_dir + "*")]
                    for filename in rars_in_downloaddir:
                        if filename not in files_in_verifieddir:
                            logger.info("PAR_VERIFIER > copying " + filename + " to verifiedrar_dir")
                            shutil.copy(download_dir + filename, verifiedrar_dir)
                        # delete all rars in download_dir
                    verifiedstatus = 1
                else:
                    logger.error("PAR_VERIFIER > repair failed!")
            else:
                logger.warning("PAR_VERIFIER > No par2vol files present, cannot repair!")
                verifiedstatus = -1
        else:
            logger.warning("PAR_VERIFIER > no par files exist!")
            verifiedstatus = -1
    else:
        logger.info("PAR_VERIFIER > All files ok, no repair needed!")
        verifiedstatus = 1

    logger.warning("PAR_VERIFIER > Exiting par_verifier!")
    mp_outqueue.put((-9999, verifiedstatus))


def multipartrar_repair(directory, parvolname, logger):
    cwd0 = os.getcwd()
    os.chdir(directory)
    logger.warning("MULTIRAR_REPAIR > checking if repair possible ...")
    ssh = subprocess.Popen(['par2verify', parvolname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
    sshres = ssh.stdout.readlines()
    repair_is_required = False
    repair_is_possible = False
    exitstatus = 0
    for ss in sshres:
        ss0 = ss.decode("utf-8")
        if "Repair is required" in ss0:
            repair_is_required = True
        if "Repair is possible" in ss0:
            repair_is_possible = True
    if repair_is_possible and repair_is_required:
        logger.info("MULTIRAR_REPAIR > Repair is required and possible, performing par2repair ...")
        # repair
        ssh = subprocess.Popen(['par2repair', parvolname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
        sshres = ssh.stdout.readlines()
        repair_complete = False
        for ss in sshres:
            ss0 = ss.decode("utf-8")
            if "Repair complete" in ss0:
                repair_complete = True
        if not repair_complete:
            exitstatus = -1
            logger.error("MULTIRAR_REPAIR > Could not repair")
        else:
            logger.info("MULTIRAR_REPAIR > Repair success!!")
            exitstatus = 1
    elif repair_is_required and not repair_is_possible:
        logger.error("MULTIRAR_REPAIR > Repair is required but not possible!")
        exitstatus = -1
    elif not repair_is_required and not repair_is_possible:
        logger.error("MULTIRAR_REPAIR > Repair is not required - all OK!")
        exitstatus = 1
    os.chdir(cwd0)
    return exitstatus


def multipartrar_test(directory, rarname0, logger):
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
        # print(-1)
        return -1              # -1 cannot check, rar in between is missing
    # ok sorted, unrar t
    cmd = "unrar t " + rarname0
    child = pexpect.spawn(cmd)
    str0 = []
    str00 = ""
    status = 1

    while True:
        try:
            a = child.read_nonblocking().decode("utf-8")
            if a == "\n":
                if str00:
                    str0.append(str00)
                    str00 = ""
            if ord(a) < 32:
                continue
            str00 += a
        except pexpect.exceptions.EOF:
            break
    # logger.info("MULTIPARTRAR_TEST > " + str(str0))
    for i, s in enumerate(str0):
        if rarname0 in s:
            try:
                if "- checksum error" in str0[i + 2]:
                    status = -2
                if "Cannot find" in s:
                    status = -1
            except Exception as e:
                logger.info("MULTIPARTRAR_TEST > " + str(e))
                status = -1

    os.chdir(cwd0)
    return status


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
        gg_rar = re.search(r"\S*[.]rar", str0, flags=re.IGNORECASE)
        if gg_rar:
            rar_events.append((gg_rar.group(), flgs0))
    return rar_events


def get_rar_files(directory):
    rarlist = []
    for rarf in glob.glob("*.rar"):
        gg = re.search(r"[0-9]+[.]rar", rarf, flags=re.IGNORECASE)
        rarlist.append((int(gg.group().split(".")[0]), rarf))
    return rarlist


def partial_unrar(unrarqueue, directory, unpack_dir, logger):
    cwd0 = os.getcwd()
    os.chdir(directory)
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
    logger.info("PARTIAL_UNRAR > executing 'unrar x -y -o+ -vp'")
    cmd = "unrar x -y -o+ -vp " + rar_sortedlist[0][1] + " " + unpack_dir
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
            break
        if "All OK" in str0:
            statmsg = "All OK"
            status = 0
            break
        # percstr = re.findall(r" [0-9]+%", str0)[-1]
        # print(str0)
        # print(str0)
        # print("-" * 16)
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
    # print("100% - done")
    logger.info("PARTIAL_UNRAR > " + str(status) + " " + statmsg)
    os.chdir(cwd0)
    unrarqueue.put((status, statmsg))


def renamer(directory, logger):
    cwd0 = os.getcwd()
    # logger.debug("Starting renamer ...")
    os.chdir(directory)

    # rename par2files
    par2list = []
    for p in glob.glob("*"):
        pname = p.split("/")[-1]
        with open(pname, "rb") as f:
            content = f.read()
            length = len(content)
            f.seek(length - 50)
            bstr0 = b"PAR 2.0\0Creator\0"
            lastcontent = f.read(50)
            if bstr0 in lastcontent:
                par2list.append((pname, length))
    if not par2list:
        return -1, None
    p2files = sorted(par2list, key=lambda length: length[1])
    par2basename = p2files[0][0].split(".")[0]
    p2files = [p2 for p2, p2l in p2files]
    try:
        par2name = par2basename + ".par2"
        # os.rename(p2files[0], par2basename + ".par2")
        p2obj = Par2File(par2name)
    except Exception as e:
        # logger.exception("RENAMER > " + str(e))
        return -1, None
    for i, p2 in enumerate(p2files):
        if i == 0:
            continue
        try:
            aa = 1
            # os.rename(p2files[i], par2basename + ".vol" + str(i).zfill(3) + "+001.PAR2")
        except Exception as e:
            # logger.exception("RENAMER > " + str(e))
            return -1, None

    # rename rar files
    if not p2obj:
        return -1
    rarfileslist = p2obj.md5_16khash()
    rarfileslist0 = [r for r, hash in rarfileslist]
    allfilelist = []
    for r in glob.glob("*"):
        rshort = r.split("/")[-1]
        allfilelist.append((rshort, calc_file_md5hash_16k(r)))
    for a_name, a_md5 in allfilelist:
        try:
            r_name = [fn for fn, r_md5 in rarfileslist if r_md5 == a_md5][0]
            if r_name != a_name:
                print("Renaming " + a_name + " to " + r_name)
                os.rename(a_name, r_name)
            rarfileslist0.remove(r_name)
        except IndexError:
            pass
        except Exception as e:
            print(str(e))

    if rarfileslist0:
        print("something happened")

    # todo:
    #     for a in allfilelist:
    #         if file mit Kennung "RAR", aber ohne match --> was tun?

    os.chdir(cwd0)
    return 1, par2basename + ".par2"


renamer("/home/stephan/.ginzibix/incomplete/st502304a4df4c023adf43c1462a.nfo/_downloaded0", None)
