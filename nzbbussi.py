import nntplib
import xml.etree.ElementTree as ET
import time
import yenc
from threading import Thread
import sys
import ssl
import posix_ipc
import configparser
import subprocess
import os

# python 3 unrar
# https://pypi.python.org/pypi/unrar/
# https://github.com/markokr/rarfile

# NZB = "saltatio-mortis.nzb"
# NZB = "Vorstadtweiber.nzb"
# NZB = "Fuenf.Freunde.1.nzb"
NZB = "nzb/Der.Gloeckner.von.Notre.nzb"

SEMAPHORE = posix_ipc.Semaphore("news_sema", posix_ipc.O_CREAT)
SEMAPHORE.release()
# SEMAPHORE.acquire

BYTES_WRITTEN = 0
DTIME = 0
AVGBYTES = 0


def writer(nobyt):
    global SEMAPHORE
    global DTIME
    global BYTES_WRITTEN
    global AVGBYTES
    SEMAPHORE.acquire()
    if DTIME == 0:
        DTIME = time.time()
    BYTES_WRITTEN += nobyt
    AVGBYTES = BYTES_WRITTEN / (time.time() - DTIME)
    SEMAPHORE.release()


class Download(Thread):

    def __init__(self, threadno, items):
        Thread.__init__(self)
        global USER
        global SERVER
        global PASSWORD
        global PORT
        self.ITEMS = items
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        self.THREADNO = threadno
        self.SERVER = nntplib.NNTP_SSL(SERVER, user=USER, password=PASSWORD, ssl_context=self.context, port=PORT, readermode=True)

    def run(self):
        global BYTES_WRITTEN
        for header, filelist in self.ITEMS:
            # nr_articles = len(filelist)
            '''byt = bytearray()
            nr = 1
            for f in filelist:
                if nr > int(filelist[0]):
                    break
                if f == filelist[0]:
                    continue
                print(str(self.THREADNO) + " > File:" + header + " --> downloading article #" + str(f[1]) + " of " + filelist[0] + ": " + f[0])
                resp, info = self.SERVER.body(f[0])
                del info.lines[-1]
                del info.lines[0:2]
                for inf in info.lines:
                    byt.extend(inf)
                nr += 1'''
            nr_articles = int(filelist[0])
            articlesprocessed = []
            byt = bytearray()
            for f in filelist:
                if f == filelist[0]:
                    continue
                if f[1] not in articlesprocessed:
                    print(str(self.THREADNO) + " > File:" + header + " --> downloading article #" + str(f[1]) + " of " + str(nr_articles) + ": " + f[0])
                    resp, info = self.SERVER.body(f[0])
                    headerfound = 0
                    endfound = 0
                    partfound = 0
                    # check article integrity
                    # todo: check more thoroughly!! see:
                    #          http://www.yenc.org/yenc-draft.1.3.txt
                    byt0 = byt
                    for inf in info.lines:
                        try:
                            inf0 = inf.decode()
                            if inf0 == "":
                                continue
                            if inf0[:7] == "=ybegin":
                                headerfound += 1
                                continue
                            if inf0[:5] == "=yend":
                                endfound += 1
                                continue
                            if inf0[:6] == "=ypart":
                                partfound += 1
                                continue
                        except Exception as e:
                            pass
                        byt.extend(inf)
                    #print(info.lines[0])
                    #print(info.lines[1])
                    #print(info.lines[2])
                    #print(info.lines[3])
                    #print(headerfound, endfound)
                    if headerfound != 1 or endfound != 1 or partfound > 1:
                        byt = byt0
                        continue
                    articlesprocessed.append(f[1])
                    # del info.lines[-1]
                    # del info.lines[0:2]
                    #for inf in info.lines:
                    #    byt.extend(inf)
            length, crc32, decoded = yenc.decode(byt)
            f0 = open("download/" + header, "wb")
            f0.write(decoded)
            print(header + " saved!!")
            f0.flush()
            f0.close()
        self.SERVER.quit()


if __name__ == "__main__":

    nzbbussicfg = configparser.ConfigParser()
    nzbbussicfg.read("config/nzbbussi.config")
    SERVER = nzbbussicfg["SERVERS"]["SERVER"]
    USER = nzbbussicfg["SERVERS"]["USER"]
    PASSWORD = nzbbussicfg["SERVERS"]["PASSWORD"]
    PORT = int(nzbbussicfg["SERVERS"]["PORT"])

    tree = ET.parse(NZB)
    root = tree.getroot()

    connections = 10

    server = nntplib.NNTP_SSL(SERVER, user=USER, password=PASSWORD, port=PORT, readermode=True)

    filedic = {}
    threads = []
    SIZE = 0
    PAR2FILE = None
    for r in root:
        headers = r.attrib
        try:
            poster = headers["poster"]
            date = headers["date"]
            subject = headers["subject"]
            print(subject)
            hn_list = subject.split('"')
            hn = hn_list[1]
            an = hn_list[0]
            if PAR2FILE is None and hn[-5:].lower() == ".par2":
                PAR2FILE = hn
        except Exception as e:
            continue
        for s in r:
            i = 1
            byt = b''
            filelist = []
            segfound = True
            for r0 in s:
                if r0.tag[-5:] == "group":
                    segfound = False
                    continue
                nr0 = r0.attrib["number"]
                filename = "<" + r0.text + ">"
                filelist.append((filename, int(nr0)))
                i += 1
            i -= 1
            if segfound:
                # print(nr0)
                # print(hn, len(filelist))
                filelist.insert(0, nr0)
                # print(filelist)
                #print("-" * 80)
                filedic[hn] = filelist
    
    '''threadlist = []
    for co in range(connections):
        threadlist.append([])
    sn = 0
    for key, item in filedic.items():
        threadlist[sn].append((key, item))
        sn += 1
        if sn >= connections:
            sn = 0
    sn = 0
    threads = []
    print
    for t in threadlist:
        th = Download(sn, t)
        threads.append(th)
        sn += 1

    for th in threads:
        print("Starting Thread")
        th.start()

    for th in threads:
        th.join()'''

    print("par repair:")

    # PAR-REPAIR
    os.chdir("download/")
    ssh = subprocess.Popen(["/usr/bin/par2verify", PAR2FILE], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # ssh = subprocess.Popen(["ls", "-al"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sshres = ssh.stdout.readlines()
    answer = sshres[-1].decode("utf-8")
    if answer[0:21] == "All files are correct":
        print("No par repair required")
    else:
        print("par2 repair required")
        ssh = subprocess.Popen(["/usr/bin/par2repair", PAR2FILE], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sshres = ssh.stdout.readlines()
        for ssh0 in sshres:
            print(ssh0)

    # UNRAR
