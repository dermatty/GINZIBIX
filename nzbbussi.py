import nntplib
import xml.etree.ElementTree as ET
import time
import yenc
from threading import Thread
import glob
import ssl
import posix_ipc
import configparser
import subprocess
import os
from os.path import expanduser
import sys

USERHOME = expanduser("~")
MAIN_DIR = USERHOME + "/.nzbbussi/"
CONFIG_DIR = MAIN_DIR + "config/"
NZB_DIR = MAIN_DIR + "nzb/"
COMPLETE_DIR = MAIN_DIR + "complete/"
INCOMPLETE_DIR = MAIN_DIR + "incomplete/"
LOGS_DIR = MAIN_DIR + "logs/"

# python 3 unrar
# https://pypi.python.org/pypi/unrar/
# https://github.com/markokr/rarfile


SEMAPHORE = posix_ipc.Semaphore("news_sema", posix_ipc.O_CREAT)
SEMAPHORE.release()

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
        global COMPLETE_DIR
        for header, filelist in self.ITEMS:
            nr_articles = int(filelist[0])
            articlesprocessed = []
            byt = bytearray()
            for f in filelist:
                if f == filelist[0]:
                    continue
                if f[1] not in articlesprocessed:
                    print(str(self.THREADNO) + " > File:" + header + " --> downloading article #" + str(f[1]) + " of " + str(nr_articles) + ": " + f[0])
                    resp, info = self.SERVER.body(f[0])
                    resp_h, info_h = self.SERVER.body(f[0])

                    # todo: if 2 articles with same number --> check if both identical, else: error

                    # Ednuerf Fneuf-1-Pittis AVCHD1080p fuer DVD5.Ger.part025.rar&quot; (27/99) yEnc (1/136)
                    # <segment bytes="398458" number="27">1Ph1PL9Er40A8WQkste2_27o99@JBinUp.local</segment>
                    # <segment bytes="398380" number="27">QrivyXCU7lV26U7sqtW9_27o99@JBinUp.local</segment>
                    # '<1Ph1PL9Er40A8WQkste2_27o99@JBinUp.local>'
                    # '<QrivyXCU7lV26U7sqtW9_27o99@JBinUp.local>'

                    '''cat '<1Ph1PL9Er40A8WQkste2_27o99@JBinUp.local>'
                    =ybegin part=27 total=134 line=128 size=51200000 name=Ednuerf Fneuf-1-Pittis AVCHD1080p fuer DVD5.Ger.part025.rar
                    =ypart begin=9984001 end=10368000
                    =yend size=384000 part=27 pcrc32=7d2799b9
                    check crc32: 7d2799b9
                    check length: 384000'''

                    '''cat '<QrivyXCU7lV26U7sqtW9_27o99@JBinUp.local>'
                    =ybegin part=27 total=134 line=128 size=51200000 name=Ednuerf Fneuf-1-Pittis AVCHD1080p fuer DVD5.Ger.part025.rar
                    =ypart begin=9984001 end=10368000
                    =yend size=384000 part=27 pcrc32=7d2799b9
                    check crc32: 7d2799b9
                    check length: 384000'''

                    '''byt1 = bytearray()
                    with open(COMPLETE_DIR + "articles/" + f[0], "w") as ff1:
                        for inf in info_h.lines:
                            try:
                                inf0 = inf.decode()
                                ff1.write(inf0 + "\n")
                            except:
                                byt1.extend(inf)
                        length, crc32, decoded = yenc.decode(byt1)
                        ff1.write("check crc32: " + str(crc32) + "\n")
                        ff1.write("check length: " + str(length) + "\n")
                        ff1.close()'''

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
                    if headerfound != 1 or endfound != 1 or partfound > 1:
                        byt = byt0
                        continue
                    articlesprocessed.append(f[1])
            length, crc32, decoded = yenc.decode(byt)
            f0 = open(COMPLETE_DIR + header, "wb")
            f0.write(decoded)
            print(header + " saved!!")
            f0.flush()
            f0.close()
        self.SERVER.quit()


if __name__ == "__main__":

    CFG = configparser.ConfigParser()
    CFG.read(CONFIG_DIR + "/nzbbussi.config")

    # only 1 server atm hardcoded
    SERVER_NAME = CFG["SERVER1"]["SERVER_NAME"]
    SERVER = CFG["SERVER1"]["SERVER_URL"]
    USER = CFG["SERVER1"]["USER"]
    PASSWORD = CFG["SERVER1"]["PASSWORD"]
    PORT = int(CFG["SERVER1"]["PORT"])
    SSL = True if CFG["SERVER1"]["SSL"].lower() == "yes" else False
    LEVEL = int(CFG["SERVER1"]["LEVEL"])
    CONNECTIONS = int(CFG["SERVER1"]["CONNECTIONS"])

    # get nzbs -> atm only pls only 1 nzb in nzb dir!
    os.chdir(NZB_DIR)
    print("Getting NZB files from " + NZB_DIR)
    for NZB in glob.glob("*.nzb"):
        pass
    try:
        tree = ET.parse(NZB)
        print("Downloading " + NZB)
    except Exception as e:
        print(str(e) + ": please provide at least 1 NZB file, exiting!")
        sys.exit()
    root = tree.getroot()
    os.chdir(MAIN_DIR)

    connections = CONNECTIONS

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

    threadlist = []
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
        th.join()

    print("par repair:")

    # PAR-REPAIR
    os.chdir(COMPLETE_DIR)
    ssh = subprocess.Popen(["/usr/bin/par2verify", PAR2FILE], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # ssh = subprocess.Popen(["ls", "-al"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sshres = ssh.stdout.readlines()
    answer = sshres[-1].decode("utf-8")
    if answer[0:21] == "All files are correct":
        print("No par repair required")
    else:
        print("par2 repair required")
        # TO DO: only download all par2 files if we are here
        ssh = subprocess.Popen(["/usr/bin/par2repair", PAR2FILE], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sshres = ssh.stdout.readlines()
        answer = sshres[-1].decode("utf-8")
        if answer[0:15] == "Repair complete":
            print("Repair complete!")
        else:
            print("Cannot repair!")
    os.chdir(MAIN_DIR)
    # UNRAR

    # DELETE ALL INTERIM FILES
