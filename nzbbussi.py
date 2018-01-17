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

FAILED_ARTICLE_LIST = []
# 0 = started, 1 = Full Success, 2 = Articles missing, -1 = finally no success -> repair useless, too many articles missing
ACTIVE_THREADS_STATUS = []

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


def set_global_active_thread_status(nr, status):
    global ACTIVE_THREADS_STATUS
    SEMAPHORE.acquire()
    ACTIVE_THREADS_STATUS[nr] = (status)
    SEMAPHORE.release()


def append_global_failed_articels(failedarticles):
    global FAILED_ARTICLE_LIST
    SEMAPHORE.acquire()
    FAILED_ARTICLE_LIST.append(failedarticles)
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

    # downloads an article, returns list of data
    def download_article(self, articles):
        infolist = []
        for art in articles:
            print("Downloading article #" + str(art[1]) + ": " + art[0])
            try:
                resp_h, info_h = self.SERVER.head(art[0])  # 221 = article retrieved head follows: 222 0 <rocWBjTgD4RpOrSMspot_6o99@JBinUp.local>
                resp, info = self.SERVER.body(art[0])      # 222 = article retrieved head follows: 221 0 <TDG36IMQlKcVW5NHovr11_10o99@JBinUp.local>
                if resp_h[:3] != "221" or resp[:3] != "222":
                    continue
            except Exception as e:
                print("Article download error: " + str(e))
                continue
            infolist.append(info)
            break
        return infolist

    # yenc decode infolist from article(s)
    def decode_articles(self, infolist):
        headerfound = 0
        endfound = 0
        partfound = 0
        byt = bytearray()
        pcrc32 = ""
        for info in infolist:
            for inf in info.lines:
                try:
                    inf0 = inf.decode()
                    if inf0 == "":
                            continue
                    if inf0[:7] == "=ybegin":
                        headerfound += 1
                        continue
                    if inf0[:5] == "=yend":
                        pcrc32 = inf0.split("pcrc32=")[1]
                        endfound += 1
                        continue
                    if inf0[:6] == "=ypart":
                        partfound += 1
                        continue
                except Exception as e:
                    pass
                byt.extend(inf)
            if headerfound != 1 or endfound != 1 or partfound > 1:
                print("Wrong yenc structure detected")
                byt.clear()
                continue           # try next article if provided
            else:
                break              # all fine, return first article as bytearray + crc32
        return byt, pcrc32

    def run(self):
        global BYTES_WRITTEN, COMPLETE_DIR
        failedarticles = []
        # loop over whole (.rar) file
        success = False
        for header, articlelist_ext in self.ITEMS:
            nr_articles = int(articlelist_ext[0])
            articlelist = articlelist_ext[1:]
            byt = bytearray()
            # downlaod all articles
            for artnr in range(nr_articles):
                articles_for_download = [f for f in articlelist if f[1] == artnr + 1]
                # if article no. not found in articlelist -> failedarticles
                if articles_for_download == []:
                    print("Article #" + str(artnr+1) + " is missing in NZB file")
                    failedarticles.append((header, artnr + 1, nr_articles))
                    continue
                infolist = self.download_article(articles_for_download)
                # if cannot download article -> failedarticles
                if infolist == []:
                    print("Download of Article failed")
                    failedarticles.append((header, artnr + 1, nr_articles))
                    continue
                # all ok until now, decode it!
                article_bytes, pcrc32 = self.decode_articles(infolist)
                if pcrc32 == "":
                    failedarticles.append((header, artnr + 1, nr_articles))
                    print("Could not decode article!")
                    continue
                # check crc32
                _, crc32, decoded = yenc.decode(article_bytes)
                if crc32.strip("0") != pcrc32.strip("0"):
                    failedarticles.append((header, artnr + 1, nr_articles))
                    print("CRC32 checksum error: " + crc32 + " / " + pcrc32)
                    continue
                # all ok, add bytes from article to file
                byt.extend(decoded)
            with open(COMPLETE_DIR + header, "wb") as f0:
                f0.write(byt)
                f0.flush()
                f0.close()
                print(header + " saved!!")
                success = True
        append_global_failed_articels(failedarticles)
        if success:
            set_global_active_thread_status(self.THREADNO, 1)
        else:
            set_global_active_thread_status(self.THREADNO, 2)
        self.SERVER.quit()

        '''
         =ybegin part=27 total=134 line=128 size=51200000 name=Ednuerf Fneuf-1-Pittis AVCHD1080p fuer DVD5.Ger.part025.rar
         =ypart begin=9984001 end=10368000
         =yend size=384000 part=27 pcrc32=7d2799b9
         check crc32: 7d2799b9
         check length: 384000
        '''


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
        ACTIVE_THREADS_STATUS.append(0)
        print("Starting Thread")
        th.start()

    # 1 = full success
    # -1 = par2 repair makes sense
    # -2 = par2 repair makes NO sense
    downloadstatus = 0
    while True:
        # Download ended with "Article missing" -> try to get from fill server, if any
        # if no fill server -> check if too much articels are missing
        # 0 = started
        # 1 = Full Success
        # 2 = Articles missing, start thread for alternative download or check if par2 makes sense
        # -1 = cannot redownload missing articles, but par2 repair makes sense
        # -2 = cannot redownload missing articles and par2 repair makes NO sense
        if 2 in ACTIVE_THREADS_STATUS:
            # try to redownload, from other server. if no server left, check if repair is useless
            pass
        # if status for all threads is only "success" or "no success": exit loop
        if 0 not in ACTIVE_THREADS_STATUS and 2 not in ACTIVE_THREADS_STATUS:
            if -1 not in ACTIVE_THREADS_STATUS and -2 not in ACTIVE_THREADS_STATUS:
                downloadstatus = 1
            break
        time.sleep(1)

    sys.exit()
        
    for th in threads:
        print("------------------------")
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
