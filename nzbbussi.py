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

SERVER_CONFIGS = []
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


'''
class ArticleDownloader():

    def __init__(self, threadno, items, serverlist):
        global USER
        global SERVER
        global PASSWORD
        global PORT
        self.ITEMS = items
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        self.THREADNO = threadno
        self.SERVERLIST = serverlist
        self.SERVER = []
        # (server_name, server, user, password, port, usessl, level, connections, 0))
        for s in self.SERVERLIST:
            server_name, server, user, password, port, usessl, level, connections, threads_active = s
            if usessl:
                self.SERVER.append(nntplib.NNTP_SSL(server, user=user, password=password, ssl_context=self.context, port=port, readermode=True))
            else:
                self.SERVER.append(nntplib.NNTP(server, user=user, password=password, ssl_context=self.context, port=port, readermode=True))

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
'''
         =ybegin part=27 total=134 line=128 size=51200000 name=Ednuerf Fneuf-1-Pittis AVCHD1080p fuer DVD5.Ger.part025.rar
         =ypart begin=9984001 end=10368000
         =yend size=384000 part=27 pcrc32=7d2799b9
         check crc32: 7d2799b9
         check length: 384000
'''


def modify_articlelist(idx, item):
        global ARTICLELIST
        SEMAPHORE.acquire()
        ARTICLELIST[idx] = item
        SEMAPHORE.release()


class ArticleDownload(Thread):

    def __init__(self, art_nr, art_name, server, sidx, servers):
        Thread.__init__(self)
        global ARTICLELIST
        self.daemon = True
        self.art_nr = art_nr
        self.art_name = art_name
        self.server = server
        self.serverindex = sidx
        self.servers = servers
        # ARTICLELIST = [(art_nr, art_name, age, status (-1 default), unused_servers, content)]
        self.article = [(idx, art_nr0, art_name0, age0, status0, unused_servers0, content)
                        for idx, (art_nr0, art_name0, age0, status0, unused_servers0, content) in enumerate(ARTICLELIST)
                        if art_nr == self.art_nr and art_name0 == self.art_name][0]
        self.articleindex, _, _, self.age, self.status, self.unused_servers, _ = self.article

    def get_article(self):
        print("Downloading article #" + str(self.art_nr) + ": " + self.art_name)
        try:
            print(">>>", self.servers.server_threadlist)
            resp_h, info_h = self.server.head(self.art_name)  # 221 = article retrieved head follows: 222 0 <rocWBjTgD4RpOrSMspot_6o99@JBinUp.local>
            resp, info = self.server.body(self.art_name)      # 222 = article retrieved head follows: 221 0 <TDG36IMQlKcVW5NHovr11_10o99@JBinUp.local>
            if resp_h[:3] != "221" or resp[:3] != "222":
                raise("No Succes answer from server!")
        except Exception as e:
            print("Article download error: " + str(e))
            return False, None
        return True, info

    def run(self):
        server, nr_active, maxconn, retention, level = self.servers.server_threadlist[self.serverindex]
        self.servers.modify_serverthreadlist(self.serverindex, (server, nr_active + 1, maxconn, retention, level))
        unused_new = self.unused_servers[:]
        unused_new.remove(self.server)
        modify_articlelist(self.articleindex, (self.art_nr, self.art_name, self.age, 1, unused_new, None))

        success, info = self.get_article()
        # ARTICLELIST = [(art_nr, art_name, age, status, unused_servers, content)]
        # status = -2 failed
        #          -1 queued
        #          0 success
        #          1 downloading
        if success:
            # [(art_nr, art_name, age, status, unused_servers, content)]
            modify_articlelist(self.articleindex, (self.art_nr, self.art_name, self.age, 0, unused_new, info))
        elif not success and not unused_new:
            modify_articlelist(self.articleindex, (self.art_nr, self.art_name, self.age, -2, unused_new, info))
        elif not success and unused_new:
            modify_articlelist(self.articleindex, (self.art_nr, self.art_name, self.age, -1, unused_new, info))
        self.servers.modify_serverthreadlist(self.serverindex, (self.server, nr_active, maxconn, retention, level))
        return


class Downloader():
    def __init__(self, servers):
        self.servers = servers

    def get_next_article_from_articlelist(self):
        global ARTICLELIST, SEMAPHORE
        SEMAPHORE.acquire()
        res0 = False, False
        for i, art in enumerate(ARTICLELIST):
            art_nr, art_name, age, status, unused_servers, content = art
            if status == -1 and unused_servers:
                res0 = True, (art_nr, art_name, age, unused_servers, content)
                modify_articlelist(i, (art_nr, art_name, age, 1, unused_servers, content))
                break
            if status == -1 and not unused_servers:
                print("*** dead ***")
                modify_articlelist(i, (art_nr, art_name, age, -2, [], None))
            if status == 1:
                res0 = False, True
        SEMAPHORE.release()
        return res0

    def get_free_server_from_server_threadlist(self, age, unused_servers):
        t0 = time.time()
        min_server = None
        # (server_name, connection_list[(nntplib_obj, is_busy)], retention, level)
        # print("-----", unused_servers)
        while not min_server and time.time() - t0 < 1:            # max 1 sekunde suchen
            suited_servers = [(s0, connlist, level) for s0, no_active, connlist, retention, level in self.servers.server_threadlist
                              if s0 in unused_servers and retention > age * 0.9]
            for s_name, s_connlist, s_level in suited_servers:
                for nntpob, is_busy in s_connlist:
                    if not is_busy:
                        
            if not suited_servers:
                continue
            min_server = None
            min_level = 10000
            for s, level in suited_servers:
                if level < min_level:
                    min_level = level
                    min_server = s
            if min_server:
                idx = [idx0 for idx0, (s0, no_active, maxconn, retention, level) in enumerate(self.servers.server_threadlist) if min_server == s0][0]
                return min_server, idx
        return min_server, -1

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

    def download(self):
        global ARTICLELIST
        # ARTICLELIST = [(art_nr, art_name, age, status, unused_servers, content)]
        # status = -1 queued
        #          0 success
        #          1 failed
        #          -2 downloading
        for i, art0 in enumerate(ARTICLELIST):
            art_nr, art_name, age, status, _, _ = art0
            modify_articlelist(i, (art_nr, art_name, age, status, self.servers.unused_serverlist, None))

        stillrunning = True
        while stillrunning:
            res0, stillrunning = self.get_next_article_from_articlelist()  # get article with status 0 oder -1
            if not res0 and not stillrunning:
                break            # fertisch!!!!
            if not res0 and stillrunning:
                continue
            artnr, art_name, age, unused_servers, _ = stillrunning
            print("ART", artnr)
            s0, sidx = self.get_free_server_from_server_threadlist(age, unused_servers)
            print(s0, sidx)
            if s0:
                ad = ArticleDownload(artnr, art_name, s0, sidx, self.servers)
                ad.start()
            else:
                print("Somehting bad happened!")
                sys.exit()
            time.sleep(0.1)

        print("All articles downloaded where possible")

        lenarticlelist = len(ARTICLELIST)
        articles_status = [status for _, _, _, status, _, _ in ARTICLELIST]
        print(articles_status)
        nr_okarticles = len([status for status in articles_status if status == 0])
        print(lenarticlelist, nr_okarticles)
        if nr_okarticles == lenarticlelist:
            # postprocess articles
            infolist = []
            for i in range(len(ARTICLELIST)):
                for art_nr, _, _, _, _, info in ARTICLELIST:
                    if art_nr - 1 == i:
                        infolist.append(info)
            bytes0 = bytearray()
            stat_decode = 0
            for info in infolist:
                bytesresult, pcrc32 = self.decode_articles(infolist)
                if pcrc32 == "":
                    print("CRC32 checksum missing!")
                    stat_decode = -1
                    # continue
                # check crc32
                _, crc32, decoded = yenc.decode(bytesresult)
                if crc32.strip("0") != pcrc32.strip("0"):
                    print("CRC32 checksum error: " + crc32 + " / " + pcrc32)
                    stat_decode = -1
                # all ok, add bytes from article to file
                bytes0.extend(decoded)
            return stat_decode, bytes0
        else:
            return -2, None

        '''nr_damagedarticles = len([status for status in articles_status if status == -2])
        if nr_damagedarticles / lenarticlelist > 0.03:
            return -1           # >= 3% (??) par repair useless -> return -1 ---> ausprobieren
        else:
            return 1            # < 3% par repair sinnvoll: return 1 (means par2 files nachladen und repair versuchen)
        '''


class Servers():

    def __init__(self, cfg):
        self.cfg = cfg
        self.server_config = self.get_server_config(self.cfg)
        self.server_threadlist, self.unused_serverlist = self.connect()

        # self.server_threadlist
        # (server_name, connection_list[(nntplib_obj, is_busy)], retention, level)

    def modify_serverthreadlist(self, idx, item):
        SEMAPHORE.acquire()
        self.server_threadlist[idx] = item
        SEMAPHORE.release()

    def connect(self):
        serverthreads = []
        unused = []
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        for s in self.server_config:
            server_name, server, user, password, port, usessl, level, max_connections, retention, threads_active = s
            connlist = []
            for mc in max_connections:
                try:
                    if usessl:
                        server0 = nntplib.NNTP_SSL(server, user=user, password=password, ssl_context=context, port=port, readermode=True)
                    else:
                        server0 = nntplib.NNTP(server, user=user, password=password, ssl_context=context, port=port, readermode=True)
                    connlist.append((server0, False))
                except Exception as e:
                    pass
            if connlist:
                unused.append(server_name)
                serverthreads.append((server_name, connlist, retention, level))
            else:
                print("Cannot connect to server " + server_name + ", skipping ...")
        return serverthreads, unused

    def get_server_config(self, cfg):
        # get servers from config
        snr = 0
        sconf = []
        while True:
            try:
                snr += 1
                snrstr = "SERVER" + str(snr)
                server_name = cfg[snrstr]["SERVER_NAME"]
                server = cfg[snrstr]["SERVER_URL"]
                user = cfg[snrstr]["USER"]
                password = cfg[snrstr]["PASSWORD"]
                port = int(cfg[snrstr]["PORT"])
                usessl = True if cfg[snrstr]["SSL"].lower() == "yes" else False
                level = int(cfg[snrstr]["LEVEL"])
                connections = int(cfg[snrstr]["CONNECTIONS"])
            except Exception as e:
                snr -= 1
                break
            try:
                retention = int(cfg[snrstr]["RETENTION"])
                sconf.append((server_name, server, user, password, port, usessl, level, connections, retention, 0))
            except Exception as e:
                sconf.append((server_name, server, user, password, port, usessl, level, connections, 999999, 0))
        if not sconf:
            return None
        return sconf


# Parse NZB xml
def ParseNZB(nzbroot):
    filedic = {}
    for r in nzbroot:
        headers = r.attrib
        try:
            # poster = headers["poster"]
            date = headers["date"]
            age = int((time.time() - float(date))/(24 * 3600))
            subject = headers["subject"]
            # print(subject)
            hn_list = subject.split('"')
            hn = hn_list[1]
            # an = hn_list[0]
            filetype = hn.split(".")[-1]
            if filetype.lower() == "par2":
                if hn.split(".")[-2][:3] == "vol":
                    filetype = "PAR2"
            # if PAR2FILE is None and hn[-5:].lower() == ".par2":
            #     PAR2FILE = hn
        except Exception as e:
            continue
        for s in r:
            filelist = []
            segfound = True
            for i, r0 in enumerate(s):
                if r0.tag[-5:] == "group":
                    segfound = False
                    continue
                nr0 = r0.attrib["number"]
                filename = "<" + r0.text + ">"
                filelist.append((filename, int(nr0)))
            i -= 1
            if segfound:
                filelist.insert(0, (age, filetype, int(nr0)))
                filedic[hn] = filelist
    return filedic


if __name__ == "__main__":

    CFG = configparser.ConfigParser()
    CFG.read(CONFIG_DIR + "/nzbbussi.config")

    # get servers
    servers = Servers(CFG)
    if not servers:
        print("At least one server has to be provided, exiting!")
        sys.exit()

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
    nzb_root = tree.getroot()
    os.chdir(MAIN_DIR)

    filedic = ParseNZB(nzb_root)

    servers.connect()
    j = 0
    for key, filelist in filedic.items():
        ARTICLELIST = []
        SERVER_THREAD_LIST = []
        for i, f in enumerate(filelist):
            if i == 0:
                age, filetype, nr_articles = f
            else:
                fn, nr = f
                ARTICLELIST.append((nr, fn, age, -1, [], None))
        d = Downloader(servers)
        print("Downloading File: " + key)
        result, binaryfile = d.download()
        # result = 0:   all articles downloaded & checksums ok -> no par repair necessary
        #        =-1:   all articles downloaded but checksums not ok -> par check nec., download par2 files
        #        =-2:   failed, articles missing, stop!
        if result != -2:
            print(INCOMPLETE_DIR + key)
            with open(INCOMPLETE_DIR + key, "wb") as f0:
                f0.write(binaryfile)
                f0.flush()
                f0.close()
        else:
            print("File damaged, with articles missing!!")
            sys.exit()
        j += 1
        if j == 5:
            sys.exit()

    # downloader = Downloader(filedic, SERVERLIST)
    # success = downloader.download()

    ''' threadlist = []
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
        th = Download(sn, t, SERVERLIST)
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

    # DELETE ALL INTERIM FILES'''
