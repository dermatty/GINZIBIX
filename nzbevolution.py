import threading
from threading import Thread
import time
# import queue
from random import randint
import sys
import os
import queue
from os.path import expanduser
import configparser
import signal
import glob
import xml.etree.ElementTree as ET
import nntplib
import ssl
import posix_ipc
import yenc

USERHOME = expanduser("~")
MAIN_DIR = USERHOME + "/.nzbbussi/"
CONFIG_DIR = MAIN_DIR + "config/"
NZB_DIR = MAIN_DIR + "nzb/"
COMPLETE_DIR = MAIN_DIR + "complete/"
INCOMPLETE_DIR = MAIN_DIR + "incomplete/"
LOGS_DIR = MAIN_DIR + "logs/"

LOCK = threading.Lock()
SEMAPHORE = posix_ipc.Semaphore("news_sema", posix_ipc.O_CREAT)
SEMAPHORE.release()


# ---- Procedures ----


def siginthandler(signum, frame):
    global SERVERS, threads
    for t in threads:
        t.stop()
        t.join()
    SERVERS.close_all_connections()
    sys.exit()


def ParseNZB(nzbdir):
    cwd0 = os.getcwd()
    os.chdir(nzbdir)
    print("Getting NZB files from " + nzbdir)
    for nzb in glob.glob("*.nzb"):
        pass
    try:
        tree = ET.parse(nzb)
        print("Downloading " + nzb)
    except Exception as e:
        print(str(e) + ": please provide at least 1 NZB file!")
        return None
    nzbroot = tree.getroot()
    os.chdir(cwd0)
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


def download(article, nntpob):
    r = randint(0, 99)
    # return True, "info!"
    if r <= 95:
        return True, "info!"
    else:
        return False, False


# ---- Classes ----


class Servers():

    def __init__(self, cfg):
        self.cfg = cfg
        # server_config = [(server_name, server_url, user, password, port, usessl, level, connections, retention)]
        self.server_config = self.get_server_config(self.cfg)
        # all_connections = [(server_name, conn#, nntp_obj)]
        self.all_connections = self.get_all_connections()
        # level_servers0 = {"0": ["EWEKA", "BULK"], "1": ["TWEAK"], "2": ["NEWS", "BALD"]}
        self.level_servers = self.get_level_servers()

    def __bool__(self):
        if not self.server_config:
            return False
        return True

    def get_single_server_config(self, server_name0):
        for server_name, server_url, user, password, port, usessl, level, connections, retention in self.server_config:
            if server_name == server_name0:
                return server_name, server_url, user, password, port, usessl, level, connections, retention
        return None

    def get_all_connections(self):
        conn = []
        for s_name, _, _, _, _, _, _, s_connections, _ in self.server_config:
            for c in range(s_connections):
                conn.append((s_name, c + 1, None))
        return conn

    def get_level_servers(self):
        s_tuples = []
        for s_name, _, _, _, _, _, s_level, _, _ in self.server_config:
            s_tuples.append((s_name, s_level))
        sorted_s_tuples = sorted(s_tuples, key=lambda server: server[1])
        ls = {}
        for s_name, s_level in sorted_s_tuples:
            if str(s_level) not in ls:
                ls[str(s_level)] = []
            ls[str(s_level)].append(s_name)
        return ls

    def open_connection(self, server_name0, conn_nr):
        result = None
        for idx, (sn, cn, nobj) in enumerate(self.all_connections):
            if sn == server_name0 and cn == conn_nr:
                if nobj:
                    return nobj
                else:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                    sc = self.get_single_server_config(server_name0)
                    if sc:
                        server_name, server_url, user, password, port, usessl, level, connections, retention = self.get_single_server_config(server_name0)
                        try:
                            if usessl:
                                nntpobj = nntplib.NNTP_SSL(server_url, user=user, password=password, ssl_context=context, port=port, readermode=True)
                            else:
                                nntpobj = nntplib.NNTP(server_url, user=user, password=password, ssl_context=context, port=port, readermode=True)
                            print("Opened Connection #" + str(conn_nr) + " on server " + server_name0)
                            result = nntpobj
                            self.all_connections[idx] = (sn, cn, nntpobj)
                            break
                        except Exception as e:
                            print("Server " + server_name0 + " connect error: " + str(e))
                            self.all_connections[idx] = (sn, cn, None)
                            break
                    else:
                        print("Cannot get server config for server: " + server_name0)
                        self.all_connections[idx] = (sn, cn, None)
                        break
        return result

    def close_all_connections(self):
        for (sn, cn, nobj) in self.all_connections:
            if nobj:
                try:
                    nobj.quit()
                    print("Closed connection #" + str(cn) + " on " + sn)
                except Exception as e:
                    print("Cannot quit server " + sn + ": " + str(e))

    def get_server_config(self, cfg):
        # get servers from config
        snr = 0
        sconf = []
        while True:
            try:
                snr += 1
                snrstr = "SERVER" + str(snr)
                server_name = cfg[snrstr]["SERVER_NAME"]
                server_url = cfg[snrstr]["SERVER_URL"]
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
                sconf.append((server_name, server_url, user, password, port, usessl, level, connections, retention))
            except Exception as e:
                sconf.append((server_name, server_url, user, password, port, usessl, level, connections, 999999))
        if not sconf:
            return None
        return sconf


class ConnectionWorker(Thread):
    def __init__(self, lock, connection, articlequeue, servers):
        Thread.__init__(self)
        self.connection = connection
        self.articlequeue = articlequeue
        self.lock = lock
        self.servers = servers
        self.nntpobj = None
        self.running = True

    def stop(self):
        self.running = False

    def download_article(self, article):
        articlenr, artname, age, remaining_servers = article
        sn, _ = self.connection
        server_name, server_url, user, password, port, usessl, level, connections, retention = self.servers.get_single_server_config(sn)
        if retention < age * 0.95:
            return False, None
        try:
            resp_h, info_h = self.nntpobj.head(artname)
            resp, info = self.nntpobj.body(artname)
            if resp_h[:3] != "221" or resp[:3] != "222":
                raise("No Succes answer from server!")
            else:
                resp = True
        except Exception as e:
            resp = False
            info = None
            print("Article download error: " + str(e))
        return resp, info

    def run(self):
        global resultarticles_dic
        name, conn_nr = self.connection
        idn = name + " #" + str(conn_nr)
        if not self.nntpobj:
            self.nntpobj = self.servers.open_connection(name, conn_nr)
        if not self.nntpobj:
            print("Could not connect to server " + idn + ", exiting thread")
            self.stop()
            return
        else:
            print(idn + " starting !")
        while self.running:
            try:
                article = self.articlequeue.get_nowait()
                # (artnr, fn, age, level_servers)
                articlenr, artname, age, remaining_servers = article
                self.articlequeue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print("Error in article queue get: " + str(e))
            if name not in remaining_servers:
                self.articlequeue.put(article)
            else:
                print("Downloading on server " + idn + ": + for article #" + str(articlenr), remaining_servers)
                res, info = self.download_article(article)
                # res, info = download(article, self.nntpobj)
                if res:
                    print("Download success on server " + idn + ": for article #" + str(articlenr), remaining_servers)
                    with self.lock:
                        resultarticles_dic[str(articlenr)] = (artname, age, info)
                else:
                    article = (articlenr, artname, age, [x for x in remaining_servers if x != name])
                    if not article[3]:
                        print(">>>> Download finally failed on server " + idn + ": for article #" + str(articlenr))
                        with self.lock:
                            resultarticles_dic[str(articlenr)] = (artname, age, None)
                    else:
                        self.articlequeue.put(article)
                        print(">>>> Download failed on server " + idn + ": for article #" + str(articlenr), ", requeuing on servers:",
                              article[3])
        print(idn + " exited!")


class Downloader():
    def __init__(self, servers, lock, level_servers, all_connections):
        self.servers = servers
        self.lock = lock
        self.level_servers = level_servers
        self.all_connections = all_connections

    def article_producer(self, articles, articlequeue):
        for article in articles:
            articlequeue.put(article)

    def make_allfilelist(self, filedic):
        allfilelist = []
        for idx, (filename, filelist) in enumerate(filedic.items()):
            for i, f in enumerate(filelist):
                if i == 0:
                    age, filetype, nr_articles = f
                    allfilelist.append([(filename, age, filetype, nr_articles)])
                else:
                    fn, nr = f
                    allok = True    # check for duplicate art. #
                    if len(allfilelist[idx]) > 2:
                        for i1, art in enumerate(allfilelist[idx]):
                            if i1 > 1:
                                nr1, fn1 = art
                                if nr1 == nr:
                                    allok = False
                                    break
                    if allok:
                        allfilelist[idx].append((nr, fn))
        return allfilelist

    def decode_articles(self, infolist):
        bytes0 = bytearray()
        bytesfinal = bytearray()
        pcrc32 = ""
        status = 0
        for info in infolist:
            headerfound = 0
            endfound = 0
            partfound = 0
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
                bytes0.extend(inf)
            if headerfound != 1 or endfound != 1 or partfound > 1:
                print("Wrong yenc structure detected")
                status = -1
            if pcrc32 == "":
                status = -1
            _, crc32, decoded = yenc.decode(bytes0)
            if crc32.strip("0") != pcrc32.strip("0"):
                print("CRC32 checksum error: " + crc32 + " / " + pcrc32)
                status = -1
            bytesfinal.extend(decoded)
            bytes0 = bytearray()
        return bytesfinal, status  # status = 0 -> ok, = -1 -> repair (par2) needed

    def save_bytesresult(self, bytesresult, filename):
        global COMPLETE_DIR
        with open(COMPLETE_DIR + filename, "wb") as f0:
            f0.write(bytesresult)
            f0.flush()
            f0.close()
            print("!!!! " + filename + " saved!!")

    def download_and_process(self, filedic, articlequeue):
        global resultarticles_dic
        allfileslist = self.make_allfilelist(filedic)
        # iterate over all files in NZB
        for file_articles in allfileslist:
            resultarticles_dic = {}
            # iterate over all articles in file
            for i, art0 in enumerate(file_articles):
                if i == 0:
                    filename, age, filetype, nr_articles = art0
                    continue
                nr, fn = art0
                resultarticles_dic[str(nr)] = (fn, age, None)
            dl.download_file(articlequeue)     # result in resultarticles_dic
            lenresultarticles = len(resultarticles_dic)
            len_ok_resultarticles = len([key for key, (_, _, info) in resultarticles_dic.items() if info])
            # resultarticles_dic[str(articlenr)] = (artname, age, info)
            if len_ok_resultarticles < lenresultarticles:
                print("*" * 80)
                print("Articles missing!!!!")
                print("*" * 80)
                input("Press Enter to continue")
                continue    # break !???
            infolist = []
            for i in range(lenresultarticles):
                for key, (_, _, info) in resultarticles_dic.items():
                    if str(key) == str(i + 1):
                        infolist.append(info)
                        break
            bytesresult, status = self.decode_articles(infolist)
            self.save_bytesresult(bytesresult, filename)

    def download_file(self, articlequeue):
        global resultarticles_dic, threads
        for level, serverlist in self.level_servers.items():
            level_servers = serverlist
            # articles = [(key, level_servers) for key, item in resultarticles_dic.items() if not item]
            articles = [(artnr, fn, age, level_servers) for artnr, (fn, age, info) in resultarticles_dic.items() if not info]
            if not articles:
                print("All articles downloaded")
                break
            # print("####", articles)
            level_connections = [(name, connection) for name, connection, _ in self.all_connections if name in level_servers]
            if not level_connections:
                continue
            # Produce
            self.article_producer(articles, articlequeue)
            # consumer
            threads = []
            for c in level_connections:
                t = ConnectionWorker(self.lock, c, articlequeue, self.servers)
                threads.append(t)
                t.start()

            articlequeue.join()

            for t in threads:
                t.stop()
                t.join()

            print("Download failed:", [(key, fn) for key, (fn, age, info) in resultarticles_dic.items() if not info])
            l0 = len([info for key, (fn, age, info) in resultarticles_dic.items()])
            l1 = len([info for key, (fn, age, info) in resultarticles_dic.items() if info])
            print("Complete  Articles after level", level, ": " + str(l1) + " out of " + str(l0))
            print("-" * 80)


# main
if __name__ == '__main__':

    resultarticles_dic = {}

    CFG = configparser.ConfigParser()
    CFG.read(CONFIG_DIR + "/nzbbussi.config")

    # get servers
    SERVERS = Servers(CFG)
    if not SERVERS:
        print("At least one server has to be provided, exiting!")
        sys.exit()

    signal.signal(signal.SIGINT, siginthandler)
    signal.signal(signal.SIGTERM, siginthandler)

    filedic = ParseNZB(NZB_DIR)
    level_servers0 = SERVERS.level_servers
    all_connections = SERVERS.all_connections
    articlequeue = queue.Queue()

    dl = Downloader(SERVERS, LOCK, level_servers0, all_connections)
    dl.download_and_process(filedic, articlequeue)

    SERVERS.close_all_connections()
